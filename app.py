#!/usr/bin/env python3
"""
app.py
======
Febarr web app entrypoint. Runs a persistent Flask server exposing:
  - a small web UI (queue exports, watch live progress, edit settings)
  - a JSON API the UI talks to

Run it with:

    python app.py

then open http://127.0.0.1:5000 in a browser. The server keeps running
(and keeps processing queued exports) as long as the process is alive.

By default it only binds to 127.0.0.1 (localhost) -- your Febbox login
cookie is stored on disk in data/state.json and used by this server, so
don't bind it to 0.0.0.0 / expose it beyond your own machine unless you
know what you're doing.
"""

import argparse
import os

from flask import Flask, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

from febarr import classify, core, media_library, tmdb_episodes
from febarr.analyzer import AnalyzeManager
from febarr.discovered import DiscoveredManager
from febarr.links import SavedLinksManager
from febarr.state import StateStore
from febarr.tasks import CATEGORY_LABELS, TaskManager, compute_target_path, year_belongs_in_folder

app = Flask(__name__)

store = StateStore()
manager = TaskManager(store)
discovered = DiscoveredManager(store)
links = SavedLinksManager(store)
analyzer = AnalyzeManager(store, manager, discovered, links)


def _handle_task_settled(task: dict) -> None:
    """TaskManager.on_settled -- fires once a Cancel actually finishes
    (see tasks.TaskManager.cancel_task()/_run_task()). Recreates the
    task as a discovered library item (falling back to parsing category/
    title/year out of target_path for a manually-targeted task, same as
    media_library.augment_with_pipeline() does for display) and deletes
    the task record, so "cancelled" never sticks around as an observable
    status -- the title just lands back in the library instead.

    A cancelled *download* task skips the discovered-item recreation
    entirely -- the title it belongs to is already Ready/on-disk (that's
    the only state a download task is ever created from, see
    media_library.build_download_items()), so there's nothing to
    rediscover; whatever files it did finish stay downloaded, the rest
    stay .strm, and the record is just dropped."""
    if task.get("task_type") != "download":
        category = task.get("category")
        title = task.get("detected_title")
        year = task.get("detected_year")
        if category not in CATEGORY_LABELS:
            category, title, year = media_library.category_title_year_from_target_path(task.get("target_path"))
        if category in CATEGORY_LABELS:
            discovered.add(
                share_link=task["share_link"],
                share_key=task["share_key"],
                kind=task.get("root_kind", "share_root"),
                root_fid=task.get("root_fid"),
                root_items=task.get("root_items"),
                category=category,
                title=title or "Untitled",
                year=year,
                source=task.get("classification_source") or "manual",
                confidence=task.get("classification_confidence") or "high",
                tmdb_id=task.get("tmdb_id"),
            )
        # No recognizable Category/Title (Year) at all (a target_path that
        # doesn't follow the convention) -- nothing library-shaped to
        # recreate, so it's just dropped rather than left as a phantom task.
    try:
        manager.delete_task(task["id"])
    except (KeyError, ValueError):
        pass


manager.on_settled = _handle_task_settled

# Started at import time (not just inside main()) so the dispatcher/
# auto-resync threads come up whether the app is run via `python app.py`
# or imported by a WSGI server (gunicorn's `app:app`, see Dockerfile) --
# idempotent (TaskManager.start() no-ops if already running), so main()
# calling it again for the direct-run case is harmless.
manager.start()

app.secret_key = store.get_or_create_secret_key()

MIN_PASSWORD_LENGTH = 8


def _auth_configured() -> bool:
    settings = store.get_settings()
    return bool(settings.get("auth_username") and settings.get("auth_password_hash"))


def _safe_next_path(value: str) -> str:
    # Only ever redirect to a path within this app -- never an open redirect.
    if value and value.startswith("/") and not value.startswith("//"):
        return value
    return "/"


@app.before_request
def require_login():
    if request.path == "/login" or request.path == "/healthz" or request.path.startswith("/static/"):
        return None
    if not _auth_configured() or session.get("authenticated"):
        return None
    if request.path.startswith("/api/"):
        return jsonify({"error": "authentication required"}), 401
    return redirect(url_for("login", next=request.path))


# --------------------------------------------------------------------------
# Pages
# --------------------------------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html", auth_configured=_auth_configured())


@app.route("/healthz")
def healthz():
    """Unauthenticated liveness/readiness probe -- Docker's HEALTHCHECK
    (see Dockerfile) and any orchestrator that wants one hit this
    instead of "/", which would otherwise redirect to /login once an
    account's configured. Touches the same store every real request
    does, so a genuinely broken state.json (corrupt disk, bad
    permissions) fails the check instead of reporting healthy."""
    store.get_settings()
    return jsonify({"status": "ok"})


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    next_path = _safe_next_path(request.args.get("next") or request.form.get("next") or "/")
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        settings = store.get_settings()
        stored_username = settings.get("auth_username") or ""
        stored_hash = settings.get("auth_password_hash") or ""
        if stored_username and username == stored_username and stored_hash and check_password_hash(stored_hash, password):
            session.clear()
            session["authenticated"] = True
            session.permanent = True
            return redirect(next_path)
        error = "Incorrect username or password."
    return render_template("login.html", error=error, next_path=next_path)


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return redirect(url_for("login"))


# --------------------------------------------------------------------------
# Settings API
# --------------------------------------------------------------------------
@app.route("/api/settings", methods=["GET"])
def get_settings():
    return jsonify(store.get_settings_masked())


@app.route("/api/settings", methods=["POST"])
def update_settings():
    body = request.get_json(silent=True) or {}
    patch = {}

    if "login_cookie" in body:
        patch["login_cookie"] = (body.get("login_cookie") or "").strip()
    if "tmdb_api_key" in body:
        patch["tmdb_api_key"] = (body.get("tmdb_api_key") or "").strip()
    if "min_release_year" in body:
        try:
            min_year = int(body.get("min_release_year") or 0)
        except (TypeError, ValueError):
            return jsonify({"error": "min_release_year must be a whole number"}), 400
        if min_year != 0 and (min_year < 1900 or min_year > 2100):
            return jsonify({"error": "min_release_year must be 0 (disabled) or between 1900 and 2100"}), 400
        patch["min_release_year"] = min_year
    if "auto_queue_discovered" in body:
        patch["auto_queue_discovered"] = bool(body.get("auto_queue_discovered"))
    if "flaresolverr_url" in body:
        patch["flaresolverr_url"] = (body.get("flaresolverr_url") or "").strip()
    if "export_root" in body:
        export_root = (body.get("export_root") or "").strip()
        if not export_root:
            return jsonify({"error": "export_root cannot be empty"}), 400
        patch["export_root"] = export_root
    if "max_parallel_workers" in body:
        try:
            workers = int(body.get("max_parallel_workers"))
        except (TypeError, ValueError):
            return jsonify({"error": "max_parallel_workers must be a number"}), 400
        if workers < 1:
            return jsonify({"error": "max_parallel_workers must be at least 1"}), 400
        patch["max_parallel_workers"] = workers
    if "max_parallel_downloads" in body:
        try:
            downloads = int(body.get("max_parallel_downloads"))
        except (TypeError, ValueError):
            return jsonify({"error": "max_parallel_downloads must be a number"}), 400
        if downloads < 1:
            return jsonify({"error": "max_parallel_downloads must be at least 1"}), 400
        patch["max_parallel_downloads"] = downloads
    if "resync_stale_days" in body:
        try:
            days = float(body.get("resync_stale_days"))
        except (TypeError, ValueError):
            return jsonify({"error": "resync_stale_days must be a number"}), 400
        if days < 0 or days > 365:
            return jsonify({"error": "resync_stale_days must be between 0 (disabled) and 365"}), 400
        patch["resync_stale_days"] = days
    if "request_timeout_seconds" in body:
        try:
            request_timeout = int(body.get("request_timeout_seconds"))
        except (TypeError, ValueError):
            return jsonify({"error": "request_timeout_seconds must be a whole number"}), 400
        if request_timeout < 5 or request_timeout > 120:
            return jsonify({"error": "request_timeout_seconds must be between 5 and 120"}), 400
        patch["request_timeout_seconds"] = request_timeout
    if "max_retries" in body:
        try:
            max_retries = int(body.get("max_retries"))
        except (TypeError, ValueError):
            return jsonify({"error": "max_retries must be a whole number"}), 400
        if max_retries < 0 or max_retries > 10:
            return jsonify({"error": "max_retries must be between 0 and 10"}), 400
        patch["max_retries"] = max_retries
    if "max_folder_depth" in body:
        try:
            max_folder_depth = int(body.get("max_folder_depth"))
        except (TypeError, ValueError):
            return jsonify({"error": "max_folder_depth must be a whole number"}), 400
        if max_folder_depth < 1 or max_folder_depth > 50:
            return jsonify({"error": "max_folder_depth must be between 1 and 50"}), 400
        patch["max_folder_depth"] = max_folder_depth
    if "user_agent" in body:
        patch["user_agent"] = (body.get("user_agent") or "").strip()
    if "queue_poll_seconds" in body:
        try:
            queue_poll_seconds = float(body.get("queue_poll_seconds"))
        except (TypeError, ValueError):
            return jsonify({"error": "queue_poll_seconds must be a number"}), 400
        if queue_poll_seconds < 1 or queue_poll_seconds > 30:
            return jsonify({"error": "queue_poll_seconds must be between 1 and 30"}), 400
        patch["queue_poll_seconds"] = queue_poll_seconds

    store.update_settings(patch)
    return jsonify(store.get_settings_masked())


@app.route("/api/account", methods=["POST"])
def update_account():
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    confirm_password = body.get("confirm_password") or ""
    if not username:
        return jsonify({"error": "username is required"}), 400

    password_hash = None
    if password or confirm_password:
        if password != confirm_password:
            return jsonify({"error": "passwords do not match"}), 400
        if len(password) < MIN_PASSWORD_LENGTH:
            return jsonify({"error": f"password must be at least {MIN_PASSWORD_LENGTH} characters"}), 400
        password_hash = generate_password_hash(password)
    elif not _auth_configured():
        return jsonify({"error": "a password is required to enable login"}), 400

    store.set_account(username, password_hash)
    session["authenticated"] = True  # don't lock yourself out by changing your own credentials
    return jsonify(store.get_settings_masked())


@app.route("/api/account", methods=["DELETE"])
def disable_account():
    store.clear_account()
    session.clear()
    return jsonify(store.get_settings_masked())


# --------------------------------------------------------------------------
# Share analysis -- streams straight to the library as it goes: every
# group with a confident TMDB match is added to Movies/Series (as a
# discovered item, not yet an export task) the moment it's found, no
# manual confirm step (see analyzer.py). Anything without a match, or
# already covered by an existing discovered item/task/disk export, is
# recorded as "denied" and shown, never added twice. Runs as a background
# job so it shows up in Activity immediately instead of leaving the UI
# blank until a multi-folder share finishes being inspected. The job
# itself isn't persisted long-term -- it's a throwaway progress tracker
# that lives only as long as the run; the discovered items it creates
# along the way are real DiscoveredManager records and outlive it.
# --------------------------------------------------------------------------
@app.route("/api/shares/analyze", methods=["POST"])
def analyze_share():
    body = request.get_json(silent=True) or {}
    try:
        job = analyzer.start(body.get("share_link", ""))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(job), 202


@app.route("/api/shares/analyze_jobs", methods=["GET"])
def list_analyze_jobs():
    return jsonify(analyzer.list_jobs())


@app.route("/api/shares/analyze/<job_id>", methods=["GET"])
def get_analyze_job(job_id):
    job = analyzer.get_job(job_id)
    if job is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(job)


@app.route("/api/shares/analyze/<job_id>", methods=["DELETE"])
def dismiss_analyze_job(job_id):
    try:
        analyzer.dismiss(job_id)
    except KeyError:
        return jsonify({"error": "not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True})


@app.route("/api/shares/analyze/<job_id>/cancel", methods=["POST"])
def cancel_analyze_job(job_id):
    try:
        job = analyzer.cancel(job_id)
    except KeyError:
        return jsonify({"error": "not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(job)


# --------------------------------------------------------------------------
# Links -- the permanent record of every share ever submitted for
# Analyze (see links.SavedLinksManager), so it can be found again and
# re-checked for new files without digging the URL back up. Every
# /api/shares/analyze call above already saves/refreshes the matching
# row (analyzer.start() -> links.remember()); these routes are just the
# Links page's own list/recheck/remove.
# --------------------------------------------------------------------------
@app.route("/api/links", methods=["GET"])
def list_links():
    return jsonify(links.list_items())


@app.route("/api/links/<item_id>/recheck", methods=["POST"])
def recheck_link(item_id):
    """Re-runs Analyze on an already-saved link -- same job/flow a fresh
    paste starts (any new files/titles land in Movies/Series exactly the
    same way), just sourced from the saved share_link instead of user
    input."""
    item = links.get(item_id)
    if item is None:
        return jsonify({"error": "not found"}), 404
    try:
        job = analyzer.start(item["share_link"])
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(job), 202


@app.route("/api/links/<item_id>", methods=["DELETE"])
def remove_link(item_id):
    try:
        links.remove(item_id)
    except KeyError:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


# --------------------------------------------------------------------------
# Tasks API
# --------------------------------------------------------------------------
@app.route("/api/tasks", methods=["GET"])
def list_tasks():
    return jsonify(manager.list_tasks())


@app.route("/api/tasks/queue_status", methods=["GET"])
def queue_status():
    return jsonify(manager.queue_status())


@app.route("/api/tasks/pause", methods=["POST"])
def pause_queue():
    manager.set_paused(True)
    return jsonify(manager.queue_status())


@app.route("/api/tasks/resume_queue", methods=["POST"])
def resume_queue():
    manager.set_paused(False)
    return jsonify(manager.queue_status())


@app.route("/api/tasks", methods=["POST"])
def create_task():
    body = request.get_json(silent=True) or {}
    try:
        task = manager.create_task(
            share_link=body.get("share_link", ""),
            target_path=body.get("target_path", ""),
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(task), 201


@app.route("/api/tasks/<task_id>", methods=["GET"])
def get_task(task_id):
    task = manager.get_task(task_id)
    if task is None:
        return jsonify({"error": "not found"}), 404
    return jsonify(task)


@app.route("/api/tasks/<task_id>/cancel", methods=["POST"])
def cancel_task(task_id):
    try:
        task = manager.cancel_task(task_id)
    except KeyError:
        return jsonify({"error": "not found"}), 404
    return jsonify(task)


@app.route("/api/tasks/<task_id>/resume", methods=["POST"])
def resume_task(task_id):
    try:
        task = manager.resume_task(task_id)
    except KeyError:
        return jsonify({"error": "not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(task)


@app.route("/api/tasks/<task_id>/run_now", methods=["POST"])
def run_task_now(task_id):
    try:
        task = manager.run_now(task_id)
    except KeyError:
        return jsonify({"error": "not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(task)


@app.route("/api/tasks/<task_id>/resync", methods=["POST"])
def resync_task(task_id):
    try:
        task = manager.resync_task(task_id, note="[server] Resync requested by user.")
    except KeyError:
        return jsonify({"error": "not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(task)


@app.route("/api/tasks/<task_id>/remove", methods=["POST"])
def remove_task(task_id):
    """Movies/Series' "Remove" action -- a hard removal, gone from the
    queue *and* the library entirely (queued/errored only; a running
    task must be cancelled first -- Cancel is available on Movies/Series
    now too, not just Activity). This is the only place Cancel and
    Remove ever meet: Cancel just stops a task and settles it straight
    back into a discovered library item (see TaskManager.on_settled);
    Remove is what actually deletes it, no library trace left behind."""
    try:
        manager.remove_task(task_id)
    except KeyError:
        return jsonify({"error": "not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True})


@app.route("/api/tasks/<task_id>", methods=["DELETE"])
def delete_task(task_id):
    try:
        manager.delete_task(task_id)
    except KeyError:
        return jsonify({"error": "not found"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"ok": True})


# --------------------------------------------------------------------------
# Manual TMDB search -- look a title up by name (poster/overview/year),
# used by the global search bar's "not a share link" path. No tracking,
# no watch list, no auto-matching -- purely a lookup.
# --------------------------------------------------------------------------
@app.route("/api/tmdb/search", methods=["GET"])
def tmdb_search():
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"results": []})
    settings = store.get_settings()
    tmdb = classify.TMDBClient(settings.get("tmdb_api_key", ""))
    if not tmdb.available:
        return jsonify({"error": "Add a TMDB API key in Settings first"}), 400
    return jsonify({"results": tmdb.search_many(query)})


# --------------------------------------------------------------------------
# Media library -- Movies/Series pages: every title through its whole
# lifecycle (discovered -> queued -> exporting -> ready), not just what's
# finished on disk. Starts from a pure export_root scan (media_library.
# list_media()) and folds in discovered items + active tasks on top (see
# augment_with_pipeline()), plus delete/queue/dismiss/download operations.
# --------------------------------------------------------------------------
def _find_task(media_id: str, task_type: str = "export", statuses: set = None):
    """Shared 'find the task record belonging to this title' lookup --
    matched on target_path, which is exactly media_id already (the
    convention compute_target_path()/list_media() both use)."""
    norm_media_id = (media_id or "").replace("\\", "/").strip().lower()
    tasks = manager.list_tasks()
    matching = []
    for t in tasks:
        if t.get("task_type", "export") != task_type:
            continue
        if statuses is not None and t.get("status") not in statuses:
            continue
        task_target = (t.get("target_path") or "").replace("\\", "/").strip().lower()
        if task_target == norm_media_id:
            matching.append(t)
            continue
        cat = t.get("category")
        title = t.get("detected_title")
        year = t.get("detected_year")
        if cat and title:
            try:
                computed = compute_target_path(
                    cat, title, year, include_year=year_belongs_in_folder(cat, store.get_settings())
                ).replace("\\", "/").strip().lower()
                if computed == norm_media_id:
                    matching.append(t)
            except Exception:
                pass

    if not matching:
        return None
    status_priority = {"done": 0, "running": 1, "queued": 2, "error": 3, "cancelled": 4}
    matching.sort(key=lambda t: (status_priority.get(t.get("status"), 99), -(t.get("files_written", 0))))
    return matching[0]


@app.route("/api/media", methods=["GET"])
def list_media_route():
    settings = store.get_settings()
    export_root = settings.get("export_root", "")
    media_type = request.args.get("type") or None  # "movie" | "series" | None (both)
    entries = media_library.list_media(export_root, media_type=media_type)
    entries = media_library.augment_with_pipeline(
        entries, discovered.list_items(), manager.list_tasks(), media_type=media_type,
        export_root=export_root, settings=settings,
    )
    return jsonify(entries)


@app.route("/api/discovered/<item_id>/queue", methods=["POST"])
def queue_discovered(item_id):
    """Promotes a discovered library item into a real export task -- the
    explicit "Queue" action from Movies/Series."""
    item = discovered.get(item_id)
    if item is None:
        return jsonify({"error": "not found"}), 404
    try:
        task = manager.create_task_from_proposal(
            share_link=item["share_link"],
            kind=item["kind"],
            root_fid=item["root_fid"],
            root_items=item["root_items"],
            category=item["category"],
            title=item["title"],
            year=item.get("year"),
            source=item.get("source", "manual"),
            confidence=item.get("confidence", "high"),
            tmdb_id=item.get("tmdb_id"),
        )
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    discovered.remove(item_id)
    return jsonify(task), 201


@app.route("/api/discovered/<item_id>", methods=["DELETE"])
def dismiss_discovered(item_id):
    """Drops a discovered item without ever exporting it."""
    try:
        discovered.remove(item_id)
    except KeyError:
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


@app.route("/api/media/<path:media_id>", methods=["GET"])
def get_media_detail_route(media_id):
    settings = store.get_settings()
    source_task = _find_task(media_id, "export")
    try:
        detail = media_library.get_media_detail(settings.get("export_root", ""), media_id, task=source_task)
    except (FileNotFoundError, ValueError):
        parts = (media_id or "").replace("\\", "/").split("/", 1)
        label = parts[0]
        folder_name = parts[1] if len(parts) > 1 else parts[0]
        category = next((c for c, l in CATEGORY_LABELS.items() if l == label), None)
        title, year = media_library._parse_title_year(folder_name)
        is_series = category in media_library._SERIES_CATEGORIES
        detail = {
            "id": media_id,
            "category": category,
            "is_series": is_series,
            "title": title,
            "year": year,
            "file_count": 0,
            "files": [] if not is_series else None,
            "seasons": [] if is_series else None,
        }

    # Prefer the tmdb_id already confirmed at Analyze time (recorded on
    # the matching finished export task) over a fresh fuzzy title search
    # -- authoritative rather than a guess, and it's also what the
    # episode reconciliation below needs to mean anything. Falls back to
    # search_many() (the old behavior) only when there's no such task/id,
    # e.g. a manually-targeted export that never went through Analyze.
    tmdb_id = source_task.get("tmdb_id") if source_task else None

    detail["metadata"] = None
    tmdb = classify.TMDBClient(settings.get("tmdb_api_key", ""))
    if tmdb.available and tmdb_id:
        raw = tmdb.get_tv_details(tmdb_id) if detail["is_series"] else tmdb.get_movie_details(tmdb_id)
        if raw:
            detail["metadata"] = {
                "tmdb_id": tmdb_id,
                "title": raw.get("name") or raw.get("title") or detail["title"],
                "overview": raw.get("overview") or "",
                "poster_path": raw.get("poster_path"),
                "backdrop_path": raw.get("backdrop_path"),
            }
    if detail["metadata"] is None and tmdb.available:
        candidates = tmdb.search_many(detail["title"])
        if candidates:
            best = next((c for c in candidates if c.get("year") == detail["year"]), candidates[0]) \
                if detail["year"] else candidates[0]
            detail["metadata"] = best
            tmdb_id = tmdb_id or best.get("tmdb_id")

    # Every episode TMDB knows about for this show, not just the ones
    # with a file -- whichever of TMDB's episode orderings scores best
    # against what's actually on disk (see tmdb_episodes.py). Only
    # touches detail["seasons"] when reconciliation actually produced
    # something; otherwise the plain disk-only scan from
    # media_library.get_media_detail() (already in `detail`) stands as
    # the fallback. file_count deliberately stays the on-disk count, not
    # TMDB's -- it's "how many files you actually have," unchanged.
    if detail["is_series"]:
        disk_seasons = detail.get("seasons") or []
        reconciled = None
        if tmdb_id:
            force_refresh = request.args.get("refresh") == "1"
            reconciled = tmdb_episodes.get_reconciled_seasons(
                tmdb, tmdb_id, disk_seasons, force_refresh=force_refresh
            )
        if reconciled is not None:
            detail["seasons"] = reconciled
        else:
            # No TMDB reconciliation available (no key, no confirmed
            # match, or the show has no episode data at all) -- still
            # normalize to the same {"episode_number","name","air_date",
            # "on_disk"} shape a reconciled result uses, so the frontend
            # only ever has one episode shape to render either way.
            detail["seasons"] = [
                {
                    "season": s["season"],
                    "label": s["label"],
                    "episodes": [
                        {"episode_number": ep.get("episode"), "name": None, "air_date": None, "on_disk": ep}
                        for ep in s["episodes"]
                    ],
                }
                for s in disk_seasons
            ]

    # A live glance at an in-flight download for this title, if there is
    # one -- lets the detail page show progress/Cancel inline without a
    # second poll loop of its own; Activity remains the authoritative
    # queue view.
    detail["download_task"] = _find_task(media_id, "download", {"queued", "running"})
    return jsonify(detail)


@app.route("/api/media/<path:media_id>", methods=["DELETE"])
def delete_media_route(media_id):
    settings = store.get_settings()
    try:
        media_library.delete_media(settings.get("export_root", ""), media_id)
    except (FileNotFoundError, ValueError):
        pass
    done_task = _find_task(media_id, "export")
    if done_task is not None:
        try:
            manager.delete_task(done_task["id"])
        except (KeyError, ValueError):
            pass
    return jsonify({"ok": True})


@app.route("/api/media/<path:media_id>/rename", methods=["POST"])
def rename_media_route(media_id):
    """Movies/Series detail page's "edit path" control -- renames the
    folder-name segment of a title's Category/Title (Year) id (category
    itself stays fixed; that's a re-classification, not a rename). Moves
    the on-disk folder if any downloaded files exist there, and points
    every task/discovered record that was keyed to the old id at the new
    one, so the title keeps being tracked as the same thing under its
    new name -- see media_library.rename_media(), TaskManager.
    retarget_tasks(), DiscoveredManager.retarget()."""
    data = request.get_json(silent=True) or {}
    new_name = (data.get("name") or "").strip()
    if not new_name:
        return jsonify({"error": "name is required"}), 400

    category, _old_title, _old_year = media_library.category_title_year_from_target_path(media_id)
    if category is None:
        return jsonify({"error": "not found"}), 404

    settings = store.get_settings()
    export_root = settings.get("export_root", "")

    new_media_id = f"{CATEGORY_LABELS[category]}/{core.sanitize_name(new_name)}"
    if new_media_id != media_id:
        existing_ids = {e["id"] for e in media_library.list_media(export_root)}
        existing_ids |= {
            compute_target_path(it["category"], it["title"], it.get("year"),
                                 include_year=year_belongs_in_folder(it["category"], settings))
            for it in discovered.list_items()
        }
        existing_ids |= {t.get("target_path") for t in manager.list_tasks() if t.get("target_path")}
        if new_media_id in existing_ids:
            return jsonify({"error": "a title with that name already exists"}), 409

    try:
        new_media_id = media_library.rename_media(export_root, media_id, new_name)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    new_title, new_year = media_library._parse_title_year(new_media_id.split("/", 1)[1])
    manager.retarget_tasks(media_id, new_media_id, new_title, new_year)
    discovered.retarget(media_id, new_title, new_year)

    return jsonify({"id": new_media_id, "title": new_title, "year": new_year})


@app.route("/api/media/<path:media_id>/file", methods=["DELETE"])
def delete_media_file_route(media_id):
    settings = store.get_settings()
    file_rel_path = (request.args.get("path") or "").strip()
    if not file_rel_path:
        return jsonify({"error": "path is required"}), 400
    try:
        media_library.delete_media_file(settings.get("export_root", ""), media_id, file_rel_path)
    except (FileNotFoundError, ValueError):
        pass

    source_task = _find_task(media_id, "export")
    if source_task is not None:
        with manager._lock:
            task = manager._records.get(source_task["id"])
            if task is not None:
                task["items"] = [it for it in task["items"] if it["rel_path"] != file_rel_path]
                task["total_items"] = len(task["items"])
                manager._recompute_counts(task)
                manager._persist_locked()
    return jsonify({"ok": True})


# --------------------------------------------------------------------------
# Download -- once a title is Streamable, turn some/all of its .strm
# files into real files on disk. Rides the exact same TaskManager queue
# used for exports (see tasks.TaskManager.create_download_task()); these
# routes' only job is finding the right source items (the finished
# export task's own item list, filtered down to what's still actually a
# .strm right now -- media_library.build_download_items()) and hitting
# "go". A single, shared helper builds the tasks either way (whole title
# or one file), the two routes just differ in which items they pass.
#
# One task per file, not one task for the whole batch -- so Activity/
# Downloads shows each file as its own queue entry with its own progress
# bar instead of one merged row, and so nothing needs to be blocked or
# merged when another download for the same title is already queued/
# running: this just adds more (single-item) tasks alongside it.
# --------------------------------------------------------------------------
def _start_downloads(media_id: str, items: list):
    if not items:
        return [], ("nothing left to download", 400)
    source_task = _find_task(media_id, "export", {"done", "running"})
    if source_task is None:
        return [], ("no export found for this title -- nothing to download from", 400)
    category, title, year = media_library.category_title_year_from_target_path(media_id)
    tasks = []
    for item in items:
        try:
            tasks.append(manager.create_download_task(
                share_link=source_task["share_link"],
                target_path=media_id,
                items=[item],
                category=category,
                title=title,
                year=year,
                tmdb_id=source_task.get("tmdb_id"),
            ))
        except ValueError as e:
            return tasks, (str(e), 400)
    return tasks, None


def _chrono_sort_key(item: dict):
    """(season, episode) ascending, so a multi-episode Download queues
    oldest-first instead of whatever order the source list happened to be
    in (FebBox's own file listing tends to come back newest-first).
    Episode-less items (specials, movies) sort after numbered ones within
    their season, by filename."""
    season, episode = media_library.parse_rel_path_season_episode(item["rel_path"])
    return (season, episode if episode is not None else float("inf"), item["rel_path"])


@app.route("/api/media/<path:media_id>/download", methods=["POST"])
def download_media_route(media_id):
    """Whole-title Download -- queues every file that's still streamable
    in this title right now. An optional ?season=N scopes it to just one
    season (the media detail page's per-season "Download all" control),
    matched the same filename-first/folder-fallback way the season
    breakdown itself is built -- see media_library.
    parse_rel_path_season_episode()."""
    settings = store.get_settings()
    source_task = _find_task(media_id, "export", {"done", "running"})
    items = media_library.build_download_items(
        settings.get("export_root", ""), media_id, source_task.get("items", []) if source_task else []
    )
    season_param = request.args.get("season")
    if season_param is not None:
        try:
            season_num = int(season_param)
        except ValueError:
            return jsonify({"error": "season must be a whole number"}), 400
        items = [it for it in items if media_library.parse_rel_path_season_episode(it["rel_path"])[0] == season_num]
    items.sort(key=_chrono_sort_key)
    tasks, error = _start_downloads(media_id, items)
    if error is not None and not tasks:
        return jsonify({"error": error[0]}), error[1]
    return jsonify(tasks), 201


@app.route("/api/media/<path:media_id>/file/download", methods=["POST"])
def download_media_file_route(media_id):
    """Single-file Download -- same as the whole-title version, filtered
    down to just the one file (?path=...)."""
    file_rel_path = (request.args.get("path") or "").strip()
    if not file_rel_path:
        return jsonify({"error": "path is required"}), 400
    settings = store.get_settings()
    source_task = _find_task(media_id, "export", {"done", "running"})
    items = media_library.build_download_items(
        settings.get("export_root", ""), media_id, source_task.get("items", []) if source_task else []
    )
    items = [it for it in items if it["rel_path"] == file_rel_path]
    tasks, error = _start_downloads(media_id, items)
    if error is not None and not tasks:
        return jsonify({"error": error[0]}), error[1]
    return jsonify(tasks), 201


@app.route("/api/media/<path:media_id>/file/revert", methods=["POST"])
def revert_media_file_route(media_id):
    """Undoes a download for one file -- restores its .strm from the
    link sidecar recorded at download time (see core.write_link_sidecar/
    revert_link_to_stream), removes the real file. The process this
    reverses never throws away the original streaming link specifically
    so this stays possible."""
    settings = store.get_settings()
    file_rel_path = (request.args.get("path") or "").strip()
    if not file_rel_path:
        return jsonify({"error": "path is required"}), 400
    try:
        media_library.revert_file_to_stream(settings.get("export_root", ""), media_id, file_rel_path)
    except (FileNotFoundError, ValueError):
        return jsonify({"error": "not found"}), 404
    return jsonify({"ok": True})


def _env_flag(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def main():
    # Env var defaults (FEBARR_HOST/FEBARR_PORT) let a container's CMD
    # stay plain `python app.py` with the bind address set via -e
    # instead of baking it into the image; an explicit --host/--port
    # flag still wins over either. Only relevant if something runs the
    # Flask dev server directly -- the Docker image's own CMD uses
    # gunicorn instead (see Dockerfile), which binds independently.
    parser = argparse.ArgumentParser(description="Febarr export server")
    parser.add_argument("--host", default=os.environ.get("FEBARR_HOST", "127.0.0.1"),
                         help="Bind address (default: 127.0.0.1, localhost-only)")
    parser.add_argument("--port", type=int, default=int(os.environ.get("FEBARR_PORT", "5000")),
                         help="Port (default: 5000)")
    parser.add_argument("--debug", action="store_true", default=_env_flag("FEBARR_DEBUG"),
                         help="Enable Flask debug/reload mode")
    args = parser.parse_args()

    manager.start()
    print(f"Febarr running at http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, threaded=True, debug=args.debug, use_reloader=False)


if __name__ == "__main__":
    main()
