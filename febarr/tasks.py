"""
tasks.py
========
TaskManager: owns the in-memory task queue, a dispatcher thread that starts
queued tasks as capacity frees up (respecting a live-editable
max_parallel_workers setting), per-task item tracking (for real progress
bars and resumability), and an auto-resync scheduler that keeps finished
exports' streaming links fresh. Backed by state.StateStore for persistence
across restarts.

Resumability & crash-friendliness, in one sentence: every task's item list
(one entry per video file, each with its own status) is persisted after
every change, a task interrupted by a server restart is requeued -- not
failed -- on the next startup, and re-running an export only processes
items that aren't already marked "done", so work already completed is
never redone.
"""

import datetime
import threading
import time
import uuid

from . import core
from .state import StateStore

LOG_MAX_LINES = 400
DISPATCH_POLL_SECONDS = 0.5
AUTO_RESYNC_POLL_SECONDS = 60
# How often a download's byte-level progress is actually flushed to disk
# (state.json) -- every chunk would mean rewriting the whole task list on
# every ~1MiB of a multi-GB transfer. In-memory reads (GET /api/tasks)
# always see the live number regardless; see TaskManager._update_item_progress.
PROGRESS_FLUSH_SECONDS = 2.0

TERMINAL_STATUSES = {"done", "error", "cancelled"}
# "cancelled" is an internal transition only, never a status a user is
# meant to see settle -- see cancel_task()/_run_task()'s finally block
# and TaskManager.on_settled. Only "error" is ever resumable in
# practice, but TERMINAL_STATUSES above keeps "cancelled" too since a
# task passes through it (briefly, or as the delete_task() precondition)
# on its way to being converted back into a library item or removed.
RESUMABLE_STATUSES = {"error"}

CATEGORY_LABELS = {
    "tv": "TV",
    "movie": "Movies",
    "anime": "Anime",
    "anime_movie": "Anime Movies",
}


SERIES_CATEGORIES = {"tv", "anime"}


def year_belongs_in_folder(category: str, settings: dict) -> bool:
    """Whether compute_target_path() should suffix " (Year)" onto this
    category's folder name. Always yes for movies/anime movies; for
    TV/Anime it's gated by the "series_year_in_folder" setting (default
    on, matching every folder before this setting existed) -- some media
    servers expect a bare "Show Name" folder per series, not one that
    forks by year."""
    if category not in SERIES_CATEGORIES:
        return True
    return bool((settings or {}).get("series_year_in_folder", True))


def compute_target_path(category: str, title: str, year, include_year: bool = True) -> str:
    """The Category/Title (Year) convention every auto-classified export
    uses -- shared so "where would this title's folder be" is computed
    identically everywhere instead of duplicated and risking drift.
    include_year=False drops the " (Year)" suffix -- see
    year_belongs_in_folder(), the settings-aware gate every caller that
    has a settings dict handy should go through instead of passing this
    directly."""
    if category not in CATEGORY_LABELS:
        raise ValueError(f"invalid category: {category!r}")
    safe_title = core.sanitize_name((title or "").strip() or "Untitled")
    year_int = None
    if year not in (None, ""):
        try:
            year_int = int(year)
        except (TypeError, ValueError):
            year_int = None
    suffix = f" ({year_int})" if year_int and include_year else ""
    return f"{CATEGORY_LABELS[category]}/{safe_title}{suffix}"


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def _parse_iso(value):
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return None


class TaskHandle:
    """
    Implements the ctx protocol core.py's pipeline drives (see
    core.RunContext for the interface docstring), bound to one task id.
    Every method just delegates to a TaskManager method that takes the
    lock and persists -- this class holds no state of its own.
    """

    def __init__(self, manager: "TaskManager", task_id: str):
        self._manager = manager
        self._task_id = task_id

    def log(self, msg: str) -> None:
        self._manager._append_log(self._task_id, msg)

    def check_cancelled(self) -> None:
        if self._manager._is_cancelled(self._task_id):
            raise core.Cancelled()

    def set_phase(self, phase) -> None:
        self._manager._set_phase(self._task_id, phase)

    def set_current(self, label: str) -> None:
        self._manager._set_current(self._task_id, label)

    def upsert_items(self, items: list) -> None:
        self._manager._upsert_items(self._task_id, items)

    def get_items(self) -> list:
        return self._manager._get_items_snapshot(self._task_id)

    def mark_item(self, fid, status: str, error: str = None, quality: str = None, url: str = None) -> None:
        self._manager._mark_item(self._task_id, fid, status, error=error, quality=quality, url=url)

    def update_item_progress(self, fid, bytes_downloaded: int, bytes_total) -> None:
        self._manager._update_item_progress(self._task_id, fid, bytes_downloaded, bytes_total)


class TaskManager:
    def __init__(self, store: StateStore):
        self.store = store
        self._lock = threading.RLock()
        self._records = {}          # id -> task dict
        self._order = []            # ids, creation order
        self._cancel_events = {}    # id -> threading.Event (running/queued tasks only)
        self._advance_exempt = set()  # task ids that get to start despite the queue being paused
        self._last_progress_flush = {}  # task id -> time.monotonic() of last disk flush, downloads only
        self._stop_event = threading.Event()
        self._dispatcher_thread = None
        self._resync_thread = None
        # Optional callback(task_dict), set from app.py -- fired exactly
        # once a task actually finishes cancelling (queued: immediately,
        # inline; running: once its worker notices, from _run_task's
        # finally block). app.py's handler converts it into a discovered
        # library item and deletes the task record, which is the whole
        # reason "cancelled" never persists as a status a user sees --
        # see cancel_task().
        self.on_settled = None

        self._load_from_store()

    # -- startup ---------------------------------------------------------
    def _load_from_store(self) -> None:
        loaded = self.store.get_tasks()
        with self._lock:
            for task in loaded:
                self._migrate_task(task)
                if task["status"] == "running":
                    # No worker actually holds this task anymore -- the
                    # process that was running it is gone. Requeue it
                    # rather than failing it: discovery + export both
                    # skip work already recorded as done, so this just
                    # continues from wherever it got to.
                    task["status"] = "queued"
                    task["phase"] = None
                    task.setdefault("log", []).append(
                        "[server] Resuming: server restarted while this task was running."
                    )
                self._records[task["id"]] = task
                self._order.append(task["id"])
            self._persist_locked()

    @staticmethod
    def _migrate_task(task: dict) -> None:
        """Fills in fields older task records (pre item-tracking, pre
        auto-classification, pre-download) won't have."""
        task.setdefault("items", [])
        task.setdefault("total_items", len(task["items"]))
        task.setdefault("items_failed", 0)
        task.setdefault("phase", None)
        task.setdefault("last_synced_at", None)
        task.setdefault("current_item", "")
        task.setdefault("files_written", sum(1 for it in task["items"] if it.get("status") == "done"))
        task.pop("items_seen", None)
        # "export" (the original, only kind before downloads existed) or
        # "download" -- see create_download_task()/_run_task(). Every
        # other field on a task record is shared by both types.
        task.setdefault("task_type", "export")
        for item in task["items"]:
            item.setdefault("url", None)
            item.setdefault("bytes_downloaded", 0)
            item.setdefault("bytes_total", None)
        # Auto-classification fields -- absent/None means "walk the whole
        # share" (root_kind falls through to share_root in core.run_export),
        # i.e. exactly the old manually-targeted behavior.
        task.setdefault("root_kind", "share_root")
        task.setdefault("root_fid", None)
        task.setdefault("root_items", None)
        task.setdefault("category", None)
        task.setdefault("detected_title", None)
        task.setdefault("detected_year", None)
        task.setdefault("classification_source", None)
        task.setdefault("classification_confidence", None)
        task.setdefault("tmdb_id", None)

    def start(self) -> None:
        if not (self._dispatcher_thread and self._dispatcher_thread.is_alive()):
            self._stop_event.clear()
            self._dispatcher_thread = threading.Thread(
                target=self._dispatch_loop, name="febarr-dispatcher", daemon=True
            )
            self._dispatcher_thread.start()
        if not (self._resync_thread and self._resync_thread.is_alive()):
            self._resync_thread = threading.Thread(
                target=self._auto_resync_loop, name="febarr-auto-resync", daemon=True
            )
            self._resync_thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    # -- persistence -------------------------------------------------------
    def _persist_locked(self) -> None:
        """Caller must hold self._lock."""
        snapshot = [self._records[tid] for tid in self._order if tid in self._records]
        self.store.replace_tasks(snapshot)

    # -- dispatcher --------------------------------------------------------
    def _dispatch_loop(self) -> None:
        while not self._stop_event.is_set():
            self._try_dispatch()
            self._stop_event.wait(DISPATCH_POLL_SECONDS)

    @staticmethod
    def _positive_int(value, default: int) -> int:
        try:
            return max(1, int(value))
        except (TypeError, ValueError):
            return default

    def _try_dispatch(self) -> None:
        with self._lock:
            settings = self.store.get_settings()
            max_workers = self._positive_int(settings.get("max_parallel_workers", 2), 2)
            max_downloads = self._positive_int(settings.get("max_parallel_downloads", 1), 1)
            paused = bool(settings.get("tasks_paused"))

            def is_download(tid):
                return self._records.get(tid, {}).get("task_type") == "download"

            running_export = 0
            running_download = 0
            for t in self._records.values():
                if t["status"] != "running":
                    continue
                if t.get("task_type") == "download":
                    running_download += 1
                else:
                    running_export += 1

            def queued_of(want_download: bool):
                ids = [
                    tid for tid in self._order
                    if self._records.get(tid, {}).get("status") == "queued"
                    and is_download(tid) == want_download
                ]
                if paused:
                    # Only tasks explicitly advanced past the pause (see
                    # run_now()) may start; everything else waits for resume.
                    ids = [tid for tid in ids if tid in self._advance_exempt]
                return ids

            # Two independent pools -- export and download tasks never
            # compete for each other's slots, so a handful of large
            # downloads can't starve the (lightweight, metadata-only)
            # export queue, or vice versa. Both still honor the one
            # shared pause switch.
            to_start = (
                queued_of(False)[: max(0, max_workers - running_export)]
                + queued_of(True)[: max(0, max_downloads - running_download)]
            )
            for tid in to_start:
                task = self._records[tid]
                task["status"] = "running"
                task["started_at"] = _now_iso()
                task["error"] = None
                self._cancel_events[tid] = threading.Event()
                self._advance_exempt.discard(tid)
            if to_start:
                self._persist_locked()

        for tid in to_start:
            threading.Thread(
                target=self._run_task, args=(tid,), name=f"febarr-task-{tid}", daemon=True
            ).start()

    # -- pause / advance -----------------------------------------------------
    def set_paused(self, paused: bool) -> None:
        self.store.update_settings({"tasks_paused": bool(paused)})

    def is_paused(self) -> bool:
        return bool(self.store.get_settings().get("tasks_paused"))

    def run_now(self, task_id: str) -> dict:
        """Jumps a queued task to the front of the order and exempts it
        from a paused queue (for itself only) -- still respects
        max_parallel_workers, so if there's no free slot right now it
        just becomes first in line for the next one to open up."""
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                raise KeyError(task_id)
            if task["status"] != "queued":
                raise ValueError("Only a queued task can be advanced")
            self._order.remove(task_id)
            self._order.insert(0, task_id)
            self._advance_exempt.add(task_id)
            self._persist_locked()
            return dict(task)

    def queue_status(self) -> dict:
        # "running"/"queued" stay export-only (their pre-download
        # meaning -- every task was an export task before this existed);
        # the download pool gets its own counters alongside.
        with self._lock:
            is_dl = lambda t: t.get("task_type") == "download"  # noqa: E731
            running = sum(1 for t in self._records.values() if t["status"] == "running" and not is_dl(t))
            queued = sum(1 for t in self._records.values() if t["status"] == "queued" and not is_dl(t))
            running_downloads = sum(1 for t in self._records.values() if t["status"] == "running" and is_dl(t))
            queued_downloads = sum(1 for t in self._records.values() if t["status"] == "queued" and is_dl(t))
        settings = self.store.get_settings()
        max_workers = self._positive_int(settings.get("max_parallel_workers", 2), 2)
        max_downloads = self._positive_int(settings.get("max_parallel_downloads", 1), 1)
        return {
            "paused": bool(settings.get("tasks_paused")),
            "running": running,
            "queued": queued,
            "max_parallel_workers": max_workers,
            "running_downloads": running_downloads,
            "queued_downloads": queued_downloads,
            "max_parallel_downloads": max_downloads,
        }

    # -- auto-resync scheduler ----------------------------------------------
    def _auto_resync_loop(self) -> None:
        while not self._stop_event.is_set():
            self._check_auto_resync()
            self._stop_event.wait(AUTO_RESYNC_POLL_SECONDS)

    def _check_auto_resync(self) -> None:
        settings = self.store.get_settings()
        try:
            days = float(settings.get("resync_stale_days") or 0)
        except (TypeError, ValueError):
            days = 0
        if days <= 0:
            return

        threshold = datetime.timedelta(days=days)
        now = datetime.datetime.now(datetime.timezone.utc)
        due = []
        with self._lock:
            for tid, task in self._records.items():
                if task["status"] != "done" or task.get("task_type") == "download":
                    continue  # nothing to resync -- a finished download has no .strm links left
                last = _parse_iso(task.get("last_synced_at") or task.get("finished_at"))
                if last is None or (now - last) >= threshold:
                    due.append(tid)

        for tid in due:
            try:
                self.resync_task(tid, note="[server] Auto-resync: refreshing streaming links.")
            except (KeyError, ValueError):
                pass

    # -- mutation helpers (each takes the lock itself) ----------------------
    def _append_log(self, task_id: str, message: str) -> None:
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                return
            log = task.setdefault("log", [])
            log.append(message)
            if len(log) > LOG_MAX_LINES:
                del log[: len(log) - LOG_MAX_LINES]
            self._persist_locked()

    def _set_phase(self, task_id: str, phase) -> None:
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                return
            task["phase"] = phase
            self._persist_locked()

    def _set_current(self, task_id: str, label: str) -> None:
        # Cosmetic/transient -- deliberately not persisted on every call
        # (it's paired with a log() call almost everywhere it's set,
        # which already triggers a flush).
        with self._lock:
            task = self._records.get(task_id)
            if task is not None:
                task["current_item"] = label

    def _upsert_items(self, task_id: str, new_items: list) -> None:
        if not new_items:
            return
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                return
            existing_by_fid = {it["fid"]: it for it in task["items"]}
            changed = False
            for ni in new_items:
                fid = ni["fid"]
                existing = existing_by_fid.get(fid)
                if existing is None:
                    task["items"].append({
                        "fid": fid,
                        "rel_path": ni["rel_path"],
                        "dest_path": ni["dest_path"],
                        "status": "pending",
                        "error": None,
                        "quality": "",
                        "url": None,
                        "synced_at": None,
                        "bytes_downloaded": 0,
                        "bytes_total": None,
                    })
                    changed = True
                elif existing["rel_path"] != ni["rel_path"] or existing["dest_path"] != ni["dest_path"]:
                    existing["rel_path"] = ni["rel_path"]
                    existing["dest_path"] = ni["dest_path"]
                    changed = True
            if changed:
                task["total_items"] = len(task["items"])
                self._recompute_counts(task)
                self._persist_locked()

    def _get_items_snapshot(self, task_id: str) -> list:
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                return []
            return [dict(it) for it in task["items"]]

    def _mark_item(self, task_id: str, fid, status: str, error: str = None, quality: str = None, url: str = None) -> None:
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                return
            for item in task["items"]:
                if item["fid"] == fid:
                    item["status"] = status
                    item["error"] = error
                    if quality is not None:
                        item["quality"] = quality
                    if url is not None:
                        item["url"] = url
                    if status == "done":
                        item["synced_at"] = _now_iso()
                    break
            self._recompute_counts(task)
            self._persist_locked()

    def _update_item_progress(self, task_id: str, fid, bytes_downloaded: int, bytes_total) -> None:
        """Byte-level progress for one in-flight download item -- called
        once per chunk (see core.download_one_file's on_progress), so
        unlike _mark_item() this deliberately does NOT persist on every
        call: the in-memory record is always updated (GET /api/tasks
        reads it live), but state.json is only rewritten at most once
        every PROGRESS_FLUSH_SECONDS per task -- same "cosmetic/transient,
        paired with something that already flushes eventually" idea
        _set_current() already uses, just with an explicit periodic
        flush too since a large download can otherwise run for minutes
        between the log()/mark_item() calls that would otherwise persist
        it."""
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                return
            for item in task["items"]:
                if item["fid"] == fid:
                    item["bytes_downloaded"] = bytes_downloaded
                    item["bytes_total"] = bytes_total
                    break
            else:
                return
            now = time.monotonic()
            last = self._last_progress_flush.get(task_id, 0)
            if now - last >= PROGRESS_FLUSH_SECONDS:
                self._last_progress_flush[task_id] = now
                self._persist_locked()

    @staticmethod
    def _recompute_counts(task: dict) -> None:
        items = task["items"]
        task["files_written"] = sum(1 for it in items if it["status"] == "done")
        task["items_failed"] = sum(1 for it in items if it["status"] == "error")

    def _is_cancelled(self, task_id: str) -> bool:
        ev = self._cancel_events.get(task_id)
        return ev.is_set() if ev else False

    # -- worker --------------------------------------------------------------
    def _run_task(self, task_id: str) -> None:
        handle = TaskHandle(self, task_id)

        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                return
            share_link = task["share_link"]
            target_path = task["target_path"]
            root_kind = task.get("root_kind", "share_root")
            root_fid = task.get("root_fid")
            root_items = task.get("root_items")
            task_type = task.get("task_type", "export")

        settings = self.store.get_settings()
        flaresolverr = core.FlareSolverr(settings["flaresolverr_url"])

        handle.log(f"Checking for FlareSolverr at {settings['flaresolverr_url']}...")
        try:
            if flaresolverr.detect():
                handle.log("FlareSolverr detected -- routing requests through its browser.")
            else:
                handle.log("FlareSolverr not found -- continuing with direct requests.")
        except Exception as e:
            handle.log(f"FlareSolverr detection failed ({e}); continuing with direct requests.")

        final_status = "done"
        error_message = None
        try:
            if task_type == "download":
                core.run_download(
                    handle,
                    share_link,
                    target_path,
                    settings["export_root"],
                    flaresolverr,
                    settings["login_cookie"],
                    request_timeout=int(settings.get("request_timeout_seconds") or 20),
                    max_retries=int(settings.get("max_retries") or 5),
                    user_agent=(settings.get("user_agent") or "").strip() or None,
                )
            else:
                core.run_export(
                    handle,
                    share_link,
                    target_path,
                    settings["export_root"],
                    flaresolverr,
                    settings["login_cookie"],
                    root_kind=root_kind,
                    root_fid=root_fid,
                    root_items=root_items,
                    request_timeout=int(settings.get("request_timeout_seconds") or 20),
                    max_retries=int(settings.get("max_retries") or 5),
                    max_folder_depth=int(settings.get("max_folder_depth") or core.MAX_DEPTH),
                    user_agent=(settings.get("user_agent") or "").strip() or None,
                )
        except core.Cancelled:
            final_status = "cancelled"
            handle.log("Cancelled by user.")
        except ValueError as e:
            final_status = "error"
            error_message = str(e)
            handle.log(f"Error: {e}")
        except Exception as e:  # noqa: BLE001 - surface any failure into the task record
            final_status = "error"
            error_message = str(e)
            handle.log(f"Error: {e}")
        finally:
            try:
                flaresolverr.destroy_session()
            except Exception:
                pass

        settled = None
        with self._lock:
            task = self._records.get(task_id)
            if task is not None:
                task["status"] = final_status
                task["error"] = error_message
                task["finished_at"] = _now_iso()
                task["phase"] = None
                task["current_item"] = ""
                if final_status == "done":
                    done = task["files_written"]
                    total = task["total_items"]
                    failed = task["items_failed"]
                    suffix = f" ({failed} failed)" if failed else ""
                    if task.get("task_type") == "download":
                        task.setdefault("log", []).append(
                            f"Download completed: {done}/{total} file(s) downloaded{suffix}."
                        )
                        # Deliberately no last_synced_at bump here -- that
                        # field drives export's auto-resync scheduler
                        # (_check_auto_resync), which download tasks are
                        # excluded from entirely (resyncing a title with
                        # no .strm files left doesn't mean anything).
                    else:
                        task["last_synced_at"] = _now_iso()
                        task.setdefault("log", []).append(
                            f"Export completed: {done}/{total} stream link(s) recorded{suffix}."
                        )
                self._persist_locked()
                if final_status == "cancelled":
                    settled = dict(task)
            self._cancel_events.pop(task_id, None)
            self._last_progress_flush.pop(task_id, None)
        # Outside the lock -- on_settled (app.py) turns this into a
        # discovered library item and deletes the task record, which
        # itself needs the lock. Only reachable via a running task that
        # was asked to stop through cancel_task() (remove_task() never
        # lets a running task get here directly), so this is always the
        # "soft" landing, never a hard removal.
        if settled is not None and self.on_settled:
            self.on_settled(settled)

    # -- public API used by the Flask routes ---------------------------------
    def list_tasks(self) -> list:
        with self._lock:
            return [dict(self._records[tid]) for tid in self._order if tid in self._records]

    def get_task(self, task_id: str):
        with self._lock:
            task = self._records.get(task_id)
            return dict(task) if task is not None else None

    def retarget_tasks(self, old_target_path: str, new_target_path: str, new_title: str, new_year) -> None:
        """The Movies/Series detail page's "edit path" control
        (media_library.rename_media() handles the disk side) needs every
        task record -- export or download, any status -- that was keyed
        to the old Category/Title (Year) id pointed at the new one, so
        the title keeps resolving to the same task/download history
        under its new name instead of orphaning it."""
        with self._lock:
            changed = False
            for task in self._records.values():
                if task.get("target_path") == old_target_path:
                    task["target_path"] = new_target_path
                    task["detected_title"] = new_title
                    task["detected_year"] = new_year
                    changed = True
            if changed:
                self._persist_locked()

    def create_task(self, share_link: str, target_path: str) -> dict:
        """Manual mode: caller supplies the exact target path, and the
        whole share is walked as one unit (no auto-classification/split).
        This is the original single-task-per-share behavior, kept as a
        fallback for edge cases (no TMDB key, unusual layouts, etc.)."""
        share_link = (share_link or "").strip()
        target_path = (target_path or "").strip()
        if not share_link:
            raise ValueError("share_link is required")
        if not target_path:
            raise ValueError("target_path is required")

        return self._create_task_record(
            share_link=share_link,
            target_path=target_path,
            root_kind="share_root",
            root_fid=None,
            root_items=None,
            category=None,
            detected_title=None,
            detected_year=None,
            classification_source=None,
            classification_confidence=None,
        )

    def create_task_from_proposal(
        self,
        share_link: str,
        kind: str,
        root_fid,
        root_items,
        category: str,
        title: str,
        year,
        source: str = "manual",
        confidence: str = "high",
        tmdb_id=None,
    ) -> dict:
        """Auto-classification mode: one task for one detected title group
        (see grouping.py / classify.py). target_path is derived from the
        category + title + year, not supplied by the caller, so exports
        always land under the right Category/Title (Year) folder."""
        share_link = (share_link or "").strip()
        if not share_link:
            raise ValueError("share_link is required")
        if kind not in ("folder", "fids", "share_root"):
            raise ValueError(f"invalid kind: {kind!r}")
        if category not in CATEGORY_LABELS:
            raise ValueError(f"invalid category: {category!r}")
        if kind == "folder" and root_fid is None:
            raise ValueError("root_fid is required for kind=folder")
        if kind == "fids" and not root_items:
            raise ValueError("root_items is required for kind=fids")

        title = (title or "").strip() or "Untitled"
        settings = self.store.get_settings()
        target_path = compute_target_path(category, title, year, include_year=year_belongs_in_folder(category, settings))
        try:
            year_int = int(year) if year not in (None, "") else None
        except (TypeError, ValueError):
            year_int = None

        return self._create_task_record(
            share_link=share_link,
            target_path=target_path,
            root_kind=kind,
            root_fid=root_fid,
            root_items=root_items,
            category=category,
            detected_title=title,
            detected_year=year_int,
            classification_source=source,
            classification_confidence=confidence,
            tmdb_id=tmdb_id,
        )

    def _create_task_record(
        self,
        share_link,
        target_path,
        root_kind,
        root_fid,
        root_items,
        category,
        detected_title,
        detected_year,
        classification_source,
        classification_confidence,
        tmdb_id=None,
        task_type="export",
    ) -> dict:
        task_id = uuid.uuid4().hex[:10]
        task = {
            "id": task_id,
            "task_type": task_type,
            "share_link": share_link,
            "share_key": core.extract_key(share_link),
            "target_path": target_path,
            "status": "queued",
            "created_at": _now_iso(),
            "started_at": None,
            "finished_at": None,
            "phase": None,
            "current_item": "",
            "items": [],
            "total_items": 0,
            "files_written": 0,
            "items_failed": 0,
            "last_synced_at": None,
            "error": None,
            "log": [],
            "root_kind": root_kind,
            "root_fid": root_fid,
            "root_items": root_items,
            "category": category,
            "detected_title": detected_title,
            "detected_year": detected_year,
            "classification_source": classification_source,
            "classification_confidence": classification_confidence,
            "tmdb_id": tmdb_id,
        }
        with self._lock:
            self._records[task_id] = task
            self._order.append(task_id)
            self._persist_locked()
        return dict(task)

    def create_download_task(
        self,
        share_link: str,
        target_path: str,
        items: list,
        category: str,
        title: str,
        year,
        tmdb_id=None,
    ) -> dict:
        """A download task, riding the exact same queue/dispatcher/
        cancel/resume/progress machinery as an export task (see
        _run_task's task_type branch) -- just seeded with a ready-made
        item list instead of discovering one. `items` is
        [{"fid","rel_path","dest_path"}, ...], already filtered down to
        what's actually still a .strm on disk right now -- see
        media_library.build_download_items(), which is what app.py's
        download routes call before this."""
        share_link = (share_link or "").strip()
        if not share_link:
            raise ValueError("share_link is required")
        if not target_path:
            raise ValueError("target_path is required")
        if not items:
            raise ValueError("items is required")
        if category not in CATEGORY_LABELS:
            raise ValueError(f"invalid category: {category!r}")

        task = self._create_task_record(
            share_link=share_link,
            target_path=target_path,
            root_kind="fids",
            root_fid=None,
            root_items=None,
            category=category,
            detected_title=(title or "").strip() or "Untitled",
            detected_year=year,
            classification_source=None,
            classification_confidence=None,
            tmdb_id=tmdb_id,
            task_type="download",
        )
        self._upsert_items(task["id"], items)
        return self.get_task(task["id"])

    def cancel_task(self, task_id: str) -> dict:
        """Stops a queued or running task -- the one and only "soft" stop
        action (Activity keeps it; Movies/Series has it too now, for
        queued/running rows). A queued task resolves instantly, and
        on_settled fires right here so it lands back as a library item
        immediately, no lingering "Cancelled" status to look at; a
        running task can only be asked to stop -- it settles the same
        way, just later, once its worker actually notices (see
        _run_task's finally block). Either way the caller gets the task
        back as it stood the moment this returned, which for the queued
        case may already be gone (converted) by the time on_settled
        finishes -- callers needing the post-settle state should re-fetch."""
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                raise KeyError(task_id)
            settled = None
            if task["status"] == "queued":
                task["status"] = "cancelled"
                task["finished_at"] = _now_iso()
                task.setdefault("log", []).append("[server] Cancelled before it started.")
                self._persist_locked()
                settled = dict(task)
            elif task["status"] == "running":
                ev = self._cancel_events.get(task_id)
                if ev is not None:
                    ev.set()
                task.setdefault("log", []).append("[server] Cancellation requested...")
                self._persist_locked()
            result = dict(task)
        if settled is not None and self.on_settled:
            self.on_settled(settled)
        return result

    def resume_task(self, task_id: str) -> dict:
        """Requeues an errored task. Already-done items are left alone --
        only items that never finished get processed. There's no
        "cancelled" task left lying around to resume -- cancelling one
        settles it straight back into a library item instead (see
        cancel_task()) -- so re-queuing that is Movies/Series' "Queue"
        button on the resulting Discovered row, not this."""
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                raise KeyError(task_id)
            if task["status"] not in RESUMABLE_STATUSES:
                raise ValueError("Only an errored task can be resumed")
            task["status"] = "queued"
            task["error"] = None
            task["finished_at"] = None
            done = task["files_written"]
            total = task["total_items"]
            task.setdefault("log", []).append(
                f"[server] Resuming ({done}/{total} already done, remaining items will continue)."
            )
            self._persist_locked()
            return dict(task)

    def resync_task(self, task_id: str, note: str = None) -> dict:
        """Re-queues a finished task with every item reset to "pending" so
        the next run refreshes all of their streaming links. Discovery
        also re-runs, so newly-added files in the share get picked up.
        Export tasks only -- a finished download task has no .strm links
        left to refresh (see build_download_items/run_download); its
        "done" items are real files now, resync doesn't mean anything
        there."""
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                raise KeyError(task_id)
            if task["status"] != "done" or task.get("task_type") == "download":
                raise ValueError("Only a finished export task can be resynced")
            for item in task["items"]:
                item["status"] = "pending"
                item["error"] = None
            self._recompute_counts(task)
            task["status"] = "queued"
            task["phase"] = None
            task["current_item"] = ""
            task["finished_at"] = None
            task["error"] = None
            task.setdefault("log", []).append(
                note or "[server] Resync requested -- refreshing streaming links for all items."
            )
            self._persist_locked()
            return dict(task)

    def delete_task(self, task_id: str) -> None:
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                raise KeyError(task_id)
            if task["status"] not in TERMINAL_STATUSES:
                raise ValueError("Cannot delete a queued or running task -- cancel it first")
            del self._records[task_id]
            self._order.remove(task_id)
            self._cancel_events.pop(task_id, None)
            self._last_progress_flush.pop(task_id, None)
            self._persist_locked()

    def remove_task(self, task_id: str) -> None:
        """The Movies/Series "Remove" action -- a hard removal, gone from
        the queue *and* the library entirely, as opposed to Cancel (which
        only ever stops a task, then settles it back into a library item
        -- see cancel_task()/on_settled). Deliberately does NOT call the
        public cancel_task() for a queued task -- that would trigger the
        same soft settle and leave a discovered item behind, exactly what
        a hard Remove isn't supposed to do -- so it duplicates that one
        small transition inline instead. An errored task is just deleted
        outright. A running task can't be removed directly -- same rule
        delete_task() already enforces -- it has to be cancelled first
        (Activity, or now Movies/Series too) so its worker thread stops
        touching the record before anything deletes it out from under it."""
        with self._lock:
            task = self._records.get(task_id)
            if task is None:
                raise KeyError(task_id)
            status = task["status"]
            if status == "queued":
                task["status"] = "cancelled"
                task["finished_at"] = _now_iso()
                task.setdefault("log", []).append("[server] Removed before it started.")
                self._persist_locked()
            elif status not in RESUMABLE_STATUSES:
                raise ValueError(
                    "Cancel this task before removing it" if status == "running"
                    else "Only a queued or errored task can be removed"
                )
        self.delete_task(task_id)
