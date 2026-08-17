"""
analyzer.py
===========
Runs share analysis (grouping + classification -- see grouping.py/
classify.py) as a background job instead of blocking the request that
triggered it, and instead of collecting everything into a batch for the
user to manually confirm, streams straight through: every group with a
confident TMDB match is added to the library (see discovered.py) the
moment it's found (via grouping.iter_share_groups() -- see there for why
"folder" groups stream but "fids" groups can't), no review step -- but
also no export task yet either. Queuing the actual export is a separate,
explicit action taken from Movies/Series once it's there (app.py's
/api/discovered/<id>/queue). Anything without a match (source ==
"heuristic" -- filename/structure guesswork, not a real title lookup),
or a title already covered by an existing discovered item / task / disk
export, is recorded as "denied" instead -- shown to the user, never
added twice. Febbox hasn't shown itself to need throttling here beyond
what FebboxAPI's own retry/backoff already does on an actual 429/5xx.

A share with several folders means several FlareSolverr round-trips (one
listing per folder, plus TMDB lookups); without running this as a
background job, the UI would show nothing at all -- not even in the task
queue -- until the whole thing finished. Now it appears immediately as a
running, "background"-tagged job in the same queue real export tasks and
Find searches live in, exactly like those two, and results (both
discovered titles and denied groups) show up live as they're found
rather than only once the whole share has been walked.

Ephemeral by design: jobs live in memory only, not persisted to disk.
Losing an in-progress analysis on a server restart just means re-pasting
the link -- a low enough cost that persisting a one-shot lookup isn't
worth the complexity. (Discovered items it already recorded before a
restart are real DiscoveredManager records, though, and survive fine.)

ctx.log()/set_current() feed a real per-job log + "currently listing X"
label (same shape as a task's), and ctx.check_cancelled() is wired to a
per-job cancel flag so a job that's taking a while (a big/deep share, or
one Febbox is rate-limiting) can be stopped instead of waited out --
whatever it already added to the library before being cancelled stays
there.
"""

import datetime
import threading
import uuid

from . import classify, core, grouping, media_library
from .tasks import compute_target_path, year_belongs_in_folder


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


class AnalyzeManager:
    def __init__(self, store, task_manager, discovered_manager, links_manager=None):
        self.store = store
        self.task_manager = task_manager
        self.discovered_manager = discovered_manager
        self.links_manager = links_manager  # optional -- see links.SavedLinksManager.mark_checked()
        self._lock = threading.RLock()
        self._jobs = {}            # id -> job dict
        self._order = []           # ids, creation order
        self._cancel_events = {}   # id -> threading.Event, only while running

    def start(self, share_link: str) -> dict:
        share_link = (share_link or "").strip()
        if not share_link:
            raise ValueError("share_link is required")

        if self.links_manager is not None:
            self.links_manager.remember(share_link)

        job_id = uuid.uuid4().hex[:10]
        job = {
            "id": job_id,
            "share_link": share_link,
            "share_key": core.extract_key(share_link),
            "status": "running",
            "discovered": [],   # confident TMDB matches, added to the library (not queued)
            "denied": [],       # no confident match (or a duplicate) -- shown, never added
            "tmdb_available": None,
            "error": None,
            "started_at": _now_iso(),
            "finished_at": None,
            "current": None,
            "log": [],
        }
        cancel_event = threading.Event()
        with self._lock:
            self._jobs[job_id] = job
            self._order.append(job_id)
            self._cancel_events[job_id] = cancel_event

        threading.Thread(
            target=self._run, args=(job_id, cancel_event), name=f"febarr-analyze-{job_id}", daemon=True
        ).start()
        return dict(job)

    def _log(self, job_id: str, msg: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job["log"].append(msg)

    def _set_current(self, job_id: str, label: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job["current"] = label

    def _record(self, job_id: str, bucket: str, entry: dict) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job[bucket].append(entry)

    def _run(self, job_id: str, cancel_event: threading.Event) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            share_link = job["share_link"]
            share_key = job["share_key"]

        settings = self.store.get_settings()
        ctx = core.RunContext(
            log_fn=lambda msg: self._log(job_id, msg),
            is_cancelled=cancel_event.is_set,
            set_current_fn=lambda label: self._set_current(job_id, label),
        )
        flaresolverr = core.FlareSolverr(settings["flaresolverr_url"])
        try:
            flaresolverr.detect()
        except Exception:
            pass
        api = core.FebboxAPI(ctx, flaresolverr=flaresolverr, login_cookie=settings["login_cookie"])

        error = None
        cancelled = False
        tmdb = classify.TMDBClient(settings.get("tmdb_api_key", ""))
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job["tmdb_available"] = tmdb.available
        min_release_year = int(settings.get("min_release_year") or 0) or None
        export_root = settings.get("export_root", "")
        auto_queue_discovered = bool(settings.get("auto_queue_discovered"))
        # Anything this share's groups shouldn't be re-added as, checked
        # fresh before each add and grown as this run itself adds more --
        # covers a title already in the library, already queued/exported,
        # or (rarely) two groups in the same share classifying to the
        # same title.
        seen_target_ids = {t["target_path"] for t in self.task_manager.list_tasks()}
        seen_target_ids |= self.discovered_manager.existing_target_ids()
        try:
            ctx.set_current(f"listing '{share_key}'...")
            for i, group in enumerate(grouping.iter_share_groups(api, share_key, ctx=ctx, min_release_year=min_release_year)):
                ctx.check_cancelled()
                ctx.set_current(f"classifying '{group['raw_name']}' ({i + 1} so far)")
                result = classify.classify_group(
                    group["raw_name"],
                    tmdb,
                    group.get("sample_filenames"),
                    file_count=group.get("file_count"),
                    has_season_subfolder=group.get("has_season_subfolder", False),
                )
                if result["source"] == "tmdb":
                    target_id = compute_target_path(
                        result["category"], result["title"], result["year"],
                        include_year=year_belongs_in_folder(result["category"], settings),
                    )
                    already = target_id in seen_target_ids or media_library.target_path_exists(export_root, target_id)
                    if already:
                        ctx.log(f"  Skipped '{result['title']}' -- already in the library.")
                        self._record(job_id, "denied", {
                            "raw_name": group["raw_name"],
                            "title": result["title"],
                            "year": result["year"],
                            "category": result["category"],
                            "reason": "already in the library",
                        })
                        continue
                    try:
                        item = self.discovered_manager.add(
                            share_link=share_link,
                            share_key=share_key,
                            kind=group["kind"],
                            root_fid=group["root_fid"],
                            root_items=group["root_items"],
                            category=result["category"],
                            title=result["title"],
                            year=result["year"],
                            source=result["source"],
                            confidence=result["confidence"],
                            tmdb_id=result.get("tmdb_id"),
                        )
                        seen_target_ids.add(target_id)
                        # Auto-queue (Settings -> Library, off by default):
                        # skips the manual "Queue" click a Discovered item
                        # normally waits for -- same call app.py's
                        # /api/discovered/<id>/queue makes, just fired
                        # immediately instead of on a user click. Falls
                        # back to a normal Discovered item on failure
                        # (same as the setting being off) rather than
                        # losing the match entirely.
                        queued = False
                        if auto_queue_discovered:
                            try:
                                self.task_manager.create_task_from_proposal(
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
                                self.discovered_manager.remove(item["id"])
                                queued = True
                            except (ValueError, KeyError) as e:
                                ctx.log(f"  Couldn't auto-queue '{result['title']}': {e}")
                        suffix = " and queued it for export" if queued else ""
                        ctx.log(f"  Added '{result['title']}' ({result['category']}) to the library{suffix} -- matched via TMDB.")
                        self._record(job_id, "discovered", {
                            "raw_name": group["raw_name"],
                            "title": result["title"],
                            "year": result["year"],
                            "category": result["category"],
                            "confidence": result["confidence"],
                            "discovered_id": item["id"],
                        })
                    except ValueError as e:
                        # Same "don't lose it" principle as everywhere
                        # else here -- an add failure (a bad
                        # kind/root_fid combination, say) doesn't get
                        # silently swallowed, it falls through to denied
                        # so it's still visible, just not added.
                        ctx.log(f"  Couldn't add '{result['title']}': {e}")
                        self._record(job_id, "denied", {
                            "raw_name": group["raw_name"],
                            "title": result["title"],
                            "year": result["year"],
                            "category": result["category"],
                            "reason": str(e),
                        })
                else:
                    ctx.log(f"  Denied '{group['raw_name']}' -- no confident TMDB match (guessed '{result['title']}').")
                    self._record(job_id, "denied", {
                        "raw_name": group["raw_name"],
                        "title": result["title"],
                        "year": result["year"],
                        "category": result["category"],
                        "reason": "no confident TMDB match" if tmdb.available else "no TMDB key configured",
                    })
        except core.Cancelled:
            cancelled = True
            ctx.log("Cancelled.")
        except Exception as e:
            error = f"Couldn't list this share: {e}"
            ctx.log(f"Error: {e}")
        finally:
            try:
                flaresolverr.destroy_session()
            except Exception:
                pass

        with self._lock:
            self._cancel_events.pop(job_id, None)
            job = self._jobs.get(job_id)
            if job is None:
                return
            job["finished_at"] = _now_iso()
            job["current"] = None
            if cancelled:
                job["status"] = "cancelled"
            elif error:
                job["status"] = "error"
                job["error"] = error
            else:
                job["status"] = "done"
            share_key = job["share_key"]

        if self.links_manager is not None:
            self.links_manager.mark_checked(share_key)

    def get_job(self, job_id: str):
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job is not None else None

    def list_jobs(self) -> list:
        with self._lock:
            return [dict(self._jobs[jid]) for jid in self._order if jid in self._jobs]

    def cancel(self, job_id: str) -> dict:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(job_id)
            if job["status"] != "running":
                raise ValueError("Job isn't running")
            event = self._cancel_events.get(job_id)
        if event is not None:
            event.set()  # noticed at the next check_cancelled() -- see core.RunContext
        return self.get_job(job_id)

    def dismiss(self, job_id: str) -> None:
        with self._lock:
            if job_id not in self._jobs:
                raise KeyError(job_id)
            if self._jobs[job_id]["status"] == "running":
                raise ValueError("Can't dismiss a job that's still running")
            del self._jobs[job_id]
            self._order.remove(job_id)
