"""
state.py
========
Thread-safe, file-backed persistence for Febarr's settings and task queue.
Everything lives in a single JSON file (data/state.json) so the app can
restart without losing queued/finished tasks or saved settings. Writes are
atomic (write to a temp file, then os.replace) to avoid corrupting the
file if the process is killed mid-write.
"""

import json
import os
import platform
import secrets
import threading
from copy import deepcopy

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# FEBARR_DATA_DIR overrides where state.json lives -- the Docker image
# sets this to a fixed path (/data) so a volume mounted there survives a
# container recreate regardless of where the code itself is COPYed to;
# unset (plain `python app.py`, no container) keeps the original
# "next to the app" default.
DATA_DIR = os.environ.get("FEBARR_DATA_DIR") or os.path.join(BASE_DIR, "data")
STATE_FILE = os.path.join(DATA_DIR, "state.json")


def _default_export_root() -> str:
    # FEBARR_DEFAULT_EXPORT_ROOT overrides the *first-run* default only
    # -- once state.json exists, whatever's saved there wins regardless
    # of this env var. The Docker image sets it to /exports (a sensible
    # volume-mount target); ~/Media stays the default everywhere else.
    override = os.environ.get("FEBARR_DEFAULT_EXPORT_ROOT")
    if override:
        return override.replace("\\", "/")
    return os.path.expanduser("~/Media").replace("\\", "/")


def _default_flaresolverr_url() -> str:
    # Same first-run-only override pattern as _default_export_root() --
    # the Docker Compose file's optional flaresolverr service is reached
    # by its service name (http://flaresolverr:8191/v1), not localhost,
    # since it's a separate container. Set FEBARR_DEFAULT_FLARESOLVERR_URL
    # to skip the manual Settings edit that'd otherwise take on first run.
    return os.environ.get("FEBARR_DEFAULT_FLARESOLVERR_URL") or "http://localhost:8191/v1"


DEFAULT_SETTINGS = {
    "login_cookie": "",
    "flaresolverr_url": _default_flaresolverr_url(),
    "export_root": _default_export_root(),
    "max_parallel_workers": 2,
    # Same idea as max_parallel_workers, but for download tasks -- kept
    # separate (and defaulting lower) since a download is a large
    # sequential byte transfer, not a lightweight metadata scrape, and
    # shouldn't be sized the same way. See tasks.TaskManager._try_dispatch.
    "max_parallel_downloads": 1,
    # Pauses the task queue: no new queued task is started (running ones
    # finish normally). Persisted so a pause sticks across a restart --
    # advancing a specific task past the pause is a live-only action,
    # see TaskManager.run_now(). Applies to both export and download
    # queues -- there's just the one pause switch.
    "tasks_paused": False,
    # A finished task auto-resyncs (refreshes streaming links, which
    # Febbox expires) once its last sync is this many days old. 0
    # disables auto-resync -- tasks can still be resynced manually at
    # any time.
    "resync_stale_days": 0,
    # Optional TheMovieDB API key (https://www.themoviedb.org/settings/api,
    # free) used to auto-classify shares into TV/Movies/Anime/Anime Movies.
    # Without it, classification falls back to filename heuristics.
    "tmdb_api_key": "",
    # A share whose root has bare-year folders ("2015", "2021", ...) --
    # organizational buckets, not titles themselves -- skips any of them
    # older than this outright, without even listing what's inside. 0
    # disables the check (list everything, same as before this existed).
    "min_release_year": 0,
    # On by default (matches every folder before this setting existed):
    # a TV/Anime export folder is named "Show Name (Year)", same as a
    # movie. Turn off to drop the year for series only -- "Show Name" --
    # for libraries/media servers that expect one folder per series
    # regardless of first-air year. Movies always keep their year either
    # way. See tasks.year_belongs_in_folder()/compute_target_path().
    "series_year_in_folder": True,
    # Off by default -- a confident match normally lands as a
    # "Discovered" library item and waits for an explicit "Queue" click
    # (see media_library.augment_with_pipeline()/app.py's
    # /api/discovered/<id>/queue). Turning this on skips that step: see
    # analyzer.AnalyzeManager._run(), which queues the export itself the
    # moment it finds a match instead of just adding it to the library.
    "auto_queue_discovered": False,
    # Login gate for the web UI. Opt-in: while auth_username/
    # auth_password_hash are unset, the app stays open (matches behavior
    # before this existed). Set both via /api/account, never through the
    # generic settings patch below. app_secret_key signs Flask's session
    # cookie -- generated once on first use, see get_or_create_secret_key().
    "auth_username": "",
    "auth_password_hash": "",
    "app_secret_key": "",
    # -- Advanced ------------------------------------------------------
    # Per-request timeout (seconds) for direct (non-FlareSolverr) Febbox
    # API calls. See core.FebboxAPI._get_json_direct().
    "request_timeout_seconds": 20,
    # How many times a failed direct request (HTTP 429/5xx) is retried
    # before giving up. See core.FebboxAPI.MAX_RETRIES.
    "max_retries": 5,
    # Safety cap on how deep discover_tree() will recurse into a share's
    # folder structure. See core.MAX_DEPTH.
    "max_folder_depth": 8,
    # Overrides core.DEFAULT_USER_AGENT when set; empty uses the default.
    "user_agent": "",
    # How often (seconds) the Activity page polls the server for queue
    # updates. Purely a frontend setting -- see app.js's poll loop.
    "queue_poll_seconds": 1.5,
}

# Settings the client is never sent verbatim -- see mask_secret().
SECRET_SETTINGS = {"login_cookie", "tmdb_api_key"}

# Settings that never leave the server at all, masked or otherwise, and
# can only be changed through their own dedicated (non-generic) code path.
INTERNAL_SETTINGS = {"auth_password_hash", "app_secret_key"}


def mask_secret(value: str) -> str:
    """Returns a display-safe stand-in for a secret, e.g. '••••cd12'."""
    if not value:
        return ""
    tail = value[-4:] if len(value) > 4 else value
    return "\u2022" * 4 + tail


class StateStore:
    def __init__(self, path: str = STATE_FILE):
        self.path = path
        self._lock = threading.RLock()
        self._settings = dict(DEFAULT_SETTINGS)
        self._tasks = []  # list of task dicts, in creation order
        self._discovered = []  # list of discovered-item dicts, in creation order
        self._saved_links = []  # list of saved-link dicts, in creation order -- see links.py
        self._load()

    # -- persistence -------------------------------------------------
    def _load(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return
        with self._lock:
            loaded_settings = data.get("settings") or {}
            # Only known keys survive a load -- a setting retired from
            # DEFAULT_SETTINGS (e.g. a removed feature's config) quietly
            # drops out of state.json on the next save instead of
            # lingering forever just because an old file still has it.
            loaded_settings = {k: v for k, v in loaded_settings.items() if k in DEFAULT_SETTINGS}
            self._settings = {**DEFAULT_SETTINGS, **loaded_settings}
            self._tasks = data.get("tasks") or []
            self._discovered = data.get("discovered") or []
            self._saved_links = data.get("saved_links") or []

    def _save_locked(self) -> None:
        """Caller must hold self._lock."""
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        tmp_path = self.path + ".tmp"
        payload = {
            "settings": self._settings,
            "tasks": self._tasks,
            "discovered": self._discovered,
            "saved_links": self._saved_links,
        }
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        os.replace(tmp_path, self.path)

    def save(self) -> None:
        with self._lock:
            self._save_locked()

    # -- settings ------------------------------------------------------
    def get_settings(self) -> dict:
        with self._lock:
            return deepcopy(self._settings)

    def get_settings_masked(self) -> dict:
        with self._lock:
            out = deepcopy(self._settings)
            auth_configured = bool(self._settings.get("auth_username") and self._settings.get("auth_password_hash"))
        out["login_cookie_set"] = bool(self._settings.get("login_cookie"))
        out["tmdb_api_key_set"] = bool(self._settings.get("tmdb_api_key"))
        for key in INTERNAL_SETTINGS:
            out.pop(key, None)
        out["auth_configured"] = auth_configured
        return out

    def update_settings(self, patch: dict) -> dict:
        """
        Applies a partial update.
        INTERNAL_SETTINGS keys are silently ignored here even if present --
        see set_account() for the only way to change credentials.
        """
        with self._lock:
            for key, value in patch.items():
                if key not in DEFAULT_SETTINGS or key in INTERNAL_SETTINGS:
                    continue
                self._settings[key] = value
            self._save_locked()
            return deepcopy(self._settings)

    # -- auth --------------------------------------------------------------
    def get_or_create_secret_key(self) -> str:
        """Flask session-signing key. Generated once and persisted so
        logins survive a server restart instead of being invalidated."""
        with self._lock:
            if not self._settings.get("app_secret_key"):
                self._settings["app_secret_key"] = secrets.token_hex(32)
                self._save_locked()
            return self._settings["app_secret_key"]

    def set_account(self, username: str, password_hash: str = None) -> None:
        """password_hash=None leaves the existing password untouched
        (e.g. changing just the username)."""
        with self._lock:
            self._settings["auth_username"] = username
            if password_hash is not None:
                self._settings["auth_password_hash"] = password_hash
            self._save_locked()

    def clear_account(self) -> None:
        """Turns auth back off -- the app reverts to open access."""
        with self._lock:
            self._settings["auth_username"] = ""
            self._settings["auth_password_hash"] = ""
            self._save_locked()

    # -- tasks -----------------------------------------------------------
    def get_tasks(self) -> list:
        with self._lock:
            return deepcopy(self._tasks)

    def replace_tasks(self, tasks: list) -> None:
        with self._lock:
            self._tasks = deepcopy(tasks)
            self._save_locked()

    # -- discovered items --------------------------------------------------
    # Titles Analyze found a confident TMDB match for but that haven't
    # been queued as a real export task yet -- see discovered.py. Same
    # persistence shape/pattern as tasks above, just a separate list.
    def get_discovered(self) -> list:
        with self._lock:
            return deepcopy(self._discovered)

    def replace_discovered(self, items: list) -> None:
        with self._lock:
            self._discovered = deepcopy(items)
            self._save_locked()

    # -- saved links -----------------------------------------------------
    # Every Febbox share link ever submitted for Analyze -- the Links
    # page's permanent record, so a share can be re-checked for new
    # files later without digging up the URL again. See links.py.
    def get_saved_links(self) -> list:
        with self._lock:
            return deepcopy(self._saved_links)

    def replace_saved_links(self, items: list) -> None:
        with self._lock:
            self._saved_links = deepcopy(items)
            self._save_locked()
