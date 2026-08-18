"""
core.py
=======
The Febbox scraping/export engine. Adapted from the original standalone
febbox_export.py script.

The pipeline runs in two phases so exports are resumable and give real
progress:

  1. Discovery -- walks the share's folder tree and builds a flat list of
     video items (fid, relative path, destination path). Existing items
     (matched by fid) are left alone; only newly-seen ones are added. This
     means re-running discovery on a share you've already exported is
     cheap and safe, and also picks up files added to the share since the
     last run.

  2. Export -- for every item that isn't already marked "done", requests
     its streaming link and writes the .strm file, then marks it done.
     Items already marked "done" are skipped entirely, which is what
     makes resuming a cancelled/crashed/interrupted run just a matter of
     running the pipeline again against the same item list.

A "ctx" object threads through both phases to report logs/progress and to
persist the item list back to whatever owns it (see tasks.TaskHandle) --
core.py itself has no notion of tasks, storage, or threading.

No third-party dependencies -- only the Python standard library, matching
the original script's design.
"""

import datetime
import json
import os
import re
import time
import random
import html
import urllib.request
import urllib.parse
import urllib.error

from . import classify


VIDEO_EXTENSIONS = {".mp4", ".mkv"}
DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) FebboxExportPipeline/1.0"
MAX_DEPTH = 8  # safety cap against unexpectedly deep/cyclical share trees
DOWNLOAD_CHUNK_SIZE = 1024 * 1024  # 1 MiB per read/progress tick
LINK_SIDECAR_EXT = ".febarr-link.json"


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


class Cancelled(Exception):
    """Raised internally to unwind out of the pipeline on cancellation."""


class RunContext:
    """
    Duck-typed protocol the export pipeline drives. Any object with these
    methods can be passed as `ctx` -- core.py doesn't care who implements
    them (tasks.TaskHandle does, for the web app).

    log(msg)                              -- append a log line
    check_cancelled()                     -- raise Cancelled() if asked to stop
    set_phase(phase)                      -- "discovering" | "exporting"
    set_current(label)                    -- what's being worked on right now
    upsert_items(items)                   -- items: [{"fid","rel_path","dest_path"}, ...]
                                              add any not already tracked (by fid)
    get_items()                           -- -> current list of tracked item dicts
                                              (each has at least fid/rel_path/dest_path/status)
    mark_item(fid, status, error=None, quality=None)
                                           -- status: "done" | "error"
    update_item_progress(fid, bytes_downloaded, bytes_total)
                                           -- byte-level progress between
                                              mark_item() calls, for a
                                              download's per-file progress
                                              bar (bytes_total may be None
                                              if the server didn't report
                                              a Content-Length)
    """

    def __init__(
        self,
        log_fn=None,
        is_cancelled=None,
        set_phase_fn=None,
        set_current_fn=None,
        upsert_items_fn=None,
        get_items_fn=None,
        mark_item_fn=None,
        update_item_progress_fn=None,
    ):
        self._log_fn = log_fn or (lambda msg: None)
        self._is_cancelled = is_cancelled or (lambda: False)
        self._set_phase_fn = set_phase_fn or (lambda phase: None)
        self._set_current_fn = set_current_fn or (lambda label: None)
        self._upsert_items_fn = upsert_items_fn or (lambda items: None)
        self._get_items_fn = get_items_fn or (lambda: [])
        self._mark_item_fn = mark_item_fn or (lambda *a, **kw: None)
        self._update_item_progress_fn = update_item_progress_fn or (lambda *a, **kw: None)

    def log(self, msg: str) -> None:
        self._log_fn(msg)

    def check_cancelled(self) -> None:
        if self._is_cancelled():
            raise Cancelled()

    def set_phase(self, phase: str) -> None:
        self._set_phase_fn(phase)

    def set_current(self, label: str) -> None:
        self._set_current_fn(label)

    def upsert_items(self, items: list) -> None:
        if items:
            self._upsert_items_fn(items)

    def get_items(self) -> list:
        return self._get_items_fn()

    def mark_item(self, fid, status: str, error: str = None, quality: str = None, url: str = None) -> None:
        self._mark_item_fn(fid, status, error=error, quality=quality, url=url)

    def update_item_progress(self, fid, bytes_downloaded: int, bytes_total) -> None:
        self._update_item_progress_fn(fid, bytes_downloaded, bytes_total)


# --------------------------------------------------------------------------
# Utilities (mirrors src/utils/febboxExportUtil.js)
# --------------------------------------------------------------------------
def sanitize_name(name: str) -> str:
    """
    Strip characters Windows won't allow in file/folder names (: * ? " < > |),
    plus trailing dots/spaces, so titles like 'Angolmois: Genkou Kassenki'
    don't blow up os.makedirs / open() on Windows. Also applied on
    non-Windows for consistent, portable output.
    """
    cleaned = re.sub(r'[<>:"/\\|?*]', "", name)
    cleaned = cleaned.rstrip(" .")
    return cleaned or "_"


def validate_export_path(export_root: str, dest_path: str) -> str:
    """Resolve dest_path under export_root and ensure it can't escape it."""
    export_root_abs = os.path.abspath(export_root)
    candidate = os.path.abspath(os.path.join(export_root_abs, dest_path))
    if candidate != export_root_abs and not candidate.startswith(
        export_root_abs + os.sep
    ):
        raise ValueError("targetPath resolves outside the export root")
    return candidate


def ensure_dir(directory: str) -> None:
    os.makedirs(directory, exist_ok=True)


def write_strm_file(file_path_no_ext: str, url: str) -> None:
    """
    Writes atomically (tmp file + os.replace) so a crash mid-write can
    never leave a truncated/corrupt .strm file sitting around -- the item
    stays marked "pending" until this returns, so a crash here just means
    the item gets rewritten cleanly on the next run.
    """
    strm_path = file_path_no_ext + ".strm"
    tmp_path = strm_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(url)
    os.replace(tmp_path, strm_path)


def link_sidecar_path(real_path: str) -> str:
    return real_path + LINK_SIDECAR_EXT


def write_link_sidecar(real_path: str, payload: dict) -> None:
    """
    Records the original streaming link (plus enough to identify it --
    fid/share_key/share_link/quality/when) beside a just-downloaded file,
    written *before* its .strm is removed -- see download_items(). This
    is what keeps the process reversible even if the task record that
    drove the download is deleted later: the link lives on disk, next to
    the file it belongs to, the same way .strm state already lives on
    disk rather than only in state.json. Atomic (tmp + os.replace), same
    discipline write_strm_file() uses.

    real_path is the file's real, final path -- e.g. ".../Movie.mkv",
    already carrying its real video extension, exactly what
    item["dest_path"] already is elsewhere in this module (see
    discover_tree()/seed_flat_items()) and what write_strm_file() itself
    takes (it only ever appends ".strm" on top, never re-adds a video
    extension) -- so callers never need to split/rebuild a path here.
    """
    path = link_sidecar_path(real_path)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp_path, path)


def read_link_sidecar(real_path: str):
    """Returns the sidecar dict, or None if there isn't one / it's unreadable."""
    path = link_sidecar_path(real_path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def remove_link_sidecar(real_path: str) -> None:
    try:
        os.remove(link_sidecar_path(real_path))
    except OSError:
        pass


def extract_key(share_link: str) -> str:
    """Accept either a bare share key or a full share URL."""
    share_link = share_link.strip()
    match = re.search(r"febbox\.com/share/([A-Za-z0-9]+)", share_link)
    if match:
        return match.group(1)
    # Not a URL -- assume it's already the key.
    return share_link


def extract_json_from_browser_response(body: str):
    """
    When FlareSolverr's browser hits a raw JSON endpoint, most browsers
    wrap the JSON text in a minimal HTML shell like
    <html><body><pre>{...}</pre></body></html>. Pull the JSON out of that
    if present, otherwise assume the body is already plain JSON.
    """
    match = re.search(r"<pre[^>]*>(.*?)</pre>", body, re.S)
    text = match.group(1) if match else body
    text = html.unescape(text).strip()
    return json.loads(text)


def parse_html_quality_fragment(fragment: str) -> list:
    """
    Parse the HTML fragment Febbox's /console/video_quality_list returns:
    a run of `<div class="file_quality" data-url="..." data-quality="...">`
    blocks, one per available quality/format. Returns a list of
    {"url": ..., "quality": ...} dicts in the order Febbox listed them
    (its own ordering puts the original file first, which is what we want
    as the default pick).
    """
    items = []
    for chunk in fragment.split('<div class="file_quality"')[1:]:
        tag_end = chunk.find(">")
        if tag_end == -1:
            continue
        opening_tag_attrs = chunk[:tag_end]

        url_match = re.search(r'data-url="([^"]*)"', opening_tag_attrs)
        if not url_match:
            continue
        url = html.unescape(url_match.group(1))
        if not url:
            continue

        quality_match = re.search(r'data-quality="([^"]*)"', opening_tag_attrs)
        quality = html.unescape(quality_match.group(1)) if quality_match else ""

        items.append({"url": url, "quality": quality})
    return items


def parse_html_folder_fragment(fragment: str) -> list:
    """
    Parse the HTML fragment Febbox returns for file_share_list calls made
    with is_html=1, instead of the usual JSON file_list array. Each entry
    is a `<div class="file ...">` block carrying a `data-id` attribute
    (folders additionally carry an "open_dir" class) with the name in a
    nested `<p class="file_name">`. Attribute order/extras aren't fixed,
    so this splits on each item's marker and searches within that chunk
    rather than assuming a strict attribute sequence. Returns a list of
    dicts shaped like normal file_list entries (fid/file_name/is_dir) so
    the rest of the pipeline doesn't need to know which format was used.
    """
    items = []
    # Every item's div starts with this literal, but "file" is also a
    # prefix of nested class names like "file_icon"/"file_info" inside
    # each item -- the lookahead requires "file" to be followed by a
    # space or closing quote (i.e. a real class token boundary) so we
    # don't split in the middle of an item's own markup.
    for chunk in re.split(r'<div class="file(?=[ "])', fragment)[1:]:
        tag_end = chunk.find(">")
        if tag_end == -1:
            continue
        opening_tag_attrs = chunk[:tag_end]

        id_match = re.search(r'data-id="(\d+)"', opening_tag_attrs)
        if not id_match:
            continue
        fid = int(id_match.group(1))

        is_dir = 1 if "open_dir" in opening_tag_attrs else 0

        name_match = re.search(r'<p class="file_name">(.*?)</p>', chunk, re.DOTALL)
        if not name_match:
            continue
        name = html.unescape(name_match.group(1)).strip()
        if not name:
            continue

        items.append({"fid": fid, "file_name": name, "is_dir": is_dir})
    return items


# --------------------------------------------------------------------------
# FlareSolverr client -- routes requests through a real browser end-to-end
# --------------------------------------------------------------------------
class FlareSolverr:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.available = False
        self.session_id = None

    def _post(self, payload: dict, timeout: int = 90) -> dict:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.base_url,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def detect(self) -> bool:
        """Quick health check -- is a FlareSolverr instance reachable?"""
        try:
            resp = self._post({"cmd": "sessions.list"}, timeout=5)
            self.available = resp.get("status") == "ok"
        except Exception:
            self.available = False
        return self.available

    def create_session(self) -> None:
        """Create a persistent browser session so cookies carry over calls."""
        try:
            payload = {"cmd": "sessions.create"}
            resp = self._post(payload, timeout=30)
            if resp.get("status") == "ok":
                self.session_id = resp.get("session")
        except Exception:
            pass

    def destroy_session(self) -> None:
        if not self.session_id:
            return
        try:
            self._post({"cmd": "sessions.destroy", "session": self.session_id}, timeout=15)
        except Exception:
            pass

    def get(self, url: str, cookies: list = None) -> dict:
        """
        Load `url` in the browser and return the FlareSolverr 'solution'
        dict (contains response HTML/text, status, cookies, userAgent).
        Raises RuntimeError on failure.
        """
        payload = {"cmd": "request.get", "url": url, "maxTimeout": 60000}
        if self.session_id:
            payload["session"] = self.session_id
        if cookies:
            payload["cookies"] = cookies

        resp = self._post(payload)
        if resp.get("status") != "ok":
            raise RuntimeError(resp.get("message", "unknown FlareSolverr error"))
        return resp["solution"]


# --------------------------------------------------------------------------
# FebboxAPI client (mirrors the two methods the Node pipeline relies on)
# --------------------------------------------------------------------------
class FebboxAPI:
    BASE_URL = "https://www.febbox.com"
    MAX_RETRIES = 5

    def __init__(
        self,
        ctx: RunContext,
        flaresolverr: FlareSolverr = None,
        login_cookie: str = "",
        request_timeout: int = 20,
        max_retries: int = None,
        user_agent: str = None,
    ):
        self.ctx = ctx
        self.flaresolverr = flaresolverr
        self.login_cookie = (login_cookie or "").strip()
        self.user_agent = user_agent or DEFAULT_USER_AGENT
        self.request_timeout = request_timeout
        self.max_retries = max_retries if max_retries is not None else self.MAX_RETRIES

        self._login_cookies_payload = None
        if self.login_cookie:
            self._login_cookies_payload = [
                {"name": "ui", "value": self.login_cookie, "domain": ".febbox.com"}
            ]

        if self.flaresolverr and self.flaresolverr.available:
            self.flaresolverr.create_session()

    def _get_json_via_flaresolverr(self, url: str) -> dict:
        solution = self.flaresolverr.get(url, cookies=self._login_cookies_payload)
        status = solution.get("status")
        body = solution.get("response", "")
        if status and status != 200:
            snippet = body[:300].replace("\n", " ")
            raise RuntimeError(f"browser got HTTP {status}: {snippet!r}")
        return extract_json_from_browser_response(body)

    def _get_json_direct(self, url: str, share_key: str) -> dict:
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            self.ctx.check_cancelled()
            headers = {
                "User-Agent": self.user_agent,
                "Accept": "application/json, text/plain, */*",
                "Referer": f"{self.BASE_URL}/share/{share_key}" if share_key else self.BASE_URL,
            }
            if self.login_cookie:
                headers["Cookie"] = f"ui={self.login_cookie}"

            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                return json.loads(body)
            except urllib.error.HTTPError as e:
                last_error = e
                error_body = ""
                try:
                    error_body = e.read().decode("utf-8", errors="replace")[:300]
                except Exception:
                    pass
                self.ctx.log(f"      HTTP {e.code} response body: {error_body!r}")
                if e.code == 429 or e.code >= 500:
                    retry_after = e.headers.get("Retry-After") if e.headers else None
                    if retry_after and retry_after.isdigit():
                        wait = int(retry_after)
                    else:
                        wait = min(2 ** attempt, 30) + random.uniform(0, 1)
                    self.ctx.log(
                        f"      retrying in {wait:.1f}s "
                        f"(attempt {attempt}/{self.max_retries})..."
                    )
                    time.sleep(wait)
                    continue
                raise
        raise last_error

    def _get_json(self, path: str, params: dict, share_key: str = "") -> dict:
        self.ctx.check_cancelled()
        query = urllib.parse.urlencode(params)
        url = f"{self.BASE_URL}{path}?{query}"

        if self.flaresolverr and self.flaresolverr.available:
            try:
                return self._get_json_via_flaresolverr(url)
            except Exception as e:
                self.ctx.log(f"      FlareSolverr request failed ({e}); falling back to a direct request.")

        return self._get_json_direct(url, share_key)

    def get_file_list(self, share_key: str, parent_id: int = 0) -> list:
        """Returns the list of items at parent_id for this share."""
        items = []
        page = 1
        while True:
            params = {
                "page": page,
                "share_key": share_key,
                "pwd": "",
                "parent_id": parent_id,
            }
            if parent_id != 0:
                # Root listing works fine without this; subfolder listing
                # only returned real content once this flag was added
                # (confirmed via a captured browser request).
                params["is_html"] = 1
            data = self._get_json(
                "/file/file_share_list",
                params,
                share_key=share_key,
            )
            html_field = data.get("html")
            if isinstance(html_field, str):
                file_list = parse_html_folder_fragment(html_field)
            else:
                data_field = data.get("data")
                file_list = (data_field or {}).get("file_list") if isinstance(data_field, dict) else None
                file_list = file_list or []
            if not file_list:
                if page == 1 and parent_id != 0:
                    self.ctx.log(f"      DEBUG raw response for params={params}: {json.dumps(data)[:500]}")
                break
            items.extend(file_list)
            if len(file_list) < 30:  # last page
                break
            page += 1
        return items

    def get_links(self, share_key: str, fid) -> list:
        """Returns a list of {"url": ..., "quality": ...} streaming links
        for fid, ordered as Febbox lists them (original file first)."""
        data = self._get_json(
            "/console/video_quality_list",
            {"fid": fid},
            share_key=share_key,
        )
        html_field = data.get("html")
        if isinstance(html_field, str):
            links = parse_html_quality_fragment(html_field)
        else:
            links = []

        if not links:
            self.ctx.log(f"      DEBUG raw video_quality_list response for fid={fid}: {json.dumps(data)[:500]}")
        return links


# --------------------------------------------------------------------------
# Phase 1: discovery -- walk the share tree, build/merge the flat item list
# --------------------------------------------------------------------------
def _dedupe_episode_files(candidates: list, ctx: RunContext) -> list:
    """
    Febbox shares sometimes carry more than one separate file for the
    same episode -- distinct uploads at different resolutions, not just
    alternate quality options on a single file (that's what
    FebboxAPI.get_links() already handles). Groups candidates by parsed
    episode identity and keeps only the best-quality one per episode:
    1080p first, then 720p, then 4k/2160p, then whatever has no quality
    tag at all. Items with no parseable episode identity (movies,
    specials, anything not episode-numbered) pass through untouched.
    """
    groups = {}
    passthrough = []
    for candidate in candidates:
        filename = os.path.basename(candidate["rel_path"])
        key = classify.parse_episode_identity(filename)
        if key is None:
            passthrough.append(candidate)
            continue
        groups.setdefault(key, []).append(candidate)

    kept = passthrough
    for group in groups.values():
        if len(group) == 1:
            kept.append(group[0])
            continue
        group.sort(key=lambda c: classify.quality_rank(os.path.basename(c["rel_path"])))
        best = group[0]
        kept.append(best)
        for dropped in group[1:]:
            ctx.log(
                f"      multiple files found for the same episode -- "
                f"keeping {os.path.basename(best['rel_path'])}, "
                f"skipping {os.path.basename(dropped['rel_path'])}"
            )
    return kept


def discover_tree(
    api: FebboxAPI,
    ctx: RunContext,
    share_key: str,
    parent_id,
    absolute_dir: str,
    rel_path: str,
    depth: int,
    max_depth: int = MAX_DEPTH,
) -> None:
    """
    Recursively lists parent_id, descending into subfolders (mirroring
    their names into absolute_dir) and calling ctx.upsert_items() with any
    video files found. Doesn't touch streaming links or write any files --
    that's the export phase's job, and it decides per-item whether work is
    still needed by checking each item's status (already "done" or not).
    """
    ctx.check_cancelled()

    if depth > max_depth:
        ctx.log(f"  {rel_path}: max folder depth reached, not descending further")
        return

    label = rel_path or "(root)"
    ctx.log(f"Listing '{label}'...")
    ctx.set_current(label)
    items = api.get_file_list(share_key, parent_id)
    ctx.log(f"  {len(items)} item(s) in '{label}'")

    new_items = []
    for item in items:
        raw_name = item.get("file_name") or item.get("name")
        if not raw_name:
            continue
        name = sanitize_name(raw_name)
        fid = item.get("fid") or item.get("id")
        if fid is None:
            continue
        item_rel = f"{rel_path}/{name}" if rel_path else name

        if item.get("is_dir") == 1:
            sub_dir = os.path.join(absolute_dir, name)
            discover_tree(api, ctx, share_key, fid, sub_dir, item_rel, depth + 1, max_depth)
            continue

        ext = os.path.splitext(name)[1].lower()
        if ext not in VIDEO_EXTENSIONS:
            continue

        dest_no_ext = os.path.join(absolute_dir, name)
        new_items.append({"fid": fid, "rel_path": item_rel, "dest_path": dest_no_ext})

    ctx.upsert_items(_dedupe_episode_files(new_items, ctx))


def seed_flat_items(ctx: RunContext, absolute_target: str, root_items: list) -> None:
    """
    Alternative to discover_tree() for a group of specific, already-known
    video fids sitting loose at the share root (see grouping.py) -- no
    directory walk needed, we already know exactly which files belong.
    """
    ctx.log(f"Using {len(root_items)} pre-selected file(s) (no folder to walk).")
    new_items = []
    for entry in root_items or []:
        raw_name = entry.get("file_name")
        fid = entry.get("fid")
        if not raw_name or fid is None:
            continue
        name = sanitize_name(raw_name)
        ext = os.path.splitext(name)[1].lower()
        if ext not in VIDEO_EXTENSIONS:
            continue
        dest_no_ext = os.path.join(absolute_target, name)
        new_items.append({"fid": fid, "rel_path": name, "dest_path": dest_no_ext})
    ctx.upsert_items(_dedupe_episode_files(new_items, ctx))


# --------------------------------------------------------------------------
# Phase 2: export -- fetch a streaming link + record in JSON for pending items
# --------------------------------------------------------------------------
def export_items(api: FebboxAPI, ctx: RunContext, share_key: str) -> None:
    items = ctx.get_items()
    for item in items:
        ctx.check_cancelled()
        if item.get("status") == "done":
            continue  # already exported in a previous run -- resume skips it

        rel_path = item["rel_path"]
        fid = item["fid"]
        ctx.set_current(rel_path)
        ctx.log(f"  - {rel_path}: requesting stream link...")
        try:
            links = api.get_links(share_key, fid)
        except Cancelled:
            raise
        except Exception as e:
            ctx.log(f"      failed to get link: {e}")
            ctx.mark_item(fid, "error", error=str(e))
            continue

        if not links:
            ctx.log("      no stream available, skipping")
            ctx.mark_item(fid, "error", error="no stream available")
            continue

        url = links[0]["url"]
        quality = links[0].get("quality", "")
        ctx.log(f"      recorded stream link for {rel_path}")
        ctx.mark_item(fid, "done", quality=quality, url=url)


# --------------------------------------------------------------------------
# Phase 2 (download variant): fetch a fresh streaming link + the actual
# bytes for already-exported items, replacing each .strm with the real
# file. Mirrors export_items() closely -- same per-item skip-if-done
# resumability -- but a "done" item here means the real file landed on
# disk, not just that a link was written.
# --------------------------------------------------------------------------
def download_one_file(
    url: str,
    real_path: str,
    ctx: RunContext,
    on_progress=None,
    request_timeout: int = 20,
    user_agent: str = None,
) -> None:
    """
    Streams url's body to real_path (the file's real, final path -- see
    write_link_sidecar()'s docstring for why it's a single already-
    complete path, not a stem + extension pair), writing through a
    `.part` sibling first and os.replace()-ing it into place on success --
    the same atomic-write discipline write_strm_file() uses, just for
    potentially gigabyte-sized content instead of a one-line URL.

    Resumable: if a `.part` file already exists (a previous attempt was
    cancelled, errored, or the server restarted mid-download), reopens
    the request with a Range header and appends rather than starting
    over -- falls back to a clean restart if the server doesn't honor it.

    on_progress(bytes_done, bytes_total), if given, is called after every
    chunk (bytes_total is None if the server didn't send a Content-Length).
    ctx.check_cancelled() is checked between chunks so a multi-GB
    transfer can actually be stopped, not just at the start -- the
    partial `.part` file is left in place either way, ready to resume.
    """
    final_path = real_path
    part_path = final_path + ".part"

    resume_from = os.path.getsize(part_path) if os.path.exists(part_path) else 0
    headers = {"User-Agent": user_agent or DEFAULT_USER_AGENT}
    if resume_from:
        headers["Range"] = f"bytes={resume_from}-"

    req = urllib.request.Request(url, headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=request_timeout)
    except urllib.error.HTTPError as e:
        if resume_from and e.code == 416:
            # "Range not satisfiable" -- the server thinks there's
            # nothing left past what we already have. Treat the .part as
            # complete rather than fail a download that's actually done.
            os.replace(part_path, final_path)
            if on_progress:
                on_progress(resume_from, resume_from)
            return
        raise

    try:
        resuming = bool(resume_from) and resp.status == 206
        if resume_from and not resuming:
            # Server ignored the Range request -- restart clean rather
            # than risk appending a fresh response onto a stale partial.
            resume_from = 0

        bytes_total = None
        content_length = resp.headers.get("Content-Length")
        if content_length is not None:
            try:
                bytes_total = int(content_length) + resume_from
            except ValueError:
                bytes_total = None

        bytes_done = resume_from
        with open(part_path, "ab" if resuming else "wb") as f:
            while True:
                ctx.check_cancelled()
                chunk = resp.read(DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)
                bytes_done += len(chunk)
                if on_progress:
                    on_progress(bytes_done, bytes_total)
    finally:
        resp.close()

    os.replace(part_path, final_path)


def download_items(
    api: FebboxAPI,
    ctx: RunContext,
    share_key: str,
    share_link: str,
    request_timeout: int = 20,
    user_agent: str = None,
) -> None:
    # Polls ctx.get_items() instead of taking one snapshot up front, so a
    # file appended to this task (tasks.TaskManager.add_download_items(),
    # from app.py's download routes re-queuing onto an already-running
    # download) gets picked up on the next lap rather than sitting pending
    # until this run ends and the task gets requeued. `handled` tracks fids
    # this call has already attempted -- including ones that errored --
    # so a failed item isn't retried in a tight loop each lap; it just
    # waits for the next resync/resume like before.
    handled = set()
    while True:
        items = [it for it in ctx.get_items() if it["fid"] not in handled and it.get("status") != "done"]
        if not items:
            break
        for item in items:
            ctx.check_cancelled()

            rel_path = item["rel_path"]
            fid = item["fid"]
            real_path = item["dest_path"]  # already the real, final path -- see write_link_sidecar()'s docstring
            handled.add(fid)
            ctx.set_current(rel_path)
            ctx.log(f"  - {rel_path}: requesting a fresh stream link...")
            try:
                links = api.get_links(share_key, fid)
            except Cancelled:
                raise
            except Exception as e:
                ctx.log(f"      failed to get link: {e}")
                ctx.mark_item(fid, "error", error=str(e))
                continue

            if not links:
                ctx.log("      no stream available, skipping")
                ctx.mark_item(fid, "error", error="no stream available")
                continue

            url = links[0]["url"]
            quality = links[0].get("quality", "")

            def _on_progress(done, total, _fid=fid):
                ctx.update_item_progress(_fid, done, total)

            ctx.log(f"      downloading to {real_path} ...")
            try:
                ensure_dir(os.path.dirname(real_path))
                download_one_file(
                    url, real_path, ctx,
                    on_progress=_on_progress,
                    request_timeout=request_timeout,
                    user_agent=user_agent,
                )
            except Cancelled:
                raise
            except Exception as e:
                ctx.log(f"      download failed: {e}")
                ctx.mark_item(fid, "error", error=str(e))
                continue

            write_link_sidecar(real_path, {
                "fid": fid,
                "share_key": share_key,
                "share_link": share_link,
                "url": url,
                "quality": quality,
                "downloaded_at": _now_iso(),
            })
            try:
                os.remove(real_path + ".strm")
            except OSError:
                pass
            ctx.log(f"      wrote {real_path}")
            ctx.mark_item(fid, "done", quality=quality)


# --------------------------------------------------------------------------
# Pipeline entrypoint
# --------------------------------------------------------------------------
def run_export(
    ctx: RunContext,
    share_link: str,
    target_path: str,
    export_root: str,
    flaresolverr: FlareSolverr,
    login_cookie: str,
    root_kind: str = "share_root",
    root_fid=None,
    root_items: list = None,
    request_timeout: int = 20,
    max_retries: int = 5,
    max_folder_depth: int = MAX_DEPTH,
    user_agent: str = None,
) -> None:
    """
    Runs (or resumes) an export. Idempotent/resumable by design: items
    already marked "done" on ctx are skipped, so calling this again after
    a cancellation, crash, or error just picks up where it left off. To
    force already-exported items to be refreshed (e.g. stale streaming
    URLs), the caller resets their status to "pending" before invoking
    this -- see tasks.TaskManager.resync_task.
    """
    api = FebboxAPI(
        ctx,
        flaresolverr=flaresolverr,
        login_cookie=login_cookie,
        request_timeout=request_timeout,
        max_retries=max_retries,
        user_agent=user_agent,
    )
    share_key = extract_key(share_link)

    absolute_target = validate_export_path(export_root, target_path)

    ctx.set_phase("discovering")
    if root_kind == "fids":
        seed_flat_items(ctx, absolute_target, root_items)
    else:
        start_fid = root_fid if (root_kind == "folder" and root_fid is not None) else 0
        discover_tree(api, ctx, share_key, start_fid, absolute_target, "", 0, max_folder_depth)

    ctx.set_phase("exporting")
    export_items(api, ctx, share_key)


def run_download(
    ctx: RunContext,
    share_link: str,
    target_path: str,
    export_root: str,
    flaresolverr: FlareSolverr,
    login_cookie: str,
    request_timeout: int = 20,
    max_retries: int = 5,
    user_agent: str = None,
) -> None:
    """
    Downloads real bytes for a download task's item list -- fid/rel_path/
    dest_path, already seeded onto the task at creation time -- and saves
    each file to disk.
    """
    api = FebboxAPI(
        ctx,
        flaresolverr=flaresolverr,
        login_cookie=login_cookie,
        request_timeout=request_timeout,
        max_retries=max_retries,
        user_agent=user_agent,
    )
    share_key = extract_key(share_link)

    absolute_target = validate_export_path(export_root, target_path)
    ensure_dir(absolute_target)

    ctx.set_phase("downloading")
    download_items(api, ctx, share_key, share_link, request_timeout=request_timeout, user_agent=user_agent)


def revert_link_to_stream(real_path: str) -> None:
    """
    Undoes download_items() for one file: removes the real file and
    removes the sidecar. The item remains streamable via its link in JSON.
    Raises FileNotFoundError if the real file doesn't exist.
    """
    if not os.path.isfile(real_path):
        raise FileNotFoundError(real_path)
    try:
        os.remove(real_path)
    except OSError:
        pass
    remove_link_sidecar(real_path)

    # Clean up empty parent directory if empty
    parent_dir = os.path.dirname(real_path)
    try:
        if os.path.isdir(parent_dir) and not os.listdir(parent_dir):
            os.rmdir(parent_dir)
    except OSError:
        pass
