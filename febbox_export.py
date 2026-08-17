#!/usr/bin/env python3
"""
febbox_export.py
=================
Single-file, fully interactive CLI port of the Febbox export pipeline.

Just run it:

    python3 febbox_export.py

It will prompt you for a Febbox share link (or bare share key), a target
sub-folder name, and (optionally) a Febbox login cookie, then:
  1. Fetch the root file list of the share.
  2. Filter for video files (.mp4 / .mkv).
  3. Request a streaming link for each video.
  4. Write a `<video-name>.strm` file containing the first streaming URL
     into EXPORT_ROOT/<targetPath>.

No third-party dependencies -- only the Python standard library.

FlareSolverr support
---------------------
Febbox is behind Cloudflare, and Cloudflare's bot management can flag a
request even when it carries a valid cf_clearance cookie, if the
underlying TLS/HTTP fingerprint doesn't look like a real browser (which a
plain Python request never will). Borrowing a solved cookie for our own
requests therefore isn't reliable here.

Instead, when a FlareSolverr instance (https://github.com/FlareSolverr/
FlareSolverr) is reachable at FLARESOLVERR_URL, this script routes every
Febbox API request THROUGH FlareSolverr's real browser end-to-end (via a
persistent FlareSolverr session so cookies carry over between calls). This
is slower than a raw HTTP request -- each call is a real page load -- but
it's what actually gets past Cloudflare's bot checks here.

If no FlareSolverr instance is found, the script falls back to plain
direct requests with retry/backoff, which will work for sites that aren't
actively fingerprinting, but likely won't get past this particular block.

To run FlareSolverr locally:

    docker run -d -p 8191:8191 --name flaresolverr ghcr.io/flaresolverr/flaresolverr:latest

You'll also be prompted for an optional Febbox login cookie (the value of
your browser's "ui" cookie after logging into febbox.com). Some of these
endpoints are reported to require an authenticated session in addition to
clearing Cloudflare, so this is worth supplying if you keep getting
blocked even through FlareSolverr.

NOTE on the Febbox endpoints: Febbox does not publish an official API, so
`FebboxAPI` below talks to the same undocumented endpoints the Febbox web
UI itself uses (file_share_list / player_list). Febbox can change these at
any time -- if get_links() starts returning nothing, that's the most likely
cause; only the URLs inside FebboxAPI should need adjusting.
"""

import json
import os
import re
import time
import random
import html
import platform
import urllib.request
import urllib.parse
import urllib.error

# --------------------------------------------------------------------------
# Configuration (hard-coded, no .env, matching the original pipeline)
# --------------------------------------------------------------------------
EXPORT_ROOT = os.path.expanduser("~/Media").replace("\\", "/")

VIDEO_EXTENSIONS = {".mp4", ".mkv"}
DEFAULT_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) FebboxExportPipeline/1.0"
FLARESOLVERR_URL = "http://localhost:8191/v1"


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
    strm_path = file_path_no_ext + ".strm"
    with open(strm_path, "w", encoding="utf-8") as f:
        f.write(url)


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

    def create_session(self, cookies: list = None) -> None:
        """Create a persistent browser session so cookies carry over calls."""
        try:
            payload = {"cmd": "sessions.create"}
            resp = self._post(payload, timeout=30)
            if resp.get("status") == "ok":
                self.session_id = resp.get("session")
        except Exception as e:
            print(f"      could not create FlareSolverr session: {e}")

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

    def __init__(self, flaresolverr: FlareSolverr = None, login_cookie: str = ""):
        self.flaresolverr = flaresolverr
        self.login_cookie = login_cookie.strip()
        self.user_agent = DEFAULT_USER_AGENT

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
        for attempt in range(1, self.MAX_RETRIES + 1):
            headers = {
                "User-Agent": self.user_agent,
                "Accept": "application/json, text/plain, */*",
                "Referer": f"{self.BASE_URL}/share/{share_key}" if share_key else self.BASE_URL,
            }
            if self.login_cookie:
                headers["Cookie"] = f"ui={self.login_cookie}"

            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=20) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                return json.loads(body)
            except urllib.error.HTTPError as e:
                last_error = e
                error_body = ""
                try:
                    error_body = e.read().decode("utf-8", errors="replace")[:300]
                except Exception:
                    pass
                print(f"      HTTP {e.code} response body: {error_body!r}")
                if e.code == 429 or e.code >= 500:
                    retry_after = e.headers.get("Retry-After") if e.headers else None
                    if retry_after and retry_after.isdigit():
                        wait = int(retry_after)
                    else:
                        wait = min(2 ** attempt, 30) + random.uniform(0, 1)
                    print(
                        f"      retrying in {wait:.1f}s "
                        f"(attempt {attempt}/{self.MAX_RETRIES})..."
                    )
                    time.sleep(wait)
                    continue
                raise
        raise last_error

    def _get_json(self, path: str, params: dict, share_key: str = "") -> dict:
        query = urllib.parse.urlencode(params)
        url = f"{self.BASE_URL}{path}?{query}"

        if self.flaresolverr and self.flaresolverr.available:
            try:
                return self._get_json_via_flaresolverr(url)
            except Exception as e:
                print(f"      FlareSolverr request failed ({e}); falling back to a direct request.")

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
                    print(f"      DEBUG raw response for params={params}: {json.dumps(data)[:500]}")
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
            print(f"      DEBUG raw video_quality_list response for fid={fid}: {json.dumps(data)[:500]}")
        return links


# --------------------------------------------------------------------------
# Pipeline (walks the share tree, mirroring folder structure into exports)
# --------------------------------------------------------------------------
MAX_DEPTH = 8  # safety cap against unexpectedly deep/cyclical share trees


def walk_and_export(
    api: FebboxAPI,
    share_key: str,
    parent_id,
    rel_path: str,
    depth: int,
    results: list,
) -> None:
    """
    Recursively lists parent_id, collecting streaming URLs for videos into
    results list and descending into subfolders.
    """
    if depth > MAX_DEPTH:
        print(f"  {rel_path}: max folder depth reached, not descending further")
        return

    label = rel_path or "(root)"
    print(f"Listing '{label}'...")
    items = api.get_file_list(share_key, parent_id)
    print(f"  {len(items)} item(s) in '{label}'")

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
            walk_and_export(
                api, share_key, fid, item_rel, depth + 1, results
            )
            continue

        ext = os.path.splitext(name)[1].lower()
        if ext not in VIDEO_EXTENSIONS:
            continue

        print(f"  - {item_rel}: requesting stream link...")
        links = api.get_links(share_key, fid)
        if not links:
            print("      no stream available, skipping")
            continue

        url = links[0]["url"]
        quality = links[0].get("quality", "")
        results.append({
            "fid": fid,
            "name": name,
            "rel_path": item_rel,
            "url": url,
            "quality": quality,
        })
        print(f"      recorded link for {item_rel}")


def run_export(
    share_link: str, target_path: str, flaresolverr: FlareSolverr, login_cookie: str
) -> int:
    """Runs the export. Writes extracted streaming links into a JSON file."""
    api = FebboxAPI(flaresolverr=flaresolverr, login_cookie=login_cookie)
    share_key = extract_key(share_link)

    results = []
    walk_and_export(api, share_key, 0, "", 0, results)

    json_path = os.path.join(EXPORT_ROOT, f"{target_path.replace('/', '_')}.json")
    ensure_dir(os.path.dirname(json_path))
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"share_link": share_link, "target_path": target_path, "items": results}, f, indent=2)
    print(f"\nWrote {len(results)} link(s) to {json_path}")
    return len(results)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def prompt(text: str) -> str:
    try:
        return input(text).strip()
    except EOFError:
        return ""


def main() -> None:
    print("Febbox export pipeline")
    print("=======================")
    print(f"Export root: {EXPORT_ROOT}")

    flaresolverr = FlareSolverr(FLARESOLVERR_URL)
    print(f"Checking for FlareSolverr at {FLARESOLVERR_URL}...")
    if flaresolverr.detect():
        print(
            "FlareSolverr detected -- requests will be routed through its "
            "browser to get past Cloudflare.\n"
        )
    else:
        print(
            "FlareSolverr not found -- continuing without it. If you hit "
            "Cloudflare blocks, run:\n"
            "  docker run -d -p 8191:8191 --name flaresolverr "
            "ghcr.io/flaresolverr/flaresolverr:latest\n"
            "and re-run this script.\n"
        )

    share_link = ""
    while not share_link:
        share_link = prompt("Febbox share link (or share key): ")
        if not share_link:
            print("A share link or key is required.")

    target_path = ""
    while not target_path:
        target_path = prompt("Target sub-folder name: ")
        if not target_path:
            print("A target sub-folder name is required.")

    login_cookie = prompt(
        "Febbox 'ui' login cookie (optional, press Enter to skip): "
    )

    print()
    try:
        count = run_export(share_link, target_path, flaresolverr, login_cookie)
    except ValueError as e:
        print(f"Error: {e}")
        return
    except urllib.error.URLError as e:
        print(f"Error: Febbox request failed: {e}")
        return
    except Exception as e:  # noqa: BLE001 - surface any other failure to the user
        print(f"Error: {e}")
        return
    finally:
        flaresolverr.destroy_session()

    print(f"\nExport completed: {count} stream link(s) written to JSON.")


if __name__ == "__main__":
    main()