"""
media_library.py
=================
The Movies/Series pages' data source: what's streamable (saved in state.json)
and what's downloaded (sitting in export_root as real video files), broken down
into real detail -- seasons and episodes for TV/anime, individual files for
movies/anime movies -- plus delete operations (a whole title, or one file
within it).

Disk storage (export_root) is reserved exclusively for downloaded video files;
streamable media is tracked directly in state.json task records.
"""

import os
import re
import shutil

from . import classify, core
from .core import validate_export_path
from .tasks import CATEGORY_LABELS, compute_target_path, year_belongs_in_folder

_TITLE_YEAR_RE = re.compile(r"^(.*?)(?:\s\((\d{4})\))?$")
_SERIES_CATEGORIES = {"tv", "anime"}
_MOVIE_CATEGORIES = {"movie", "anime_movie"}


def _parse_title_year(folder_name: str):
    m = _TITLE_YEAR_RE.match(folder_name.strip())
    if not m:
        return folder_name, None
    title = m.group(1).strip() or folder_name
    year = int(m.group(2)) if m.group(2) else None
    return title, year


def _downloaded_files_recursive(abs_dir: str):
    """Every real video file (core.VIDEO_EXTENSIONS) under abs_dir, at any depth,
    as (rel_path, abs_path) pairs. The sidecar written alongside a downloaded
    file (core.LINK_SIDECAR_EXT) isn't a video extension, so it's naturally skipped
    here without any extra filtering."""
    out = []
    if not os.path.isdir(abs_dir):
        return out
    for root, _dirs, files in os.walk(abs_dir):
        for name in files:
            ext = os.path.splitext(name)[1].lower()
            if ext in core.VIDEO_EXTENSIONS:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, abs_dir).replace(os.sep, "/")
                out.append((rel, full))
    return out


def _season_label(season_num) -> str:
    if season_num == 0:
        return "Specials"
    return f"Season {season_num}"


def parse_rel_path_season_episode(rel_path: str):
    """
    Best-effort (season, episode|None) for one video file's rel_path
    (either a disk scan's, or an export task item's -- same "<folder>/
    <filename>" convention either way). Filename-first, folder-name
    fallback: a season folder's own name is consulted when the
    filename itself carries no season marker or an absolute episode marker.
    Season is always an int (0 -- the Specials/no-marker-at-all bucket --
    when nothing indicates otherwise); episode is None when unparseable.
    """
    norm = (rel_path or "").replace("\\", "/")
    parts = norm.split("/")
    filename = parts[-1]
    se = classify.parse_season_episode(filename)

    folder_season = None
    if len(parts) > 1:
        folder_m = re.search(r"(?:season|series|s)\s*(\d+)", parts[0], re.IGNORECASE)
        if folder_m:
            folder_season = int(folder_m.group(1))

    if se:
        season_num, episode_num = se
        if season_num == 0 and folder_season is not None:
            season_num = folder_season
        return season_num, episode_num
    elif folder_season is not None:
        return folder_season, None
    else:
        return 0, None


def list_media(export_root: str, media_type: str = None) -> list:
    """
    One row per Category/Title (Year) folder with downloaded files under
    export_root. Counts downloaded files only -- augment_with_pipeline()
    folds in streamable titles recorded in JSON and active tasks.
    """
    entries = []
    if not export_root or not os.path.isdir(export_root):
        return entries

    for category, label in CATEGORY_LABELS.items():
        is_series = category in _SERIES_CATEGORIES
        if media_type == "series" and not is_series:
            continue
        if media_type == "movie" and is_series:
            continue
        cat_dir = os.path.join(export_root, label)
        if not os.path.isdir(cat_dir):
            continue
        for name in sorted(os.listdir(cat_dir)):
            abs_dir = os.path.join(cat_dir, name)
            if not os.path.isdir(abs_dir):
                continue
            downloaded_files = _downloaded_files_recursive(abs_dir)
            downloaded_count = len(downloaded_files)
            if downloaded_count == 0:
                continue
            title, year = _parse_title_year(name)
            entries.append({
                "id": f"{label}/{name}",
                "category": category,
                "is_series": is_series,
                "title": title,
                "year": year,
                "file_count": downloaded_count,
                "streamable_count": 0,
                "downloaded_count": downloaded_count,
                "status": "downloaded",
                "has_folder": True,
            })
    return entries


def target_path_exists(export_root: str, target_path: str) -> bool:
    """Cheap existence check for one Category/Title (Year) folder, without
    walking/counting its contents -- used by the analyzer to dedup a
    discovery against a title that has downloaded files on disk."""
    if not export_root or not os.path.isdir(export_root) or not target_path:
        return False
    abs_dir = os.path.join(export_root, *target_path.split("/"))
    return os.path.isdir(abs_dir)


def category_title_year_from_target_path(target_path):
    """Reverses compute_target_path()'s "<Category label>/<Title> (<Year>)"
    convention. Returns (None, None, None) if target_path doesn't match."""
    if not target_path or "/" not in target_path:
        return None, None, None
    label, folder_name = target_path.split("/", 1)
    category = next((c for c, l in CATEGORY_LABELS.items() if l == label), None)
    if category is None:
        return None, None, None
    title, year = _parse_title_year(folder_name)
    return category, title, year


def augment_with_pipeline(entries: list, discovered_items: list, tasks: list, media_type: str = None,
                           export_root: str = None, settings: dict = None) -> list:
    """
    Folds everything from state.json into list_media()'s disk result:
    - Titles exported to JSON (streamable media with links in task records)
    - Titles Analyze discovered awaiting a "Queue" click
    - Titles actively exporting or downloading

    settings is only consulted for year_belongs_in_folder() -- whether a
    discovered/queued series' id includes " (Year)" -- so it stays
    consistent with however that title's eventual real target_path gets
    computed (see tasks.compute_target_path()). A finished/running task's
    own stored target_path is always used as-is instead; only the
    synthesized ids computed here need the flag.
    """
    by_id = {e["id"]: e for e in entries}

    def bucket_ok(category: str) -> bool:
        is_series = category in _SERIES_CATEGORIES
        if media_type == "series" and not is_series:
            return False
        if media_type == "movie" and is_series:
            return False
        return True

    def get_or_create(media_id: str, category: str, title: str, year) -> dict:
        entry = by_id.get(media_id)
        if entry is None:
            entry = {
                "id": media_id,
                "category": category,
                "is_series": category in _SERIES_CATEGORIES,
                "title": title,
                "year": year,
                "file_count": 0,
                "streamable_count": 0,
                "downloaded_count": 0,
                "has_folder": False,
                "status": "ready",
            }
            by_id[media_id] = entry
            entries.append(entry)
        return entry

    # 1. Discovered items
    for item in discovered_items:
        category = item.get("category")
        if category not in CATEGORY_LABELS or not bucket_ok(category):
            continue
        media_id = compute_target_path(category, item["title"], item.get("year"),
                                        include_year=year_belongs_in_folder(category, settings))
        entry = get_or_create(media_id, category, item["title"], item.get("year"))
        entry["status"] = "discovered"
        entry["discovered_id"] = item["id"]
        if item.get("tmdb_id"):
            entry["tmdb_id"] = item["tmdb_id"]

    # 2. Completed export tasks (streamable media in JSON)
    for task in tasks:
        if task.get("task_type") == "download" or task.get("status") != "done":
            continue
        category = task.get("category")
        title = task.get("detected_title")
        year = task.get("detected_year")
        if category not in CATEGORY_LABELS:
            category, title, year = category_title_year_from_target_path(task.get("target_path"))
        if category not in CATEGORY_LABELS or not bucket_ok(category):
            continue
        title = title or "Untitled"
        media_id = task.get("target_path") or compute_target_path(
            category, title, year, include_year=year_belongs_in_folder(category, settings))
        entry = get_or_create(media_id, category, title, year)
        if task.get("tmdb_id"):
            entry["tmdb_id"] = task["tmdb_id"]

        done_items = [it for it in task.get("items", []) if it.get("status") == "done"]
        abs_dir = None
        if export_root and os.path.isdir(export_root):
            try:
                abs_dir = validate_export_path(export_root, media_id)
            except ValueError:
                abs_dir = None

        downloaded_count = 0
        streamable_count = 0
        for it in done_items:
            rel_path = it.get("rel_path")
            abs_file = os.path.join(abs_dir, *rel_path.split("/")) if abs_dir else None
            if abs_file and os.path.isfile(abs_file) and os.path.splitext(abs_file)[1].lower() in core.VIDEO_EXTENSIONS:
                downloaded_count += 1
            else:
                streamable_count += 1

        # Merge with any count already on disk
        if entry.get("downloaded_count", 0) > downloaded_count:
            downloaded_count = entry["downloaded_count"]

        entry["downloaded_count"] = downloaded_count
        entry["streamable_count"] = streamable_count
        entry["file_count"] = streamable_count + downloaded_count
        entry["status"] = "downloaded" if downloaded_count > 0 and streamable_count == 0 else "ready"
        entry["has_folder"] = True  # Viewable detail page
        entry["task_id"] = task["id"]
        entry.pop("discovered_id", None)

    # 3. Queued / running / error export tasks
    PIPELINE_TASK_STATUSES = ("queued", "running", "error")
    for task in tasks:
        if task.get("task_type") == "download":
            continue
        if task.get("status") not in PIPELINE_TASK_STATUSES:
            continue
        category = task.get("category")
        title = task.get("detected_title")
        year = task.get("detected_year")
        if category not in CATEGORY_LABELS:
            category, title, year = category_title_year_from_target_path(task.get("target_path"))
        if category not in CATEGORY_LABELS or not bucket_ok(category):
            continue
        title = title or "Untitled"
        media_id = task.get("target_path") or compute_target_path(
            category, title, year, include_year=year_belongs_in_folder(category, settings))
        entry = get_or_create(media_id, category, title, year)
        if task.get("tmdb_id"):
            entry["tmdb_id"] = task["tmdb_id"]
        entry["file_count"] = max(entry.get("file_count", 0), task.get("files_written", 0))
        entry["status"] = task["status"]
        entry["task_id"] = task["id"]
        entry["task_progress"] = {
            "phase": task.get("phase"),
            "files_written": task.get("files_written", 0),
            "total_items": task.get("total_items", 0),
            "current_item": task.get("current_item", ""),
        }
        entry.pop("discovered_id", None)

    # 4. Download tasks overlay
    DOWNLOAD_STATUS_MAP = {"queued": "download_queued", "running": "downloading", "error": "download_error"}
    for task in tasks:
        if task.get("task_type") != "download" or task.get("status") not in DOWNLOAD_STATUS_MAP:
            continue
        entry = by_id.get(task.get("target_path"))
        if entry is None or entry.get("status") not in ("ready", "downloaded"):
            continue
        if task.get("tmdb_id"):
            entry["tmdb_id"] = task["tmdb_id"]
        entry["status"] = DOWNLOAD_STATUS_MAP[task["status"]]
        entry["task_id"] = task["id"]
        entry["task_progress"] = {
            "phase": task.get("phase"),
            "files_written": task.get("files_written", 0),
            "total_items": task.get("total_items", 0),
            "current_item": task.get("current_item", ""),
        }

    for entry in entries:
        entry.setdefault("status", "ready")
        entry.setdefault("has_folder", True)
        entry.setdefault("streamable_count", 0)
        entry.setdefault("downloaded_count", 0)
    return entries


def get_media_detail(export_root: str, media_id: str, task: dict = None) -> dict:
    """
    Full detail for one title: parsed title/year/category plus season/episode
    breakdown (series) or file list (movies) built from streamable items in
    JSON (task['items']) and downloaded files on disk.
    """
    norm_media_id = (media_id or "").replace("\\", "/")
    parts = norm_media_id.split("/", 1)
    label = parts[0]
    folder_name = parts[1] if len(parts) > 1 else parts[0]
    category = next((c for c, l in CATEGORY_LABELS.items() if l == label), None)
    title, year = _parse_title_year(folder_name)
    is_series = category in _SERIES_CATEGORIES

    detail = {
        "id": media_id,
        "category": category,
        "is_series": is_series,
        "title": title,
        "year": year,
    }

    abs_dir = None
    if export_root:
        try:
            abs_dir = validate_export_path(export_root, media_id)
        except ValueError:
            abs_dir = None

    seen_rel_paths = set()
    files = []

    # 1. From task items in JSON
    if task and task.get("items"):
        for it in task["items"]:
            if it.get("status") not in ("done", "pending"):
                continue
            rel_path = (it.get("rel_path") or "").replace("\\", "/")
            if not rel_path:
                continue
            filename = os.path.basename(rel_path)
            abs_file = os.path.join(abs_dir, *rel_path.split("/")) if abs_dir else None
            is_downloaded = bool(abs_file and os.path.isfile(abs_file) and os.path.splitext(abs_file)[1].lower() in core.VIDEO_EXTENSIONS)
            files.append({
                "filename": filename,
                "rel_path": rel_path,
                "downloaded": is_downloaded,
                "url": it.get("url"),
                "quality": it.get("quality", ""),
            })
            seen_rel_paths.add(rel_path)

    # 2. From disk for any extra downloaded files
    if abs_dir and os.path.isdir(abs_dir):
        for rel, _abs in _downloaded_files_recursive(abs_dir):
            norm_rel = rel.replace("\\", "/")
            if norm_rel not in seen_rel_paths:
                files.append({
                    "filename": os.path.basename(norm_rel),
                    "rel_path": norm_rel,
                    "downloaded": True,
                    "url": None,
                    "quality": "",
                })
                seen_rel_paths.add(norm_rel)

    # Overall "X/X downloaded" progress for the detail page header --
    # same downloaded_count/file_count shape the Movies/Series list
    # (augment_with_pipeline()) already exposes per-title, just computed
    # here from this title's own file list instead of task records, so
    # it's exact regardless of what's in `files` (JSON items, disk
    # leftovers, or both).
    detail["downloaded_count"] = sum(1 for f in files if f["downloaded"])
    detail["streamable_count"] = len(files) - detail["downloaded_count"]

    if is_series:
        seasons = {}
        for f in files:
            season_num, episode_num = parse_rel_path_season_episode(f["rel_path"])
            seasons.setdefault(season_num, []).append({
                "filename": f["filename"],
                "rel_path": f["rel_path"],
                "episode": episode_num,
                "downloaded": f["downloaded"],
                "url": f.get("url"),
                "quality": f.get("quality", ""),
            })
        result = []
        for season_num in sorted(seasons.keys(), key=lambda n: (n == 0, n)):
            episodes = sorted(seasons[season_num], key=lambda e: (e["episode"] is None, e["episode"] or 0, e["filename"]))
            result.append({"season": season_num, "label": _season_label(season_num), "episodes": episodes})
        detail["seasons"] = result
        detail["file_count"] = sum(len(s["episodes"]) for s in result)
    else:
        detail["files"] = sorted(files, key=lambda f: f["rel_path"])
        detail["file_count"] = len(detail["files"])

    return detail


def rename_media(export_root: str, media_id: str, new_folder_name: str) -> str:
    """
    The Movies/Series detail page's "edit path" control: renames a
    title's folder-name segment within its existing Category -- moving
    its on-disk folder (if any downloaded files exist there) to the new
    name. Returns the new "<Category label>/<folder name>" id. Raises
    ValueError on an empty/unsafe name or a destination that's already
    someone else's folder.

    Category is deliberately not editable here -- that's a
    re-classification (would also move the title between Movies/Series),
    not a rename. Callers (app.py's rename route) are responsible for
    retargeting whatever task/discovered record was keyed to the old id;
    this function only touches disk.
    """
    parts = (media_id or "").replace("\\", "/").split("/", 1)
    if len(parts) != 2:
        raise ValueError("invalid media id")
    label = parts[0]
    safe_name = core.sanitize_name((new_folder_name or "").strip())
    if not safe_name or safe_name == "_":
        raise ValueError("name can't be empty")
    new_media_id = f"{label}/{safe_name}"
    if not export_root or not os.path.isdir(export_root):
        return new_media_id

    old_abs = validate_export_path(export_root, media_id)
    new_abs = validate_export_path(export_root, new_media_id)
    if os.path.normcase(old_abs) == os.path.normcase(new_abs):
        # Same folder except for case (e.g. Windows) -- a plain os.rename
        # handles that; isdir(new_abs) below would otherwise look like a
        # collision with itself.
        if os.path.isdir(old_abs) and old_abs != new_abs:
            os.rename(old_abs, new_abs)
        return new_media_id
    if os.path.isdir(new_abs):
        raise ValueError("a title with that name already exists")
    if os.path.isdir(old_abs):
        os.makedirs(os.path.dirname(new_abs), exist_ok=True)
        shutil.move(old_abs, new_abs)
    return new_media_id


def delete_media(export_root: str, media_id: str) -> None:
    """Removes a title's entire folder from disk if present."""
    if not export_root or not os.path.isdir(export_root):
        return
    try:
        abs_dir = validate_export_path(export_root, media_id)
    except ValueError:
        return
    if os.path.isdir(abs_dir):
        shutil.rmtree(abs_dir)


def delete_media_file(export_root: str, media_id: str, file_rel_path: str) -> None:
    """
    Removes one downloaded video file within a title's folder if present.
    """
    if not export_root or not os.path.isdir(export_root):
        return
    try:
        abs_dir = validate_export_path(export_root, media_id)
        abs_file = validate_export_path(abs_dir, file_rel_path)
    except ValueError:
        return
    if os.path.isfile(abs_file):
        os.remove(abs_file)
        core.remove_link_sidecar(abs_file)
        parent_dir = os.path.dirname(abs_file)
        try:
            if os.path.isdir(parent_dir) and not os.listdir(parent_dir):
                os.rmdir(parent_dir)
        except OSError:
            pass


def build_download_items(export_root: str, media_id: str, source_task_items: list) -> list:
    """
    Filters source_task_items down to only those that have NOT yet been downloaded
    to disk.
    """
    abs_dir = None
    if export_root:
        try:
            abs_dir = validate_export_path(export_root, media_id)
        except ValueError:
            abs_dir = None

    items = []
    for it in source_task_items or []:
        if it.get("status") != "done":
            continue
        rel_path = it.get("rel_path")
        abs_file = os.path.join(abs_dir, *rel_path.split("/")) if abs_dir else None
        is_downloaded = bool(abs_file and os.path.isfile(abs_file) and os.path.splitext(abs_file)[1].lower() in core.VIDEO_EXTENSIONS)
        if not is_downloaded:
            items.append({"fid": it["fid"], "rel_path": it["rel_path"], "dest_path": it["dest_path"]})
    return items


def revert_file_to_stream(export_root: str, media_id: str, file_rel_path: str) -> None:
    """
    Undoes a download for one file: removes the real file from disk. The file
    remains streamable via its link in JSON.
    """
    abs_dir = validate_export_path(export_root, media_id)
    if not os.path.isdir(abs_dir):
        raise FileNotFoundError(media_id)
    abs_file = validate_export_path(abs_dir, file_rel_path)
    ext = os.path.splitext(abs_file)[1].lower()
    if not os.path.isfile(abs_file) or ext not in core.VIDEO_EXTENSIONS:
        raise FileNotFoundError(file_rel_path)
    core.revert_link_to_stream(abs_file)
