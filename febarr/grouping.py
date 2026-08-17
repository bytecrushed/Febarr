"""
grouping.py
===========
Looks at a share's root-level listing and decides how to split it into
independent title groups -- the input to classify.classify_group() and,
ultimately, one task per group.

Grouping rules:
  - Root-level folders that don't themselves look like a title -- a
    genre bucket ("Action", "Anime"), a bare year ("2020"), or any other
    purely organizational folder a share happens to nest titles under --
    get expanded and recursed into (up to _MAX_ORGANIZATIONAL_DEPTH
    levels) instead of being treated as a title themselves. A folder
    "looks like a title" if it has a season-like subfolder, or its own
    video files that all share one parsed base title; see
    _iter_content_folders() for exactly how that's decided. This
    applies no matter how deeply titles are nested, not just at the
    literal root.
  - If the (expanded) root contains season-like folders (Season 1, S01,
    Specials, ...) directly, the whole share is treated as ONE group --
    those folders belong to a single show, not several, so splitting on
    them would wrongly chop one show into pieces per season. There's no
    folder name at the root to take a title from in this case (see
    _derive_title_from_season_folders()), so it peeks one level into a
    season folder and parses a title guess off an episode filename
    instead -- same trick the loose-file bucketing below uses. Only
    falls back to the raw share key if that peek turns up nothing at all
    (empty/inaccessible season folders).
  - Loose video files sitting directly at the root (or inside an
    organizational folder) are bucketed by their parsed base title
    (stripping episode markers/quality tags) into "fids" groups -- so
    "Show S01E01.mkv" + "Show S01E02.mkv" become one group, while two
    unrelated loose movie files dumped in the same folder become two.

Every "folder" group also carries file_count and has_season_subfolder --
ground truth about the share's actual shape, already gathered while
deciding whether that folder was a title in the first place (no
duplicate listing call), which classify.classify_group() uses to
override/corroborate TMDB's title-matched media_type. A same-named movie
and series can otherwise get mixed up from title alone; a season folder
or a pile of files can't lie about which one this actually is.
"""

import os
import re

from . import classify
from .core import VIDEO_EXTENSIONS

_SEASON_FOLDER_RE = re.compile(r"^(season\s*\d+|s\d{1,2}|series\s*\d+|specials?)$", re.IGNORECASE)
_YEAR_FOLDER_RE = re.compile(r"^[\[\(]?(19\d{2}|20\d{2})[\]\)]?$")

# How many levels of pure-organizational nesting (genre/year/random-label
# folders with no title signal of their own) to walk through looking for
# real content before giving up and just treating whatever's left as a
# title as-is. A real share is rarely organized more than 2-3 folders
# deep; this is a safety net against a pathological/misread share turning
# into an unbounded number of listing calls, not a expected depth.
_MAX_ORGANIZATIONAL_DEPTH = 4


def _is_video(item: dict) -> bool:
    name = item.get("file_name") or item.get("name") or ""
    return os.path.splitext(name)[1].lower() in VIDEO_EXTENSIONS


def _fid(item: dict):
    return item.get("fid") if item.get("fid") is not None else item.get("id")


_TRAILING_ABS_EPISODE_RE = re.compile(r"\s*-\s*\d{1,4}$")


def _base_title(name: str) -> str:
    """
    Same title parsing the loose-file bucketing below uses, plus one
    extra step: parse_group_title() strips S01E01-style markers but
    doesn't know about the absolute-numbering convention anime releases
    favor instead ("Show - 01", "Show - 02", no season/episode letters
    at all) since that's episode-identity parsing's job elsewhere, not
    title parsing's. Without stripping it here too, "Show - 01" and
    "Show - 02" would look like two different titles instead of two
    episodes of the same one, and get wrongly split apart.
    """
    base, _year = classify.parse_group_title(os.path.splitext(name)[0])
    base = (base or name).strip()
    base = _TRAILING_ABS_EPISODE_RE.sub("", base).strip()
    return base.lower()


def _iter_content_folders(api, share_key: str, folders: list, ctx=None, depth: int = 0, min_release_year=None):
    """
    Recursively descends into folders that don't look like an actual
    title -- a genre folder, a year folder, any other organizational
    bucket -- until it finds folders that DO, no matter how many levels
    deep they're nested. A folder is judged to BE a title (not just a
    container) if:
      - it has a season-like subfolder directly inside (unambiguous), or
      - its own video files all share one parsed base title (a movie's
        file(s), or a show's episode files sitting loose in its folder).
    A folder whose video files parse to more than one distinct base
    title (e.g. a "2022" folder with several unrelated movies dumped
    straight in it) is NOT content -- those files get bucketed
    individually instead of lumped under the container's own name.

    A generator, yielding each finding the moment it's made rather than
    collecting everything into two lists first -- lets a caller (the
    streaming add-a-share pipeline, see analyzer.py) act on early matches
    while a large/deeply-organized share is still being walked, instead
    of waiting for the whole tree. Yields:
      ("folder", (folder_item, structure)) -- a real title, already
        peeked once (the structure dict classify_group() needs), so the
        caller never lists a folder twice.
      ("loose", video_item) -- a video file sitting directly inside an
        organizational folder with no real title wrapper, to be bucketed
        by parsed title exactly like true root-level loose files.

    A folder that can't be listed (network error, etc.) is yielded as
    content with no structural signal rather than silently dropped --
    same "don't lose it" principle the old single-level peek used.

    min_release_year (optional): a folder whose own name is nothing but
    a bare release year ("2015", "[2015]") older than this is skipped
    outright -- not even listed, let alone recursed into -- since a
    year-named folder is never itself a title, only ever a container for
    titles released that year, and the user's said they don't want
    anything from before this year. This is purely a fast-path: it can
    only trigger on a folder whose name IS a year, never on a real title
    folder (even one with a year in its name, like "Movie (2015)" --
    that doesn't match the bare-year pattern at all).
    """
    for folder in folders:
        fid = _fid(folder)
        if fid is None:
            continue
        name = folder.get("file_name") or folder.get("name") or "Untitled"

        if min_release_year:
            year_match = _YEAR_FOLDER_RE.match(name.strip())
            if year_match and int(year_match.group(1)) < min_release_year:
                if ctx is not None:
                    ctx.log(f"  Skipping '{name}' -- older than the minimum release year ({min_release_year}).")
                continue

        if ctx is not None:
            ctx.check_cancelled()
            ctx.set_current(f"peeking '{name}'...")
        try:
            children = api.get_file_list(share_key, fid)
        except Exception:
            yield ("folder", (folder, {"file_count": None, "has_season_subfolder": False, "sample_filenames": []}))
            continue

        child_folders = [c for c in children if c.get("is_dir") == 1]
        child_videos = [c for c in children if c.get("is_dir") != 1 and _is_video(c)]
        has_season_subfolder = any(
            _SEASON_FOLDER_RE.match((c.get("file_name") or c.get("name") or "").strip())
            for c in child_folders
        )
        structure = {
            "file_count": len(child_videos),
            "has_season_subfolder": has_season_subfolder,
            "sample_filenames": [(c.get("file_name") or c.get("name") or "") for c in child_videos[:5]],
        }

        if has_season_subfolder or depth >= _MAX_ORGANIZATIONAL_DEPTH:
            yield ("folder", (folder, structure))
            continue

        # Tracks whether this folder turned out to genuinely be an
        # organizational container (some of its contents got yielded
        # elsewhere -- loose videos, or a recursive descent that itself
        # found real content). If nothing organizational actually
        # happened -- an empty folder, or a peek that came back with
        # nothing usable -- fall back to yielding the folder as content
        # rather than silently losing it, same guarantee a listing
        # failure gets just above.
        resolved_as_container = False

        if child_videos:
            bases = {_base_title(c.get("file_name") or c.get("name") or "") for c in child_videos}
            if len(bases) <= 1:
                # One title's files (a movie's file(s), or a show's
                # episodes sitting loose) -- this folder IS the title.
                yield ("folder", (folder, structure))
                continue
            # Several distinctly-titled files dumped straight in an
            # organizational folder -- bucket them individually rather
            # than lumping them under this folder's (meaningless) name.
            for v in child_videos:
                yield ("loose", v)
            resolved_as_container = True

        if child_folders:
            yield from _iter_content_folders(
                api, share_key, child_folders, ctx=ctx, depth=depth + 1, min_release_year=min_release_year
            )
            resolved_as_container = True

        if not resolved_as_container:
            yield ("folder", (folder, structure))


def _derive_title_from_season_folders(api, share_key: str, season_folders: list, ctx=None):
    """
    A share_root share has no folder name to take a title from -- the
    root listing is just the share key plus a pile of "Season 1"/"S01"/
    etc. folders, none of which say what show this is. But the episode
    *filenames* one level inside those folders almost always do (e.g.
    "Show Name S01E01 1080p.mkv"), the same way a loose pile of episode
    files at the root gets bucketed by title elsewhere in this module.

    Peeks into season folders (in listed order, skipping empty/unlistable
    ones -- a "Specials" folder with nothing in it yet is common) until
    one actually has a video file, parses a title guess off its filename,
    and returns (raw_name, sample_filenames). Returns (None, []) if none
    of them turn up anything, so the caller can fall back to the share
    key like before rather than fail outright.
    """
    for folder in season_folders:
        if ctx is not None:
            ctx.check_cancelled()
        fid = _fid(folder)
        if fid is None:
            continue
        name = folder.get("file_name") or folder.get("name") or ""
        if ctx is not None:
            ctx.set_current(f"peeking '{name}' for a title...")
        try:
            children = api.get_file_list(share_key, fid)
        except Exception:
            continue
        videos = [c for c in children if c.get("is_dir") != 1 and _is_video(c)]
        if not videos:
            continue
        samples = [(v.get("file_name") or v.get("name") or "") for v in videos[:5]]
        base, _year = classify.parse_group_title(os.path.splitext(samples[0])[0])
        base = base.strip()
        if base:
            return base, samples
    return None, []


def iter_share_groups(api, share_key: str, ctx=None, min_release_year=None):
    """
    Yields each raw (unclassified) group the moment it's found instead of
    walking the entire share before returning anything:
      {"kind": "folder"|"fids"|"share_root", "root_fid": int|None,
       "root_items": [{"fid","file_name"}, ...]|None,
       "raw_name": str, "sample_filenames": [str, ...],
       "file_count": int|None, "has_season_subfolder": bool}

    "folder"-kind groups (the common case -- one folder per title) come
    out as soon as each one is peeked. "fids"-kind groups (loose video
    files bucketed by parsed title) necessarily come out only after the
    whole folder walk finishes, since a file bucket can't be considered
    complete -- and safely handed off -- until nothing else might still
    join it.

    Used by the streaming add-a-share pipeline (see analyzer.py) so a
    confident match can be queued as an export immediately, without
    waiting for a large/deeply-organized share to finish being walked.

    ctx is optional (a core.RunContext, or anything with the same
    set_current/check_cancelled duck-typed shape) -- purely for progress
    reporting/cancellation during the recursive folder walk, which is the
    slow part on a share with a lot of folders. When omitted, this runs
    exactly as before (silent, uncancellable).

    min_release_year is optional -- see _iter_content_folders() for what
    it actually does (skip bare-year folders older than it, without even
    listing them).
    """
    root_items = api.get_file_list(share_key, 0)
    root_folders = [it for it in root_items if it.get("is_dir") == 1]
    root_files = [it for it in root_items if it.get("is_dir") != 1]

    # The root's own folders being season-like is a different, simpler
    # case than "organizational folder somewhere in the tree" -- it means
    # there's no title wrapper at all, not even one to expand past.
    season_like = [
        f for f in root_folders
        if _SEASON_FOLDER_RE.match((f.get("file_name") or f.get("name") or "").strip())
    ]
    if season_like:
        raw_name, sample_filenames = _derive_title_from_season_folders(api, share_key, season_like, ctx=ctx)
        yield {
            "kind": "share_root",
            "root_fid": None,
            "root_items": None,
            "raw_name": raw_name or share_key,
            "sample_filenames": sample_filenames,
            "file_count": None,
            "has_season_subfolder": True,
        }
        return

    loose_from_organizational = []
    for kind, payload in _iter_content_folders(
        api, share_key, root_folders, ctx=ctx, min_release_year=min_release_year
    ):
        if kind == "loose":
            loose_from_organizational.append(payload)
            continue
        folder, structure = payload
        yield {
            "kind": "folder",
            "root_fid": _fid(folder),
            "root_items": None,
            "raw_name": folder.get("file_name") or folder.get("name") or "Untitled",
            **structure,
        }

    video_files = [it for it in root_files if _is_video(it)] + loose_from_organizational

    buckets = {}  # normalized base title -> {"raw_name": str, "items": [...]}
    for item in video_files:
        name = item.get("file_name") or item.get("name") or ""
        base, _year = classify.parse_group_title(os.path.splitext(name)[0])
        key = base.lower().strip() or name.lower()
        bucket = buckets.setdefault(key, {"raw_name": base or name, "items": []})
        bucket["items"].append(item)

    for bucket in buckets.values():
        items = bucket["items"]
        yield {
            "kind": "fids",
            "root_fid": None,
            "root_items": [{"fid": _fid(it), "file_name": it.get("file_name") or it.get("name")} for it in items],
            "raw_name": bucket["raw_name"],
            "sample_filenames": [(it.get("file_name") or it.get("name") or "") for it in items[:5]],
            "file_count": len(items),
            "has_season_subfolder": False,
        }
