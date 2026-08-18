"""
tmdb_episodes.py
================
Given a confirmed TMDB tv id and what's actually on disk (see
media_library._scan_series_detail()), builds the complete season/episode
picture for the media detail page: every episode TMDB knows about, not
just the ones with a file, matched against disk files by picking
whichever of TMDB's available episode orderings ("groupings" -- the
default aired order plus any alternates it has, like absolute/DVD/
production order) lines up best with the files actually present. That's
what makes an anime release numbered "- 07" with no season marker at all
line up against the right season/episode, and what makes a season TMDB
has but the share never included show up as empty rows instead of not
existing at all.

The TMDB side (one show-details call, one call per season, one call per
alternate grouping) is the expensive, rarely-changing part -- cached
in-process per tmdb_id so the media detail page's live poll loop
(app.py's get_media_detail_route, hit every few seconds while the page is
open) doesn't refetch it every time; the disk side is cheap and always
read fresh by the caller, same as it already was before this existed.
"""

import time

CACHE_TTL_SECONDS = 6 * 3600
_CACHE = {}  # tmdb_id -> (fetched_at, groupings)


def _fetch_raw_groupings(tmdb, tmdb_id) -> list:
    """
    Returns every candidate ordering worth scoring against the disk
    files: [{"name": str, "episodes": [{"season_number",
    "episode_number","name","air_date","order"}, ...]}, ...]. "order" is
    a flattened 0-based position within that grouping specifically, used
    to match absolute-numbered (no real season marker) filenames.
    Element 0 is always the default aired-order season list (built from
    /tv/{id} + one /tv/{id}/season/{n} call per season) when the show
    has any episodes at all; any alternate orderings TMDB has
    (/tv/{id}/episode_groups) are appended after. Best-effort throughout
    -- a failed sub-fetch just drops that one season/grouping, not the
    whole result.
    """
    details = tmdb.get_tv_details(tmdb_id)
    if not details:
        return []

    groupings = []

    default_episodes = []
    order = 0
    seasons = sorted(details.get("seasons") or [], key=lambda s: s.get("season_number") or 0)
    for season in seasons:
        season_number = season.get("season_number")
        if season_number is None:
            continue
        for ep in tmdb.get_season_full(tmdb_id, season_number):
            default_episodes.append({
                "season_number": season_number,
                "episode_number": ep.get("episode_number"),
                "name": ep.get("name"),
                "air_date": ep.get("air_date"),
                "still_path": ep.get("still_path"),
                "order": order,
            })
            order += 1
    if default_episodes:
        groupings.append({"name": "default", "episodes": default_episodes})

    for group_meta in tmdb.get_episode_groups(tmdb_id):
        group = tmdb.get_episode_group_details(group_meta.get("id"))
        if not group:
            continue
        episodes = []
        order = 0
        for sub in group.get("groups") or []:
            for ep in sub.get("episodes") or []:
                episodes.append({
                    "season_number": ep.get("season_number"),
                    "episode_number": ep.get("episode_number"),
                    "name": ep.get("name"),
                    "air_date": ep.get("air_date"),
                    "still_path": ep.get("still_path"),
                    "order": order,
                })
                order += 1
        if episodes:
            groupings.append({"name": group_meta.get("name") or "alternate order", "episodes": episodes})

    return groupings


def _get_groupings(tmdb, tmdb_id, force_refresh: bool = False) -> list:
    now = time.monotonic()
    cached = _CACHE.get(tmdb_id)
    if not force_refresh and cached and (now - cached[0]) < CACHE_TTL_SECONDS:
        return cached[1]
    groupings = _fetch_raw_groupings(tmdb, tmdb_id)
    if groupings:
        # Never cache an empty/failed fetch -- a transient TMDB hiccup
        # shouldn't lock a title out of ever getting this for 6 hours;
        # let the next request just try again.
        _CACHE[tmdb_id] = (now, groupings)
    return groupings


def _disk_pairs(disk_seasons: list) -> list:
    """Flattens media_library._scan_series_detail()'s output into
    (season, episode, disk_episode_dict) tuples for scoring/matching.
    Season-less/absolute-numbered files are already (0, N) courtesy of
    classify.parse_season_episode()."""
    out = []
    for season in disk_seasons:
        for ep in season["episodes"]:
            if ep.get("episode") is not None:
                out.append((season["season"], ep["episode"], ep))
    return out


def _resolve_disk_targets(grouping: dict, disk_pairs: list) -> dict:
    """Maps each disk file to the (season_number, episode_number) key of
    the grouping episode it belongs to: a direct hit for a file that
    already carries a real season marker, or -- only for a season-less/
    absolute-numbered file (disk season 0) -- whichever grouping episode
    sits at that absolute position. The returned key isn't guaranteed to
    actually exist in the grouping (an absolute position past the end of
    it, e.g.) -- callers cross-check against the grouping's own episodes."""
    order_index = {e["order"]: e for e in grouping["episodes"]}
    targets = {}
    for season_num, episode_num, disk_ep in disk_pairs:
        if season_num == 0:
            hit = order_index.get(episode_num - 1)
            if hit is not None:
                targets[(hit["season_number"], hit["episode_number"])] = disk_ep
                continue
        targets[(season_num, episode_num)] = disk_ep
    return targets


def _score_grouping(grouping: dict, disk_pairs: list) -> int:
    """How many on-disk files this grouping actually accounts for --
    the "most compatible with the files" measure the winning grouping is
    picked by."""
    valid_keys = {(e["season_number"], e["episode_number"]) for e in grouping["episodes"]}
    targets = _resolve_disk_targets(grouping, disk_pairs)
    return sum(1 for key in targets if key in valid_keys)


def get_episode_info(tmdb, tmdb_id, season_number, episode_number) -> dict:
    """A single episode's {"name", "still_path"} for display -- app.py's
    /api/tasks route uses this to show a thumbnail + episode title on a
    download task's Activity/Downloads row instead of a bare filename.
    Deliberately just the default aired-order grouping (no disk-file
    scoring against alternates -- there's no disk_seasons to score here,
    just a (season, episode) already parsed from the task's own item
    rel_path), and it reuses _get_groupings()'s cache, so this never
    costs an extra TMDB call beyond whatever the media detail page (or an
    earlier task row) already triggered for this show. None when TMDB is
    unavailable, the show has no episode data, or no episode in the
    default grouping matches this (season, episode)."""
    groupings = _get_groupings(tmdb, tmdb_id)
    if not groupings:
        return None
    for ep in groupings[0]["episodes"]:
        if ep["season_number"] == season_number and ep["episode_number"] == episode_number:
            return {"name": ep.get("name"), "still_path": ep.get("still_path")}
    return None


def get_reconciled_seasons(tmdb, tmdb_id, disk_seasons: list, force_refresh: bool = False):
    """
    Returns the same [{"season","label","episodes":[...]}] shape
    media_library._scan_series_detail() does, except every episode TMDB
    knows about is present -- episodes[i]["on_disk"] is the matching disk
    episode dict (filename/rel_path/downloaded) or None when there's no
    file for it (yet) -- using whichever available TMDB ordering scores
    highest against disk_seasons (see _score_grouping()). Any disk file
    that still doesn't match anything in the winning grouping is appended
    under its own parsed season rather than silently dropped, so nothing
    already on disk can ever disappear from the view just because
    reconciliation couldn't place it.

    Returns None when there's no tmdb_id, no TMDB key, or the show has no
    episode data at all to reconcile against -- callers fall back to the
    plain disk-only seasons list in that case.
    """
    if not tmdb_id or not tmdb.available:
        return None

    groupings = _get_groupings(tmdb, tmdb_id, force_refresh=force_refresh)
    if not groupings:
        return None

    disk_pairs = _disk_pairs(disk_seasons)
    best = max(groupings, key=lambda g: _score_grouping(g, disk_pairs))
    targets = _resolve_disk_targets(best, disk_pairs)

    consumed_keys = set()
    seasons = {}
    for ep in best["episodes"]:
        season_num = ep["season_number"]
        if season_num is None:
            continue
        key = (season_num, ep["episode_number"])
        disk_ep = targets.get(key)
        if disk_ep is not None:
            consumed_keys.add(key)
        seasons.setdefault(season_num, []).append({
            "episode_number": ep["episode_number"],
            "name": ep.get("name"),
            "air_date": ep.get("air_date"),
            "still_path": ep.get("still_path"),
            "on_disk": disk_ep,
        })

    # Leftover disk files whose resolved key isn't actually one of the
    # winning grouping's own episodes (an absolute position past the end
    # of it, or just a mismatch) -- still shown, under their own parsed
    # season, instead of vanishing.
    for key, disk_ep in targets.items():
        if key in consumed_keys:
            continue
        season_num, episode_num = key
        seasons.setdefault(season_num, []).append({
            "episode_number": episode_num,
            "name": None,
            "air_date": None,
            "still_path": None,
            "on_disk": disk_ep,
        })

    result = []
    for season_num in sorted(seasons.keys(), key=lambda n: (n == 0, n)):
        episodes = sorted(
            seasons[season_num],
            key=lambda e: (e["episode_number"] is None, e["episode_number"] or 0),
        )
        label = "Specials" if season_num == 0 else f"Season {season_num}"
        result.append({"season": season_num, "label": label, "episodes": episodes})
    return result
