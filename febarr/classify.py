"""
classify.py
===========
Turns a raw folder/file name into a best-guess (title, year) and, given a
TMDB API key, a confident category classification (tv / movie / anime /
anime_movie). Falls back to filename heuristics when there's no TMDB key,
no match, or the lookup fails -- classification always returns *something*
(flagged with low confidence) rather than raising, since a wrong guess the
user can fix in the review step beats a failed share-add.

No third-party dependencies -- TMDB is called via urllib, same as the rest
of the project.
"""

import json
import os
import re
import urllib.parse
import urllib.request

TMDB_ANIMATION_GENRE_ID = 16

# Tags/markers release names commonly carry after the real title -- the
# first match's start position is where we truncate the title. Order
# doesn't matter; re.search finds the earliest match in the string.
_JUNK_PATTERN = re.compile(
    r"""\b(
        19\d{2}|20\d{2}                                   # year
        |S\d{1,2}(E\d{1,3})?                               # S01, S01E02
        |Season\s*\d+|Series\s*\d+
        |Complete
        |2160p|1080p|720p|480p
        |WEB[- ]?DL|WEBRip|BluRay|BDRip|HDTV|DVDRip|HDRip
        |x264|x265|H\s?264|H\s?265|HEVC|10\s?bit
        |AAC\d*|DDP?\d(\.\d)?|AC3|FLAC
        |REPACK|PROPER|EXTENDED|UNRATED|REMASTERED
        |MULTI|DUAL[- ]?AUDIO|SUBBED|DUBBED
    )\b""",
    re.IGNORECASE | re.VERBOSE,
)

_EPISODE_PATTERNS = [
    re.compile(r"S\d{1,2}E\d{1,3}", re.IGNORECASE),
    re.compile(r"\b\d{1,2}x\d{2,3}\b"),
    re.compile(r"\bSeason\s*\d+\b", re.IGNORECASE),
    re.compile(r"\bEp(?:isode)?\s*\d{1,3}\b", re.IGNORECASE),
    # Absolute-numbered episodes, e.g. "Title - 01 [1080p].mkv" -- the
    # dominant convention in fansub/anime release names, which otherwise
    # carry no S/E marker at all.
    re.compile(r"\s-\s\d{1,4}\s*(?:\[|\(|$)"),
]

_FANSUB_TAG = re.compile(r"^\s*\[[^\]]{1,24}\]")


def parse_group_title(raw_name: str):
    """
    Best-effort extraction of (title, year) from a release-style folder or
    file name, e.g. "Some.Show.S01.1080p.WEB-DL.x264-GROUP" -> ("Some
    Show", None), "Movie.Name.2019.1080p.BluRay" -> ("Movie Name", 2019).
    """
    name = (raw_name or "").strip()
    # Leading fansub/release-group tag, e.g. "[SubsPlease] Show - 01" --
    # strip it before anything else so it doesn't end up in the title or
    # pollute a TMDB search query.
    name = _FANSUB_TAG.sub("", name).strip()
    name = re.sub(r"[._]+", " ", name).strip()
    year = None

    match = _JUNK_PATTERN.search(name)
    if match:
        token = match.group(0)
        if re.fullmatch(r"19\d{2}|20\d{2}", token):
            year = int(token)
        title = name[: match.start()]
    else:
        title = name

    title = re.sub(r"[\[\(\{][^\]\)\}]*$", "", title)  # trailing dangling bracket
    title = re.sub(r"[-_\s]+$", "", title).strip(" -")
    title = re.sub(r"\s{2,}", " ", title).strip()

    return (title or name.strip() or raw_name, year)


def detect_episode_pattern(name: str) -> bool:
    return any(p.search(name or "") for p in _EPISODE_PATTERNS)


def looks_like_fansub_release(name: str) -> bool:
    return bool(_FANSUB_TAG.match(name or ""))


# -- episode-identity + quality parsing, for deduping multiple uploads of
# the same episode at different qualities down to a single file --------
_SXXEXX_RE = re.compile(r"S(\d{1,2})E(\d{1,3})", re.IGNORECASE)
_NXNN_RE = re.compile(r"\b(\d{1,2})x(\d{2,3})\b")
_ABS_EPISODE_RE = re.compile(r"\s-\s(\d{1,4})\s*(?:\[|\(|$)")

# Preference order the user wants when the same episode shows up as more
# than one separate file: 1080p first, then 720p, then 4k/2160p, then
# anything with no quality tag at all last. Lower rank = preferred.
_QUALITY_RANK_PATTERNS = [
    re.compile(r"\b1080p?\b", re.IGNORECASE),
    re.compile(r"\b720p?\b", re.IGNORECASE),
    re.compile(r"\b(2160p?|4k)\b", re.IGNORECASE),
]


def parse_episode_identity(filename: str):
    """
    Best-effort season+episode (or absolute episode number) key for
    grouping "the same episode released as more than one file" -- e.g.
    "Show.S01E05.1080p.mkv" and "Show.S01E05.720p.mkv" both key to
    "s01e05". Returns None if no episode marker is found (movies,
    specials, anything not episode-numbered) -- those are never deduped
    against each other.
    """
    name = filename or ""
    m = _SXXEXX_RE.search(name)
    if m:
        return f"s{int(m.group(1)):02d}e{int(m.group(2)):03d}"
    m = _NXNN_RE.search(name)
    if m:
        return f"s{int(m.group(1)):02d}e{int(m.group(2)):03d}"
    m = _ABS_EPISODE_RE.search(name)
    if m:
        return f"abs{int(m.group(1)):04d}"
    return None


def parse_season_episode(filename: str):
    """
    Like parse_episode_identity(), but returns the actual (season,
    episode) int pair instead of an opaque grouping key -- for display
    purposes (the media library's season/episode breakdown) rather than
    dedup grouping. Absolute-numbered episodes (no season marker at all,
    the anime-release convention) come back as (0, N) -- "season 0" is
    the same bucket Sonarr/Plex use for specials/unnumbered content.
    Returns None if no episode marker is found at all (movies, extras).
    """
    name = filename or ""
    m = _SXXEXX_RE.search(name)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = _NXNN_RE.search(name)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = _ABS_EPISODE_RE.search(name)
    if m:
        return 0, int(m.group(1))
    return None


def parse_episode_name(filename: str):
    """
    Extracts the episode title from a filename, if present -- the text
    between an S01E01-style marker and whatever release junk (quality/
    codec/group tags) follows it, e.g. "Show.S01E01.Pilot.1080p.mkv" ->
    "Pilot". Used to disambiguate between same-titled shows when a plain
    title search turns up more than one plausible TMDB match (see
    TMDBClient.search()) -- an episode name matched against a
    candidate's own episode list is a much stronger signal than
    popularity/year alone once there's genuinely more than one real
    candidate. Returns None if there's no S/E marker, or nothing
    meaningful survives after it -- plenty of releases go straight from
    the episode marker to quality tags with no episode title in the
    filename at all.
    """
    name = os.path.splitext(filename or "")[0]
    m = _SXXEXX_RE.search(name)
    if not m:
        return None
    rest = re.sub(r"[._]+", " ", name[m.end():])
    junk_match = _JUNK_PATTERN.search(rest)
    if junk_match:
        rest = rest[:junk_match.start()]
    rest = rest.strip(" -._")
    return rest or None


def quality_rank(filename: str) -> int:
    """Lower is preferred: 0=1080p, 1=720p, 2=4k/2160p, 3=no quality tag found."""
    name = filename or ""
    for rank, pattern in enumerate(_QUALITY_RANK_PATTERNS):
        if pattern.search(name):
            return rank
    return len(_QUALITY_RANK_PATTERNS)


def _result_year(result: dict):
    date = result.get("release_date") or result.get("first_air_date") or ""
    if len(date) >= 4 and date[:4].isdigit():
        return int(date[:4])
    return None


def is_anime_result(genre_ids: list, origin_country: list = None, original_language: str = None) -> bool:
    """
    TMDB has no "is anime" flag -- this is the same heuristic Sonarr/
    Radarr/Jellyfin's community uses: the Animation genre, combined with
    a Japan signal (origin_country for TV, original_language for movies
    which don't carry origin_country in TMDB's schema).
    """
    genre_ids = genre_ids or []
    origin_country = origin_country or []
    if TMDB_ANIMATION_GENRE_ID not in genre_ids:
        return False
    return "JP" in origin_country or original_language == "ja"


def normalize_list_item(media_type: str, item: dict) -> dict:
    """Turns one raw TMDB search-result entry into the shape the manual
    TMDB search endpoint hands back to the UI (poster, overview, parsed
    category, ...)."""
    title = item.get("title") or item.get("name") or "Untitled"
    original_title = item.get("original_title") or item.get("original_name") or title
    is_anime = is_anime_result(item.get("genre_ids"), item.get("origin_country"), item.get("original_language"))
    category = ("anime" if is_anime else "tv") if media_type == "tv" else ("anime_movie" if is_anime else "movie")
    return {
        "tmdb_id": item.get("id"),
        "media_type": media_type,
        "title": title,
        "original_title": original_title,
        "original_language": item.get("original_language"),
        "year": _result_year(item),
        "category": category,
        # Relative path -- the client builds the full image URL itself
        # (https://image.tmdb.org/t/p/<size><poster_path>); no API key
        # needed to fetch TMDB images, only to search/list them. Same
        # deal for backdrop_path (a wide banner image, used as the media
        # detail page's hero background) -- already present on every
        # /search/multi result, no extra request needed for it.
        "poster_path": item.get("poster_path"),
        "backdrop_path": item.get("backdrop_path"),
        "overview": item.get("overview") or "",
    }


class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str = ""):
        self.api_key = (api_key or "").strip()

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def get_season_episode_names(self, tv_id, season_number: int = 1) -> list:
        """Episode names for one season of a TV show -- used only to
        disambiguate between same-titled shows in search() below. []
        on any failure/unavailability/nonexistent season, never raises."""
        if not self.available or not tv_id:
            return []
        params = {"api_key": self.api_key}
        url = f"{self.BASE_URL}/tv/{tv_id}/season/{season_number}?{urllib.parse.urlencode(params)}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception:
            return []
        return [ep.get("name") for ep in (data.get("episodes") or []) if ep.get("name")]

    def _fetch(self, path: str, params: dict = None):
        """Shared GET-JSON helper for the detail/episode-group endpoints
        below -- same try/except/timeout=15 shape every TMDB call in this
        class already uses inline elsewhere; returns None (never raises)
        on any failure so a TMDB hiccup degrades gracefully instead of
        breaking the detail page."""
        if not self.available:
            return None
        query = dict(params or {})
        query["api_key"] = self.api_key
        url = f"{self.BASE_URL}{path}?{urllib.parse.urlencode(query)}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

    def get_tv_details(self, tv_id):
        """/tv/{id} -- backdrop_path/poster_path/overview/seasons (each
        {season_number, episode_count, name}) for a confirmed tv id.
        None on failure/unavailability -- see _fetch(). Used once we
        already have a confirmed tmdb_id (from the matching finished
        export task, see app.py's get_media_detail_route), so this is
        the authoritative detail lookup -- unlike search()/search_many(),
        it isn't a fuzzy title guess."""
        if not tv_id:
            return None
        return self._fetch(f"/tv/{tv_id}")

    def get_movie_details(self, movie_id):
        """/movie/{id} -- backdrop_path/poster_path/overview for a
        confirmed movie id. None on failure/unavailability -- see
        _fetch(); same "authoritative, not a guess" role as
        get_tv_details() above."""
        if not movie_id:
            return None
        return self._fetch(f"/movie/{movie_id}")

    def get_season_full(self, tv_id, season_number: int) -> list:
        """Full episode objects (episode_number/name/air_date) for one
        season -- the "default aired order" grouping
        febarr.tmdb_episodes builds from. [] on failure/nonexistent
        season, never raises. Separate from get_season_episode_names()
        above, which only returns bare names for search() disambiguation
        -- this needs the full objects."""
        if not tv_id:
            return []
        data = self._fetch(f"/tv/{tv_id}/season/{season_number}")
        return (data or {}).get("episodes") or []

    def get_episode_groups(self, tv_id) -> list:
        """/tv/{id}/episode_groups -- TMDB's alternate episode orderings
        for a show (absolute order, DVD order, production order, ...),
        if it has any. Each entry is {"id","name","type","group_count",
        "episode_count"}; the full episode list for one is a separate
        fetch, see get_episode_group_details(). [] if the show has none,
        or on failure."""
        if not tv_id:
            return []
        data = self._fetch(f"/tv/{tv_id}/episode_groups")
        return (data or {}).get("results") or []

    def get_episode_group_details(self, group_id):
        """/tv/episode_group/{id} -- one alternate ordering's actual
        episode list, split into its own "groups" (typically its own
        idea of seasons), each holding episodes carrying
        season_number/episode_number (their ORIGINAL numbering) plus
        their position within this grouping. None on failure/
        unavailability."""
        if not group_id:
            return None
        return self._fetch(f"/tv/episode_group/{group_id}")

    def _disambiguate_by_episode_names(self, candidates: list, sample_filenames: list):
        """
        Title/popularity/year alone can't tell apart two real shows that
        happen to share an exact title (a same-named remake, a show sold
        under the same title in different countries, ...). If we have
        actual episode filenames to go on, their episode *names* against
        each candidate's own season-1 episode list is a much stronger
        signal -- returns whichever candidate's episodes actually match
        what's in the share, or None if there's no useful overlap (falls
        back to the normal popularity-based pick).
        """
        parsed_names = {n.strip().lower() for n in (parse_episode_name(f) for f in sample_filenames) if n}
        if not parsed_names:
            return None
        best_candidate = None
        best_overlap = 0
        for r in candidates:
            episode_names = {e.strip().lower() for e in self.get_season_episode_names(r.get("id"), 1)}
            overlap = len(parsed_names & episode_names)
            if overlap > best_overlap:
                best_overlap = overlap
                best_candidate = r
        return best_candidate

    def search(self, title: str, year: int = None, sample_filenames: list = None):
        """
        Returns {"media_type","title","year","is_anime","tmdb_id"} for the
        best-scoring movie/tv match, or None if unavailable/no match/error.
        Never raises -- a TMDB hiccup should just fall back to heuristics.

        sample_filenames (optional): a few real filenames from the share
        (episode files, ideally) -- only used when the plain title search
        comes back genuinely ambiguous (more than one TV result with the
        exact same title), to pick between them by episode name instead
        of just guessing the most popular one. See
        _disambiguate_by_episode_names().
        """
        if not self.available or not title:
            return None
        params = {"api_key": self.api_key, "query": title, "include_adult": "false"}
        url = f"{self.BASE_URL}/search/multi?{urllib.parse.urlencode(params)}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

        results = [r for r in data.get("results", []) if r.get("media_type") in ("movie", "tv")]
        if not results:
            return None

        title_lower = title.strip().lower()

        def score(r):
            s = float(r.get("popularity") or 0)
            r_year = _result_year(r)
            if year and r_year == year:
                s += 1000
            name = (r.get("title") or r.get("name") or "").strip().lower()
            if name == title_lower:
                s += 500
            return s

        best = None
        exact_tv_matches = [
            r for r in results
            if r.get("media_type") == "tv" and (r.get("name") or "").strip().lower() == title_lower
        ]
        if len(exact_tv_matches) > 1 and sample_filenames:
            best = self._disambiguate_by_episode_names(exact_tv_matches, sample_filenames)

        if best is None:
            best = max(results, key=score)
        is_anime = is_anime_result(best.get("genre_ids"), best.get("origin_country"), best.get("original_language"))
        return {
            "media_type": best["media_type"],
            "title": best.get("title") or best.get("name") or title,
            "year": _result_year(best),
            "is_anime": is_anime,
            "tmdb_id": best.get("id"),
        }

    def search_many(self, query: str, limit: int = 8) -> list:
        """
        For the "search TMDB to add a title" UI -- unlike search() (which
        picks one best match for auto-classification), this returns
        several candidates, normalized and ready to display (poster,
        overview, etc.) or hand straight back to the add-to-library
        endpoint. [] if unavailable/no results/error.
        """
        if not self.available or not query:
            return []
        params = {"api_key": self.api_key, "query": query, "include_adult": "false"}
        url = f"{self.BASE_URL}/search/multi?{urllib.parse.urlencode(params)}"
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception:
            return []

        results = [r for r in data.get("results", []) if r.get("media_type") in ("movie", "tv")]
        results.sort(key=lambda r: float(r.get("popularity") or 0), reverse=True)
        return [normalize_list_item(r["media_type"], r) for r in results[:limit]]


def _structural_media_type(file_count, has_season_subfolder: bool):
    """
    What the share's own shape says about movie vs TV, independent of
    (and more trustworthy than) title-matching: a season folder is
    unambiguously a series; a single video file is almost always a
    movie; more than one is almost always episodes. Returns "tv",
    "movie", or None when there isn't enough structural signal either way.
    """
    if has_season_subfolder:
        return "tv"
    if file_count == 1:
        return "movie"
    if file_count and file_count > 1:
        return "tv"
    return None


def classify_group(
    raw_name: str,
    tmdb: TMDBClient,
    sample_filenames: list = None,
    file_count: int = None,
    has_season_subfolder: bool = False,
) -> dict:
    """
    Returns {"category","title","year","source","confidence","tmdb_id"}.
    category is one of tv/movie/anime/anime_movie. source is "tmdb" (high
    confidence) or "heuristic" (low confidence, filename-guessed -- always
    worth a glance before queueing).

    TMDB's title search can match the wrong entry (or a same-named movie
    when the share is actually a series, or vice versa) -- its media_type
    is only as good as the title match. The share's actual structure
    (file_count / has_season_subfolder, from grouping.py's peek inside
    each folder) is ground truth and overrides it when the two disagree;
    confidence drops to "low" in that case so it's still worth a glance.
    sample_filenames also doubles as episode-name disambiguation input
    when the title search itself turns up more than one same-titled show
    -- see TMDBClient.search().
    """
    structural = _structural_media_type(file_count, has_season_subfolder)
    title, year = parse_group_title(raw_name)
    match = tmdb.search(title, year, sample_filenames=sample_filenames) if tmdb.available else None
    if match:
        media_type = match["media_type"]
        confidence = "high"
        if structural and structural != media_type:
            media_type = structural
            confidence = "low"
        category = ("anime" if match["is_anime"] else "tv") if media_type == "tv" else ("anime_movie" if match["is_anime"] else "movie")
        return {
            "category": category,
            "title": match["title"] or title,
            "year": match["year"] or year,
            "source": "tmdb",
            "confidence": confidence,
            "tmdb_id": match["tmdb_id"],
        }

    samples = sample_filenames or []
    if structural:
        is_tv = structural == "tv"
    else:
        is_tv = detect_episode_pattern(raw_name) or any(detect_episode_pattern(f) for f in samples)
    is_anime_guess = looks_like_fansub_release(raw_name) or any(looks_like_fansub_release(f) for f in samples)
    category = ("anime" if is_anime_guess else "tv") if is_tv else ("anime_movie" if is_anime_guess else "movie")
    return {
        "category": category,
        "title": title,
        "year": year,
        "source": "heuristic",
        "confidence": "low",
        "tmdb_id": None,
    }
