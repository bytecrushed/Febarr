# Febarr

A small persistent service that exports Febbox shares into `.strm` files,
with a web UI for queuing multiple exports, watching live progress, and
saving your Febbox login cookie / FlareSolverr settings so you don't have
to re-enter them each run.

`febbox_export.py` is the original one-shot interactive CLI script and
still works standalone; the app below wraps the same export engine
(`febarr/core.py`) in a persistent server + queue.

## Setup

```bash
pip install -r requirements.txt
```

## Run

```bash
python app.py
```

Then open **http://127.0.0.1:5000**. Leave the process running -- queued
exports keep processing in the background as long as the server is up.

Options:

```bash
python app.py --host 0.0.0.0 --port 5000   # expose beyond localhost
python app.py --debug                      # Flask debug mode
```

By default Febarr binds to `127.0.0.1` (localhost only). Your Febbox
login cookie is stored in plain text in `data/state.json` and used
server-side for every export -- don't bind to `0.0.0.0` on a network you
don't trust. If you do, set a username/password (Settings -> Account,
see below) first.

## Docker

```bash
docker compose up -d
```

Then open **http://localhost:5000**. This builds the image locally from
the included `Dockerfile` and starts one container, with two named
volumes so settings/queue/library (`/data`) and exported/downloaded
files (`/exports`) both survive a rebuild or `docker compose down` --
inspect `docker-compose.yml` if you'd rather point `/exports` at a bind
mount (a real disk path, a NAS share, ...) instead of a named volume.

Need FlareSolverr too (see below)? `docker compose --profile
flaresolverr up -d` brings up both containers together on the same
network -- Settings -> FlareSolverr URL defaults to `http://
flaresolverr:8191/v1` (the container's name, not `localhost`) the first
time either container starts with no `data/state.json` yet.

Without Compose, plain `docker run` works too -- you're responsible for
the two volumes yourself in that case:

```bash
docker build -t febarr .
docker run -d -p 5000:5000 \
  -v febarr-data:/data \
  -v febarr-exports:/exports \
  --name febarr febarr
```

A few things specific to the container, everything else above (login
cookie storage, binding, the login/auth section right below) applies the
same as running it directly:

- It's a single process, single gunicorn worker, by design -- the task
  queue/library/analyze-jobs/saved-links all live in that one process's
  memory, so a second worker would just silently diverge from the first
  instead of sharing state. Don't raise `--workers` in the Dockerfile's
  `CMD`. Scale by giving that one worker more `--threads` instead
  (already generous by default) -- fine for an I/O-bound app like this
  one, whose real bottleneck is waiting on Febbox/TMDB/FlareSolverr, not
  CPU.
- `FEBARR_DATA_DIR` and `FEBARR_DEFAULT_EXPORT_ROOT` (baked into the
  image as `/data`/`/exports`) only set *first-run* defaults -- once
  `data/state.json` exists (i.e. after the first successful start),
  whatever's saved there always wins, identically to running it
  directly.
- `GET /healthz` is intentionally unauthenticated (unlike everything
  else once you've set a login) so Docker's built-in `HEALTHCHECK` --
  already wired into the image -- works the same whether or not you've
  set one.

## Login (optional)

Off by default -- the app stays fully open, same as before this existed.
Set a username and password under Settings -> **Account / login** and
every page/API route (except the login page itself) requires signing in
from then on. Worth doing before binding to `0.0.0.0` or exposing the
port beyond your own machine. "Disable login" turns it back off.
Credentials (as a salted hash, never the plaintext) and the session
signing key persist in `data/state.json` like everything else, so
logins survive a restart -- confirmed with an actual server restart, not
just a code read.

## Using it

The app is a sidebar of pages -- **Activity**, **Movies**, **Series**,
**Settings** -- with one search bar in the header, always available,
that does either of two things depending on what you type into it:

- **Paste a Febbox share link** and it streams straight into the
  library. Febarr walks the share, and the moment it finds a title with
  a confident TMDB match, it lands in **Movies** or **Series** tagged
  **Discovered** immediately -- watch it show up as the rest of the
  share is still being walked, no review/confirm step in the way. A
  share with a mix of shows and movies sorts into all the right
  categories at once. It's *not* queued for export yet, though -- that's
  a separate, explicit click (see below). Anything that doesn't get a
  confident match, or is already covered by something already in the
  library/queue/on disk, is recorded instead and shown in Activity
  tagged **Rejected**, so you can see what got skipped and why (`from
  "raw folder name" -- no confident TMDB match`) rather than it silently
  never appearing.
- **Type a title** instead and it searches your own library --
  everything already Discovered, Queued/Exporting, or Ready across
  Movies and Series -- not TMDB; there's no standalone "look it up"
  lookup separate from an actual share link. Click a result to jump
  straight to it. (TMDB itself is still used behind the scenes for
  auto-classification, media-detail metadata, and Grid-view posters --
  just not as something you search through here.)
- **Settings**: Febbox login cookie (the `ui` cookie value from a
  logged-in febbox.com session), an optional TMDB API key (needed for
  both of the above), FlareSolverr URL, the export root directory on
  this machine, how many exports can run in parallel, an optional
  auto-resync interval (see below), and a minimum release year (see
  below). Saved settings persist across restarts. Leaving the
  cookie/key fields blank on save keeps whatever is already stored.
- **Activity** is where everything in motion or waiting to be shows up
  -- exports, resyncs, background analyze jobs, and rejected titles --
  as a real table. Rows are sorted by status priority (running, then
  background-active, then queued, then everything else), so the top
  rows always match what's actually running right now; ties break by
  recency. Finished/rejected items are hidden by default -- the
  **Active** filter (the default view) shows only what's running,
  queued, or an in-progress background job; switch to **Completed** for
  done/errored/cancelled history, **Rejected** for titles that didn't
  get a confident match, or **All** for everything. A second filter bar
  (**All types**/**Movies**/**Series**) narrows by category on top of
  that, independently. The page-size dropdown (10/20/50/100, default
  20) and your last-used filters persist across reloads (localStorage).
  Each export task tracks every video file individually, so you get a
  real progress bar (discovering the tree, then N/M files written), not
  just a spinner. **Cancel** stops a queued or in-flight task, and it's
  deliberately "soft": there's no "Cancelled" status to leave behind --
  a queued task lands straight back in Movies/Series as a fresh
  Discovered item the instant you click it, and a running one does the
  same the moment it actually stops (files already written stay written;
  it just isn't an export task anymore). **Resume** retries an errored
  task from wherever it got to (files already exported aren't redone);
  **Resync** re-fetches streaming links for a finished task (Febbox's
  links expire) and also picks up any files added to the share since.
  **Pause queue** stops any *new* queued export from starting (whatever's
  already running finishes normally); **Run now** on a queued task jumps
  it to the front, bypassing a pause for that one task specifically (it
  still respects the parallel-export limit, though -- it just becomes
  first in line for the next free slot). Check any number of rows (or
  the header checkbox, which selects everything the current
  filter/type-filter match, not just the current page) to run
  Cancel/Resume/Resync over all of them at once -- each only applies to
  the selected rows actually eligible for it.
- **Movies** / **Series**: every title through its whole lifecycle, not
  just what's finished -- **Discovered** (Analyze found a confident
  match but nobody's queued it), **Queued**/**Exporting** (a live export
  task), **Error** (an export that failed on its own), and **Ready**
  (finished, scanned straight off disk: title, year, file count). There's
  no per-page search box anymore -- the header search bar covers Movies
  and Series both, from anywhere in the app (see "Using it" above). A
  Discovered row's **Queue** button turns
  it into a real export task (now it'll show up here as
  Queued/Exporting *and* in Activity); a Queued/Exporting row can
  **Cancel** (same soft stop as Activity -- lands back as Discovered,
  not deleted). **Remove** is this page's one hard-delete action, and the
  only one that deletes anything: it clears any exported files *and*
  whatever queue/library record exists, together -- no separate Delete
  button -- available on any row short of Exporting (cancel it first).
  Ready rows also get **View** (poster/overview pulled from TMDB live if
  you have a key set, the real season/episode breakdown for a series or
  the file list for a movie, a Remove button per file, and **Refresh
  metadata** re-pulling the TMDB lookup); Remove on a Ready row deletes
  the files and also clears that title's finished task record from
  Activity, since Activity has no remove action of its own to do it
  there. Same bulk-select as Activity: check rows (or the header
  checkbox for everything currently filtered) and run
  Queue/Cancel/Remove over all of them at once.

  The table itself is fully customizable. **Columns** opens a picker
  with every available field (Status, Title, Year, Files/Episodes,
  Category, Progress, Current file, On disk, Path) -- only a small
  default set is on to start, check/uncheck any of the rest to show or
  hide it. Drag a column header sideways to reorder it, drag the handle
  on its right edge to resize it, and click a header to sort by that
  field -- click it again to reverse the sort. Column choice, order,
  width, and sort all persist per page (localStorage), independently for
  Movies and Series.

  There's a **Grid** view too (the toggle next to Columns) -- poster
  cards instead of table rows, same underlying selection/search/
  category-filter/bulk-actions, with its own compact sort control
  (Columns doesn't apply in Grid, so it's hidden there) in place of
  clicking a header. Posters come from TMDB, loaded lazily -- a card
  only looks one up once it's actually scrolled into view, cached
  client-side after that -- so opening Grid on a large library doesn't
  fire a lookup for everything at once. No TMDB key, or no match: the
  card just shows its title as text instead of a poster. View choice
  persists per page, same as everything else here.

- **Links** keeps a permanent record of every Febbox share link ever
  submitted for Analyze -- pasting one in the header search bar (or
  Rechecking one here) saves/refreshes its row automatically, nothing to
  do by hand. Each row shows when it was added and when it was last
  actually checked; **Recheck** re-runs Analyze on it, exactly like
  pasting it again -- any files/titles added to the share since land in
  Movies/Series the same way a first-time paste would, with live
  progress showing in Activity while it runs. **Remove** only drops the
  saved link itself, never anything already in your library.

## Minimum release year

Some shares organize their root as bare-year folders ("2015", "2021",
...) with the actual titles nested one level inside. Set **Minimum
release year** in Settings and any such folder older than it gets
skipped outright during Analyze -- not even listed, let alone walked --
saving time and avoiding queuing content you don't want. 0 (the
default) disables the check. A folder like `Movie (2015)` is untouched
either way -- only a folder that's *nothing but* a bare year triggers
this.

## Resumable & crash-friendly by design

Every task persists a per-file item list (status, streaming link quality,
last-synced time) after every change. If the server process dies or is
restarted while a task is mid-export, that task is automatically
requeued (not failed) on the next startup and continues from exactly
where it left off -- files already written are never re-fetched. `.strm`
writes are atomic, so a crash mid-write never leaves a corrupt file
behind.

## Keeping links fresh (sync)

Febbox's streaming URLs expire, so a finished export can go stale even
though the `.strm` files are still sitting there. Two ways to refresh
them:

- Click **Resync** on a finished task any time.
- Set **Resync finished exports when the stream is _N_ days old** in
  Settings (0 disables it, which is the default) to have Febarr check
  each finished task's own last-sync age and resync it automatically
  once it crosses that threshold, no need to babysit it.

Either way, discovery re-runs too, so newly added episodes/files in the
share get exported alongside the refresh.

## Auto-classification (TV / Movies / Anime / Anime Movies)

Add a TMDB API key in Settings (free at
[themoviedb.org/settings/api](https://www.themoviedb.org/settings/api))
and the **Analyze** step will look each detected title up on TMDB to
decide Movie vs TV, and flag it as Anime (or Anime Movie) using TMDB's
Animation genre + Japan origin/language as the signal -- the same
heuristic tools like Sonarr/Radarr/Jellyfin's community use. Exports land
in `<export root>/<Category>/<Title> (<Year>)`.

Without a key (or if a title has no TMDB match), Febarr falls back to
filename heuristics: episode markers (`S01E01`, absolute numbering like
`- 01`, etc.) mean TV, otherwise Movie; a leading fansub-style `[Group]`
tag nudges it toward Anime. A heuristic-only guess never gets added to
the library -- no TMDB match means the title shows up **Rejected** in
Activity instead (see "Using it" above).

**Movie vs TV is cross-checked against the share's actual shape, not
just the title match.** TMDB's title search can match the wrong entry --
a same-named movie when the share is really a series, or vice versa --
so Analyze also looks at what's actually inside each folder: a season
subfolder (Season 1, S01, ...) means TV no matter what TMDB says; a
single video file means Movie; more than one leans TV. When that
disagrees with TMDB's answer, the structural read wins.

**Same-titled shows are told apart by episode name, not just
popularity.** If a plain title search turns up more than one TV result
sharing the exact same title (a remake, a show sold under the same name
in a different country), Febarr fetches each candidate's real season-1
episode list from TMDB and matches it against the episode names parsed
out of the share's actual filenames (`Show.S01E01.Pilot.mkv` ->
`"Pilot"`) -- whichever candidate's episodes actually match wins, even
if it's far less popular than the alternative.

## FlareSolverr (optional but often required)

Febbox sits behind Cloudflare. If you hit blocks, run FlareSolverr and
point the settings panel's FlareSolverr URL at it (default
`http://localhost:8191/v1`):

```bash
docker run -d -p 8191:8191 --name flaresolverr ghcr.io/flaresolverr/flaresolverr:latest
```

Running Febarr itself via Docker Compose? `docker compose --profile
flaresolverr up -d` starts both together instead -- see the Docker
section above.

## Data & persistence

All state (settings + task queue/history + discovered/not-yet-queued
library items + saved Links) lives in `data/state.json`. Deleting that
file resets Febarr to defaults.

## Project layout

```
app.py                 Flask app entrypoint (routes, CLI args)
febarr/core.py          Febbox scraping/export engine (discovery + export phases)
febarr/grouping.py      Splits a share's root listing into title groups (streaming generator)
febarr/classify.py      Title/year parsing + TMDB lookup + episode-name disambiguation + heuristic fallback
febarr/analyzer.py      Runs Analyze as a background job; streams confident matches straight to the library
febarr/discovered.py    Titles Analyze found but nobody's queued yet -- the library's "not exported" state
febarr/links.py         Permanent record of every share link submitted for Analyze -- the Links page
febarr/tasks.py         Task queue + dispatcher (dynamic concurrency, cancellation, resync scheduler)
febarr/media_library.py Movies/Series data: disk scan + discovered/task overlay, season/episode detail, delete
febarr/state.py         JSON-file persistence for settings, tasks & discovered items
templates/index.html    Web UI
templates/login.html    Login page (only reachable/relevant once an account is set)
static/app.js           Web UI logic (polling, forms, analyze flow, media library)
static/style.css        Web UI styling
data/state.json          Runtime state (gitignored)
```
