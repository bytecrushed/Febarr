# Febarr -- self-hosted Febbox export/download manager.
#
# Single stage: the app has exactly one third-party runtime dependency
# (Flask, everything else is stdlib) so there's nothing worth a
# multi-stage build over. gunicorn is installed here rather than added
# to requirements.txt so a plain `pip install -r requirements.txt` for
# local (non-Docker) dev on any platform, including Windows, never pulls
# in a Unix-only package.
#
# --workers 1 is not a tuning knob -- it's required. The task queue,
# discovered-items list, analyze jobs, and saved links all live in a
# single process's memory (see febarr/tasks.py, discovered.py,
# analyzer.py, links.py), backed by one state.json on disk. A second
# worker process would run its own independent copy of all of that,
# silently diverging from the first the moment either one touches
# anything -- so don't raise it. --threads instead gives real
# concurrency for the actual bottleneck (waiting on Febbox/TMDB/
# FlareSolverr HTTP calls), which threads handle fine since it's I/O-bound.
FROM python:3.12-slim

WORKDIR /app

# System deps: none beyond what python:slim already has -- FlareSolverr
# (if you need it) runs as its own container, see docker-compose.yml.

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn==23.0.0

COPY app.py .
COPY febarr/ febarr/
COPY templates/ templates/
COPY static/ static/

# Where the two things worth persisting across a container recreate
# live -- state.json (settings/queue/library/links) and, by default,
# exported files themselves (override with a bind mount or a different
# EXPORT_ROOT setting to put those somewhere else, e.g. a NAS share).
# See febarr/state.py -- FEBARR_DATA_DIR/FEBARR_DEFAULT_EXPORT_ROOT are
# read there, not baked into settings, so they only ever apply on a
# genuinely first run (no state.json yet); once it exists, whatever's
# saved in it always wins.
ENV FEBARR_DATA_DIR=/data
ENV FEBARR_DEFAULT_EXPORT_ROOT=/exports
RUN mkdir -p /data /exports

# Runs as a non-root user -- nothing here needs root, and the app writes
# only inside /data and /exports (both owned below), never system paths.
RUN useradd --create-home --uid 1000 febarr \
    && chown -R febarr:febarr /app /data /exports
USER febarr

EXPOSE 5000

# /healthz is intentionally unauthenticated (see app.py's require_login)
# so this works the same whether or not you've set an account password.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request as u; u.urlopen('http://127.0.0.1:5000/healthz', timeout=3)" || exit 1

# One process, many threads -- see the --workers note above. Long
# --timeout because a handful of routes (Analyze, a season's worth of
# TMDB episode lookups) can legitimately take a while, especially behind
# FlareSolverr; gunicorn killing and restarting the one worker mid-request
# would drop whatever it was doing.
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:5000", "--workers", "1", "--threads", "8", "--timeout", "300"]
