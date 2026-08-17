"""
links.py
========
Permanent record of every Febbox share link ever submitted for Analyze --
backs the Links page: paste a share once and it stays here, so it can be
re-checked for new files later (a Recheck click) without having to dig
the URL back up. One row per distinct share (keyed by share_key, same
"the part of the URL that actually identifies it" extraction core.py
already does for tasks), updated in place rather than duplicated on every
re-submit -- whether that's someone pasting the same link again or a
Links-page Recheck, both go through the same /api/shares/analyze route
(see app.py), which is the one place remember() gets called.

Deliberately separate from analyzer.py's AnalyzeManager jobs -- those are
ephemeral, in-memory-only progress trackers for one run and don't survive
a restart; this is the durable "list of shares I care about" that does,
via StateStore (same read-modify-write-under-a-lock pattern as
discovered.py, for the same reason: two concurrent submits of the same
link shouldn't be able to race each other into two rows).
"""

import datetime
import threading
import uuid

from . import core


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


class SavedLinksManager:
    def __init__(self, store):
        self.store = store
        self._lock = threading.RLock()

    def list_items(self) -> list:
        return self.store.get_saved_links()

    def get(self, item_id: str):
        for item in self.store.get_saved_links():
            if item["id"] == item_id:
                return item
        return None

    def remember(self, share_link: str) -> dict:
        """Adds a new saved link, or -- if this share is already saved
        (same share_key) -- just refreshes its URL/last_submitted_at
        instead of creating a duplicate row. Called every time
        /api/shares/analyze actually starts a job."""
        share_link = (share_link or "").strip()
        share_key = core.extract_key(share_link)
        now = _now_iso()
        with self._lock:
            items = self.store.get_saved_links()
            existing = next((it for it in items if it["share_key"] == share_key), None) if share_key else None
            if existing is not None:
                existing["share_link"] = share_link  # freshest URL wins (query params/tokens can change)
                existing["last_submitted_at"] = now
                self.store.replace_saved_links(items)
                return existing
            item = {
                "id": uuid.uuid4().hex[:10],
                "share_link": share_link,
                "share_key": share_key,
                "added_at": now,
                "last_submitted_at": now,
                # Set once that submission's analyze job actually
                # finishes (see AnalyzeManager._run's finally, wired
                # through mark_checked()) -- separate from
                # last_submitted_at (when it was kicked off) so the
                # Links page can tell "still checking" from a real
                # completed check.
                "last_checked_at": None,
            }
            items.append(item)
            self.store.replace_saved_links(items)
            return item

    def mark_checked(self, share_key: str, at_iso: str = None) -> None:
        """Called when an analyze job for this share_key finishes,
        whichever way -- done, errored, or cancelled all still mean 'a
        check just happened'."""
        if not share_key:
            return
        with self._lock:
            items = self.store.get_saved_links()
            changed = False
            for it in items:
                if it["share_key"] == share_key:
                    it["last_checked_at"] = at_iso or _now_iso()
                    changed = True
            if changed:
                self.store.replace_saved_links(items)

    def remove(self, item_id: str) -> dict:
        with self._lock:
            items = self.store.get_saved_links()
            match = next((it for it in items if it["id"] == item_id), None)
            if match is None:
                raise KeyError(item_id)
            items = [it for it in items if it["id"] != item_id]
            self.store.replace_saved_links(items)
        return match
