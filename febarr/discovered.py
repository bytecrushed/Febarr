"""
discovered.py
==============
Titles Analyze found a confident TMDB match for but that haven't been
queued as a real export yet. This is the "Library, not Activity" landing
spot: analyzer.py records a match here instead of calling
TaskManager.create_task_from_proposal() directly, so a newly-found title
shows up in Movies/Series right away (see media_library.augment_with_pipeline())
without a live export task cluttering Activity until someone actually
asks for it.

A discovered item carries exactly what create_task_from_proposal() needs
-- share_link, kind, root_fid/root_items, category/title/year, TMDB
metadata -- so "queue" is just handing those same fields to TaskManager
and dropping the discovered record. Persisted through StateStore (same
pattern as tasks) so it survives a restart.

add()/remove() are each read-modify-write against StateStore's discovered
list, so they hold their own lock across the whole operation -- without
it, two concurrent calls (the Movies/Series bulk bar fires one HTTP
request per selected row, in parallel) can both read the same snapshot,
append/filter it independently, and the second write clobbers the
first's, silently losing an item. StateStore's own lock only covers each
individual get/replace call, not the read-then-write pair, so it can't
prevent that by itself.
"""

import datetime
import threading
import uuid

from .tasks import CATEGORY_LABELS, compute_target_path, year_belongs_in_folder


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


class DiscoveredManager:
    def __init__(self, store):
        self.store = store
        self._lock = threading.RLock()

    def list_items(self) -> list:
        return self.store.get_discovered()

    def get(self, item_id: str):
        for item in self.store.get_discovered():
            if item["id"] == item_id:
                return item
        return None

    def existing_target_ids(self) -> set:
        """Every Category/Title (Year) id already covered by a discovered
        item -- used by the analyzer to avoid adding the same title twice
        (a share re-pasted, or two folders that classify to one title)."""
        items = self.store.get_discovered()
        settings = self.store.get_settings()
        return {
            compute_target_path(it["category"], it["title"], it.get("year"),
                                 include_year=year_belongs_in_folder(it["category"], settings))
            for it in items
        }

    def add(
        self,
        *,
        share_link: str,
        share_key: str,
        kind: str,
        root_fid,
        root_items,
        category: str,
        title: str,
        year,
        source: str,
        confidence: str,
        tmdb_id=None,
    ) -> dict:
        if category not in CATEGORY_LABELS:
            raise ValueError(f"invalid category: {category!r}")
        item = {
            "id": uuid.uuid4().hex[:10],
            "share_link": share_link,
            "share_key": share_key,
            "kind": kind,
            "root_fid": root_fid,
            "root_items": root_items,
            "category": category,
            "title": (title or "").strip() or "Untitled",
            "year": year,
            "source": source,
            "confidence": confidence,
            "tmdb_id": tmdb_id,
            "discovered_at": _now_iso(),
        }
        with self._lock:
            items = self.store.get_discovered()
            items.append(item)
            self.store.replace_discovered(items)
        return item

    def retarget(self, old_media_id: str, new_title: str, new_year) -> None:
        """The Movies/Series detail page's "edit path" control, applied
        to a still-discovered item (nothing on disk yet). A discovered
        item's id isn't stored -- augment_with_pipeline() computes it
        fresh from title/year every time via compute_target_path() -- so
        updating those fields here *is* the rename."""
        with self._lock:
            items = self.store.get_discovered()
            settings = self.store.get_settings()
            changed = False
            for it in items:
                computed = compute_target_path(it["category"], it["title"], it.get("year"),
                                                include_year=year_belongs_in_folder(it["category"], settings))
                if computed == old_media_id:
                    it["title"] = new_title
                    it["year"] = new_year
                    changed = True
            if changed:
                self.store.replace_discovered(items)

    def remove(self, item_id: str) -> dict:
        with self._lock:
            items = self.store.get_discovered()
            match = next((it for it in items if it["id"] == item_id), None)
            if match is None:
                raise KeyError(item_id)
            items = [it for it in items if it["id"] != item_id]
            self.store.replace_discovered(items)
        return match
