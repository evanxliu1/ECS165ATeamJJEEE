import os
from collections import OrderedDict
from lstore.page import Page, write_page_to_disk, read_page_from_disk


class BufferPool:
    def __init__(self, capacity=1000):
        self.capacity = capacity
        self.db_path = None
        self.pages = OrderedDict()
        self.dirty = set()
        self.pin_counts = {}

    def _page_filepath(self, page_id):
        a, b, tail, p, c = page_id
        seg = 'tail' if tail else 'base'
        return os.path.join(self.db_path, a, 'page_range_%d' % b, '%s_%d_%d.page' % (seg, p, c))

    def get_page(self, pid):
        if pid in self.pages:
            self.pages.move_to_end(pid)
            cnt = self.pin_counts.get(pid, 0)
            cnt = cnt + 1
            self.pin_counts[pid] = cnt
            return self.pages[pid]

        p = self._load_from_disk(pid)
        if p is None:
            p = Page()

        while len(self.pages) >= self.capacity:
            self._evict()

        self.pages[pid] = p
        self.pages.move_to_end(pid)
        cnt = self.pin_counts.get(pid, 0) + 1
        self.pin_counts[pid] = cnt
        return p

    def mark_dirty(self, pid):
        self.dirty.add(pid)

    def unpin(self, pid):
        if pid not in self.pin_counts:
            return
        c = self.pin_counts[pid] - 1
        self.pin_counts[pid] = c
        if c <= 0:
            del self.pin_counts[pid]

    def flush_all(self):
        to_flush = [x for x in self.dirty]
        for pid in to_flush:
            self._flush_page(pid)
        self.dirty.clear()

    def _evict(self):
        for pid in list(self.pages.keys()):
            if pid in self.pin_counts:
                continue
            if pid in self.dirty:
                self._flush_page(pid)
                self.dirty.discard(pid)
            del self.pages[pid]
            return
        self.capacity = self.capacity + 1

    def _load_from_disk(self, pid):
        if self.db_path is None:
            return None
        path = self._page_filepath(pid)
        if not os.path.exists(path):
            return None
        return read_page_from_disk(path)

    def _flush_page(self, pid):
        if self.db_path is None or pid not in self.pages:
            return None
        path = self._page_filepath(pid)
        d = os.path.dirname(path)
        os.makedirs(d, exist_ok=True)
        write_page_to_disk(self.pages[pid], path)
