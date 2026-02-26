import os
from collections import OrderedDict
from lstore.page import Page, write_page_to_disk, read_page_from_disk
from lstore.config import BUFFERPOOL_CAPACITY

"""
# manages page caching so we dont have to hit disk every time
# uses LRU ordered dict, evicts old pages when full
# dirty pages get writen back befor eviction
"""
class BufferPool:
    def __init__(self, capacity=BUFFERPOOL_CAPACITY):
        self.capacity = capacity
        self.db_path = None
        self.pages = OrderedDict()    # pid -> Page
        self.dirty = set()
        self.pin_counts = {}
        self._made_dirs = set()

    # builds filepath for a page from its id tuple
    def _page_filepath(self, page_id):
        tn, rn, is_tail, pn, cn = page_id
        seg = 'tail' if is_tail else 'base'
        return os.path.join(self.db_path, tn, 'page_range_%d' % rn, '%s_%d_%d.page' % (seg, pn, cn))

    # grab page from cache or load from disk, pin it
    def get_page(self, pid):
        if pid in self.pages:
            self.pages.move_to_end(pid)
            n = self.pin_counts.get(pid, 0)
            n = n + 1
            self.pin_counts[pid] = n
            return self.pages[pid]

        pg = self._load_from_disk(pid)
        if pg is None:
            pg = Page()

        while len(self.pages) >= self.capacity:
            self._evict()

        self.pages[pid] = pg
        self.pages.move_to_end(pid)
        n = self.pin_counts.get(pid, 0) + 1
        self.pin_counts[pid] = n
        return pg

    # fast path for reads - if already cached we skip pinning
    def read_value(self, pid, slot):
        pg = self.pages.get(pid)
        if pg is not None:
            return pg.read(slot)
        pg = self.get_page(pid)
        v = pg.read(slot)
        self.unpin(pid)
        return v

    # marks a page as dirty...
    def mark_dirty(self, pid):
        self.dirty.add(pid)

    # removes a page from the pinned pages
    def unpin(self, pid):
        if pid not in self.pin_counts:
            return
        n = self.pin_counts[pid] - 1
        self.pin_counts[pid] = n
        if n <= 0:
            del self.pin_counts[pid]

    # writes all dirty pages to disk
    def flush_all(self):
        dirty_list = [x for x in self.dirty]
        for pid in dirty_list:
            self._flush_page(pid)
        self.dirty.clear()

    # kick out least recently used unpinned page
    def _evict(self):
        for pid in self.pages:
            if pid not in self.pin_counts:
                if pid in self.dirty:
                    self._flush_page(pid)
                    self.dirty.discard(pid)
                del self.pages[pid]
                return
        # everything pinned, just bump capcity
        self.capacity = self.capacity + 1

    # writes all dirty pages to disk
    def _load_from_disk(self, pid):
        if self.db_path is None:
            return None
        pth = self._page_filepath(pid)
        try:
            return read_page_from_disk(pth)
        except FileNotFoundError:
            return None

    # removes all currently open pages or "flushes" them
    def _flush_page(self, pid):
        if self.db_path is None or pid not in self.pages:
            return None
        pth = self._page_filepath(pid)
        dirpath = os.path.dirname(pth)
        if dirpath not in self._made_dirs:
            os.makedirs(dirpath, exist_ok=True)
            self._made_dirs.add(dirpath)
        write_page_to_disk(self.pages[pid], pth)
