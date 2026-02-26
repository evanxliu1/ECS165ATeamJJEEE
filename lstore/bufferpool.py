import os
from collections import OrderedDict
from lstore.page import Page, write_page_to_disk, read_page_from_disk
from lstore.config import BUFFERPOOL_CAPACITY


"""
# Manages page caching so we dont have to hit disk every time
# uses an LRU ordered dict to track whats in memory and evicts old pages when full
# dirty pages get written back to disk before eviction
"""
class BufferPool:
    def __init__(self, capacity=BUFFERPOOL_CAPACITY):
        """
        Manages in memory pages for the database
        capacity: maximum number of pages allowed in memory
        """
        self.capacity = capacity
        self.db_path = None
        self.pages = OrderedDict()    # pid -> Page, ordered by access time
        self.dirty = set()             # pids that have been modified
        self.pin_counts = {}
        self._made_dirs = set()

    # builds the filepath for a page based on its id tuple
    def _page_filepath(self, page_id):
        """
        Makes the file path for a given page ID
        page_id: id tuple which identifies where the page is in storage
        """
        a, b, tail, p, c = page_id
        seg = 'tail' if tail else 'base'
        return os.path.join(self.db_path, a, 'page_range_%d' % b, '%s_%d_%d.page' % (seg, p, c))

    # grabs a page from cache or loads it from disk, pins it so it wont get evicted
    def get_page(self, pid):
        """
        Retrieves a page from memory or loads it from disk if you can't find it
        pid: unique page identifier
        """
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

    # fast path for reads - if the pages already cached we skip pinning entirely
    def read_value(self, pid, slot):
        page = self.pages.get(pid)
        if page is not None:
            return page.read(slot)
        # not in cache so load it the normal way
        page = self.get_page(pid)
        val = page.read(slot)
        self.unpin(pid)
        return val

    def mark_dirty(self, pid):
        """
        Marks a page as modified so it will be written to disk before it gets evicted
        pid: unique page identifier
        """
        self.dirty.add(pid)

    def unpin(self, pid):
        """
        Decreases the pin count of a page to allow eviction if the page is unused
        pid: unique page identifier
        """
        if pid not in self.pin_counts:
            return
        c = self.pin_counts[pid] - 1
        self.pin_counts[pid] = c
        if c <= 0:
            del self.pin_counts[pid]

    # writes all dirty pages to disk
    def flush_all(self):
        """
        Writes all dirty pages currently in memory to disk.
        """
        to_flush = [x for x in self.dirty]
        for pid in to_flush:
            self._flush_page(pid)
        self.dirty.clear()

    # kicks out the least recently used unpinned page
    def _evict(self):
        """
        Removes the least recently used unpinned page from memory.
        """
        for pid in self.pages:
            if pid not in self.pin_counts:
                if pid in self.dirty:
                    self._flush_page(pid)
                    self.dirty.discard(pid)
                del self.pages[pid]
                return
        # if everything is pinned, increase capacity
        self.capacity = self.capacity + 1

    def _load_from_disk(self, pid):
        """
        Loads a page from disk into memory if it exists.
        pid: unique page identifier
        """
        if self.db_path is None:
            return None
        path = self._page_filepath(pid)
        try:
            return read_page_from_disk(path)
        except FileNotFoundError:
            return None

    def _flush_page(self, pid):
        """
        Writes a single page from memory to disk
        pid: unique page identifier
        """
        if self.db_path is None or pid not in self.pages:
            return None
        path = self._page_filepath(pid)
        d = os.path.dirname(path)
        if d not in self._made_dirs:
            os.makedirs(d, exist_ok=True)
            self._made_dirs.add(d)
        write_page_to_disk(self.pages[pid], path)
