import os
from collections import OrderedDict
from lstore.page import Page, write_page_to_disk, read_page_from_disk


class BufferPool:

    def __init__(self, capacity=1000):
        self.capacity = capacity
        self.db_path = None
        self.pages = OrderedDict()  # page_id -> Page
        self.dirty = set()
        self.pin_counts = {}  # page_id -> int

    def _page_filepath(self, page_id):
        table_name, range_idx, is_tail, page_idx, col_idx = page_id
        prefix = 'tail' if is_tail else 'base'
        return os.path.join(
            self.db_path, table_name,
            f'page_range_{range_idx}',
            f'{prefix}_{page_idx}_{col_idx}.page'
        )

    def get_page(self, page_id):
        if page_id in self.pages:
            self.pages.move_to_end(page_id)
            self.pin_counts[page_id] = self.pin_counts.get(page_id, 0) + 1
            return self.pages[page_id]

        # try loading from disk
        page = self._load_from_disk(page_id)
        if page is None:
            page = Page()

        # evict if at capacity
        while len(self.pages) >= self.capacity:
            self._evict()

        self.pages[page_id] = page
        self.pages.move_to_end(page_id)
        self.pin_counts[page_id] = self.pin_counts.get(page_id, 0) + 1
        return page

    def mark_dirty(self, page_id):
        self.dirty.add(page_id)

    def unpin(self, page_id):
        if page_id in self.pin_counts:
            self.pin_counts[page_id] -= 1
            if self.pin_counts[page_id] <= 0:
                del self.pin_counts[page_id]

    def flush_all(self):
        for page_id in list(self.dirty):
            self._flush_page(page_id)
        self.dirty.clear()

    def _evict(self):
        for page_id in list(self.pages.keys()):
            if page_id not in self.pin_counts:
                if page_id in self.dirty:
                    self._flush_page(page_id)
                    self.dirty.discard(page_id)
                del self.pages[page_id]
                return
        # all pages pinned, temporarily grow
        self.capacity += 1

    def _load_from_disk(self, page_id):
        if self.db_path is None:
            return None
        filepath = self._page_filepath(page_id)
        if os.path.exists(filepath):
            return read_page_from_disk(filepath)
        return None

    def _flush_page(self, page_id):
        if self.db_path is None or page_id not in self.pages:
            return
        filepath = self._page_filepath(page_id)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        write_page_to_disk(self.pages[page_id], filepath)
