from lstore.index import Index
from lstore.page import Page
from lstore.config import *


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __getitem__(self, idx):
        return self.columns[idx]


class PageRange:
    """
    holds a bunch of base pages and tail pages for a group of records.
    each "page" is actually a list of Page objects, one per column
    (because columnar storage means each column gets its own page).
    we allocate pages lazily so we dont waste memory.
    """

    def __init__(self, num_cols):
        self.num_cols = num_cols
        self.base_pages = []   # list of page sets
        self.tail_pages = []
        self.num_base_records = 0
        self.num_tail_records = 0

    def has_capacity(self):
        return self.num_base_records < RECORDS_PER_PAGE_RANGE

    # make sure we have enough base pages allocated
    def _grow_base(self, need_idx):
        while len(self.base_pages) <= need_idx:
            self.base_pages.append([Page() for _ in range(self.num_cols)])

    # same thing for tail pages
    def _grow_tail(self, need_idx):
        while len(self.tail_pages) <= need_idx:
            self.tail_pages.append([Page() for _ in range(self.num_cols)])

    def add_base_record(self, vals):
        pg = self.num_base_records // RECORDS_PER_PAGE
        slot = self.num_base_records % RECORDS_PER_PAGE
        self._grow_base(pg)
        for c in range(self.num_cols):
            self.base_pages[pg][c].write_at(slot, vals[c])
        self.num_base_records += 1
        return pg, slot

    def add_tail_record(self, vals):
        pg = self.num_tail_records // RECORDS_PER_PAGE
        slot = self.num_tail_records % RECORDS_PER_PAGE
        self._grow_tail(pg)
        for c in range(self.num_cols):
            self.tail_pages[pg][c].write_at(slot, vals[c])
        self.num_tail_records += 1
        return pg, slot

    # read one column from a base record
    def get_base_val(self, pg, slot, col):
        return self.base_pages[pg][col].read(slot)

    # read one column from a tail record
    def get_tail_val(self, pg, slot, col):
        return self.tail_pages[pg][col].read(slot)

    # overwrite one column in a base record
    def set_base_val(self, pg, slot, col, val):
        self.base_pages[pg][col].write_at(slot, val)


class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_cols = num_columns + NUM_META_COLS

        self.page_ranges = []
        # maps rid -> (range_idx, is_tail, page_idx, slot)
        self.page_directory = {}
        self.next_rid = 1   # 0 is reserved as null

        self.index = Index(self)

    def new_rid(self):
        r = self.next_rid
        self.next_rid += 1
        return r

    # get a page range that has room for a new base record
    def _current_range(self):
        if len(self.page_ranges) == 0 or not self.page_ranges[-1].has_capacity():
            self.page_ranges.append(PageRange(self.total_cols))
        return len(self.page_ranges) - 1, self.page_ranges[-1]

    def __merge(self):
        print("merge is happening")
        pass
