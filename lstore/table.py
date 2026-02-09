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
    A page range is basically a container that holds a group of base pages
    and their associated tail pages. In L-Store, records get split into
    base records and tail records.

    Because we use columnar storage, each "page" here is actually a list
    of Page objects, one per column. So if we have 7 total columns
    (4 metadata + 3 user), each base page set has 7 physical pages. we only allocate new ones when we actually need the space as this keeps memory usage down.
    """

    def __init__(self, num_cols):
        self.num_cols = num_cols
        self.base_pages = []  
        self.tail_pages = []
        self.num_base_records = 0
        self.num_tail_records = 0

    def has_capacity(self):
        return self.num_base_records < RECORDS_PER_PAGE_RANGE
    def _grow_base(self, need_idx):
        while len(self.base_pages) <= need_idx:
            self.base_pages.append([Page() for _ in range(self.num_cols)])

    # same idea but for tail pages
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
    # this reads a single column value from a base record at the given page and slot
    def get_base_val(self, pg, slot, col):
        return self.base_pages[pg][col].read(slot)
    def get_tail_val(self, pg, slot, col):
        return self.tail_pages[pg][col].read(slot)
    def set_base_val(self, pg, slot, col, val):
        self.base_pages[pg][col].write_at(slot, val)


class Table:
    """
    This is the main Table class. It represents one table in our database.
    All columns store 64-bit integers (no strings or floats for now).  We tack on 4 metadata columns in front
    (indirection, rid, timestamp, schema encoding), so the total column count is always num_columns + 4.
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_cols = num_columns + NUM_META_COLS

        self.page_ranges = []
        self.page_directory = {}
        self.next_rid = 1  

        self.index = Index(self)
    def new_rid(self):
        r = self.next_rid
        self.next_rid += 1
        return r
    def _current_range(self):
        if len(self.page_ranges) == 0 or not self.page_ranges[-1].has_capacity():
            self.page_ranges.append(PageRange(self.total_cols))
        return len(self.page_ranges) - 1, self.page_ranges[-1]

    def __merge(self):
        print("merge is happening")
        pass
