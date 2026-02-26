import threading
from lstore.index import Index
from lstore.config import *


class Record:
    """
    #Creates Record object
    """
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    """
    #Creates __getitem__ function to allow objects of class Record to be accessed with an index
    """
    def __getitem__(self, idx):
        return self.columns[idx]


class PageRange:
    """
    #Creates PageRange object
    """
    def __init__(self, num_cols, table_name=None, range_idx=0, bufferpool=None):
        self.num_cols = num_cols
        self.table_name = table_name
        self.range_idx = range_idx
        self.bufferpool = bufferpool
        self.num_base_records = 0
        self.num_tail_records = 0
        self.tps = {}
        
    """
    #Creates a unique identifier (ID) for a specific page
    :param is_tail: boolean     #returns true or false depending on whether the current page is a tail page or base page
    :param page_idx: int     #which page inside the page range
    :param col_idx: int     #which column this page stores
    """
    def _page_id(self, is_tail, page_idx, col_idx):
        return (self.table_name, self.range_idx, is_tail, page_idx, col_idx)

    """
    #Returns T/F based on whether the page range still has room for more base records
    no param
    """
    def has_capacity(self):
        return self.num_base_records < RECORDS_PER_PAGE_RANGE

    """
    #Inserts one base record into storage using the buffer pool
    :param vals: list     #corresponds to column's value
    """
    def add_base_record(self, vals):
        pg = self.num_base_records // RECORDS_PER_PAGE
        slot = self.num_base_records % RECORDS_PER_PAGE
        for c in range(self.num_cols):
            pid = self._page_id(False, pg, c)
            p = self.bufferpool.get_page(pid)
            p.write_at(slot, vals[c])
            if p.num_records <= slot:
                p.num_records = slot + 1
            self.bufferpool.mark_dirty(pid)
            self.bufferpool.unpin(pid)
        self.num_base_records = self.num_base_records + 1
        return pg, slot
    
    """
    #Same thing as the function above but adds one tail record instead
    :param vals: list     #column's value
    """
    def add_tail_record(self, vals):
        pg = self.num_tail_records // RECORDS_PER_PAGE
        slot = self.num_tail_records % RECORDS_PER_PAGE
        for c in range(self.num_cols):
            pid = self._page_id(True, pg, c)
            p = self.bufferpool.get_page(pid)
            p.write_at(slot, vals[c])
            if p.num_records <= slot:
                p.num_records = slot + 1
            self.bufferpool.mark_dirty(pid)
            self.bufferpool.unpin(pid)
        self.num_tail_records = self.num_tail_records + 1
        return pg, slot

    """
    #Returns the value stored in base page, column, at slot using the buffer pool
    :param pg: int     #which base page
    :param slot: int     #which row position within that page
    :param col: int     #which column
    """
    def get_base_val(self, pg, slot, col):
        pid = self._page_id(False, pg, col)
        return self.bufferpool.read_value(pid, slot)

    """
    #Returns the value stored in tail page, column, at slot
    :param pg: int     #which base page
    :param slot: int     #which row position
    :param col: int     #which column
    """
    def get_tail_val(self, pg, slot, col):
        pid = self._page_id(True, pg, col)
        return self.bufferpool.read_value(pid, slot)

    """
    #Reads num_cols consecutive columns from a base record located at pg, slot and returns as list
    :param pg: int     #which base page
    :param slot: int     #which record position within that page
    :param start_col: int     #starting column index
    :param num_cols: int     #how many columns to read
    """
    def get_base_vals(self, pg, slot, start_col, num_cols):
        vals = []
        for i in range(num_cols):
            pid = self._page_id(False, pg, start_col + i)
            vals.append(self.bufferpool.read_value(pid, slot))
        return vals

    """
    #Reads num_cols consecutive columns from a tail record located at pg, slot and returns as list
    :param pg: int     #which base page
    :param slot: int     #which record position within that page
    :param start_col: int     #starting column index
    :param num_cols: int     #how many columns to read
    """
    def get_tail_vals(self, pg, slot, start_col, num_cols):
        vals = []
        for i in range(num_cols):
            pid = self._page_id(True, pg, start_col + i)
            vals.append(self.bufferpool.read_value(pid, slot))
        return vals

    """
    #Writes val into base page pg, column col, at position slot
    :param pg: int     #which base page
    :param slot: int     #which record position within the page
    :param col: int     #which column
    :param val: any (depending on the column)     #the value being inserted
    """
    def set_base_val(self, pg, slot, col, val):
        pid = self._page_id(False, pg, col)
        p = self.bufferpool.get_page(pid)
        p.write_at(slot, val)
        self.bufferpool.mark_dirty(pid)
        self.bufferpool.unpin(pid)


class Table:
    def __init__(self, name, num_columns, key, bufferpool=None):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_cols = num_columns + NUM_META_COLS
        self.bufferpool = bufferpool
        self.page_ranges = []
        self.page_directory = {}
        self.next_rid = 1
        self.merge_thread = None
        self.index = Index(self)

    def new_rid(self):
        r = self.next_rid
        self.next_rid = self.next_rid + 1
        return r

    def _current_range(self):
        if len(self.page_ranges) == 0:
            self.page_ranges.append(PageRange(self.total_cols, table_name=self.name, range_idx=0, bufferpool=self.bufferpool))
            return 0, self.page_ranges[0]
        last = self.page_ranges[-1]
        if not last.has_capacity():
            ri = len(self.page_ranges)
            self.page_ranges.append(PageRange(self.total_cols, table_name=self.name, range_idx=ri, bufferpool=self.bufferpool))
            return ri, self.page_ranges[-1]
        return len(self.page_ranges) - 1, last

    def merge(self, range_idx):
        try:
            pr = self.page_ranges[range_idx]
            page_dir = self.page_directory
            page_ranges = self.page_ranges
            total_cols = self.total_cols
            num_user_cols = total_cols - NUM_META_COLS
            n_base = pr.num_base_records
            n_pages = (n_base + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE
            for pg_idx in range(n_pages):
                max_tail = pr.tps.get(pg_idx, 0)
                n_slots = min(RECORDS_PER_PAGE, n_base - pg_idx * RECORDS_PER_PAGE)
                for slot in range(n_slots):
                    rid = pr.get_base_val(pg_idx, slot, RID_COLUMN)
                    if rid not in page_dir:
                        continue
                    indir = pr.get_base_val(pg_idx, slot, INDIRECTION_COLUMN)
                    if indir == NULL_RID or indir <= max_tail:
                        continue
                    if indir not in page_dir:
                        continue
                    tloc = page_dir[indir]
                    tri, _, tpg, tslot = tloc
                    tpr = page_ranges[tri]
                    vals = tpr.get_tail_vals(tpg, tslot, NUM_META_COLS, num_user_cols)
                    for i in range(num_user_cols):
                        pr.set_base_val(pg_idx, slot, NUM_META_COLS + i, vals[i])
                    max_tail = max(max_tail, indir)
                pr.tps[pg_idx] = max_tail
        except Exception:
            pass

    def maybe_trigger_merge(self, range_idx):
        pr = self.page_ranges[range_idx]
        if pr.num_tail_records < MERGE_THRESHOLD:
            return
        if self.merge_thread is not None and self.merge_thread.is_alive():
            return
        self.merge_thread = threading.Thread(target=self.merge, args=(range_idx,), daemon=True)
        self.merge_thread.start()
