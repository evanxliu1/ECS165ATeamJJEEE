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
    #Creates __getitem__ method to allow objects of class Record to be accessed with an index
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
        pgnum = self.num_base_records // RECORDS_PER_PAGE
        sl = self.num_base_records % RECORDS_PER_PAGE
        for col_ix in range(self.num_cols):
            page_id = self._page_id(False, pgnum, col_ix)
            pg = self.bufferpool.get_page(page_id)
            pg.write_at(sl, vals[col_ix])
            if pg.num_records <= sl:
                pg.num_records = sl + 1
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)
        self.num_base_records = self.num_base_records + 1
        return pgnum, sl


    """
    #Same thing as the function above but adds one tail record instead
    :param vals: list     #column's value
    """
    def add_tail_record(self, vals):
        pgnum = self.num_tail_records // RECORDS_PER_PAGE
        sl = self.num_tail_records % RECORDS_PER_PAGE
        for col_ix in range(self.num_cols):
            page_id = self._page_id(True, pgnum, col_ix)
            pg = self.bufferpool.get_page(page_id)
            pg.write_at(sl, vals[col_ix])
            if pg.num_records <= sl:
                pg.num_records = sl + 1
            self.bufferpool.mark_dirty(page_id)
            self.bufferpool.unpin(page_id)
        self.num_tail_records = self.num_tail_records + 1
        return pgnum, sl


    """
    #Returns the value stored in base page, column, at slot using the buffer pool
    :param pg: int     #which base page
    :param slot: int     #which row position within that page
    :param col: int     #which column
    """
    def get_base_val(self, pg, slot, col):
        page_id = self._page_id(False, pg, col)
        return self.bufferpool.read_value(page_id, slot)


    """
    #Returns the value stored in tail page, column, at slot
    :param pg: int     #which base page
    :param slot: int     #which row position
    :param col: int     #which column
    """
    def get_tail_val(self, pg, slot, col):
        page_id = self._page_id(True, pg, col)
        return self.bufferpool.read_value(page_id, slot)


    """
    #Reads num_cols consecutive columns from a base record located at pg, slot and returns as list
    :param pg: int     #which base page
    :param slot: int     #which record position within that page
    :param start_col: int     #starting column index
    :param num_cols: int     #how many columns to read
    """
    def get_base_vals(self, pg, slot, start_col, num_cols):
        vlist = []
        for i in range(num_cols):
            page_id = self._page_id(False, pg, start_col + i)
            vlist.append(self.bufferpool.read_value(page_id, slot))
        return vlist


     """
    #Reads num_cols consecutive columns from a tail record located at pg, slot and returns as list
    :param pg: int     #which base page
    :param slot: int     #which record position within that page
    :param start_col: int     #starting column index
    :param num_cols: int     #how many columns to read
    """
    def get_tail_vals(self, pg, slot, start_col, num_cols):
        vlist = []
        for i in range(num_cols):
            page_id = self._page_id(True, pg, start_col + i)
            vlist.append(self.bufferpool.read_value(page_id, slot))
        return vlist


    """
    #Writes val into base page pg, column col, at position slot
    :param pg: int     #which base page
    :param slot: int     #which record position within the page
    :param col: int     #which column
    :param val: any (depending on the column)     #the value being inserted
    """
    def set_base_val(self, pg, slot, col, val):
        page_id = self._page_id(False, pg, col)
        page_obj = self.bufferpool.get_page(page_id)
        page_obj.write_at(slot, val)
        self.bufferpool.mark_dirty(page_id)
        self.bufferpool.unpin(page_id)

class Table:
    """
    #Creates Table object
    """
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

    """
    #Generates a new unique record ID every time this method is called
    """
    def new_rid(self):
        r = self.next_rid
        self.next_rid = self.next_rid + 1
        return r

    """
    #Decides which page range new records should go into
    """
    def _current_range(self):
        if len(self.page_ranges) == 0:
            self.page_ranges.append(PageRange(self.total_cols, table_name=self.name, range_idx=0, bufferpool=self.bufferpool))
            return 0, self.page_ranges[0]
        last = self.page_ranges[-1]
        if not last.has_capacity():
            rng_ix = len(self.page_ranges)
            self.page_ranges.append(PageRange(self.total_cols, table_name=self.name, range_idx=rng_ix, bufferpool=self.bufferpool))
            return rng_ix, self.page_ranges[-1]
        return len(self.page_ranges) - 1, last

    """
    #Takes updates stored in tail pages and applies the latest values back into base pages
    :param range_idx: 
    def merge(self, range_idx): int     #index of a pagerange inside self.page_ranges
    """
    def merge(self, range_idx):
        try:
            prange = self.page_ranges[range_idx]
            pdir = self.page_directory
            pranges = self.page_ranges
            total_cols = self.total_cols
            n_user_cols = total_cols - NUM_META_COLS
            nrec = prange.num_base_records
            npages = (nrec + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE
            for pg_idx in range(npages):
                mt = prange.tps.get(pg_idx, 0)
                nslots = min(RECORDS_PER_PAGE, nrec - pg_idx * RECORDS_PER_PAGE)
                for sl in range(nslots):
                    rid = prange.get_base_val(pg_idx, sl, RID_COLUMN)
                    if rid not in pdir:
                        continue
                    ind = prange.get_base_val(pg_idx, sl, INDIRECTION_COLUMN)
                    if ind == NULL_RID or ind <= mt:
                        continue
                    if ind not in pdir:
                        continue
                    tl = pdir[ind]
                    ti, _, tpg, tslot = tl
                    tp = pranges[ti]
                    vls = tp.get_tail_vals(tpg, tslot, NUM_META_COLS, n_user_cols)
                    for i in range(n_user_cols):
                        prange.set_base_val(pg_idx, sl, NUM_META_COLS + i, vls[i])
                    mt = max(mt, ind)
                prange.tps[pg_idx] = mt
        except Exception:
            pass

    """
    #Decides whether or not to start a background merge
    :param range_idx: int     #index of a page range
    """
    def maybe_trigger_merge(self, range_idx):
        prange = self.page_ranges[range_idx]
        if prange.num_tail_records < MERGE_THRESHOLD:
            return
        if self.merge_thread is not None and self.merge_thread.is_alive():
            return
        self.merge_thread = threading.Thread(target=self.merge, args=(range_idx,), daemon=True)
        self.merge_thread.start()
