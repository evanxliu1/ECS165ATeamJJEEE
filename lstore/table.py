import threading
from lstore.index import Index
from lstore.config import *

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __getitem__(self, idx):
        return self.columns[idx]

class PageRange:
    def __init__(self, num_cols, table_name=None, range_idx=0, bufferpool=None):
        self.num_cols = num_cols
        self.table_name = table_name
        self.range_idx = range_idx
        self.bufferpool = bufferpool
        self.num_base_records = 0
        self.num_tail_records = 0
        self.tps = {}

    def _page_id(self, is_tail, page_idx, col_idx):
        return (self.table_name, self.range_idx, is_tail, page_idx, col_idx)

    def has_capacity(self):
        return self.num_base_records < RECORDS_PER_PAGE_RANGE

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

    def get_base_val(self, pg, slot, col):
        page_id = self._page_id(False, pg, col)
        return self.bufferpool.read_value(page_id, slot)

    def get_tail_val(self, pg, slot, col):
        page_id = self._page_id(True, pg, col)
        return self.bufferpool.read_value(page_id, slot)

    def get_base_vals(self, pg, slot, start_col, num_cols):
        vlist = []
        for i in range(num_cols):
            page_id = self._page_id(False, pg, start_col + i)
            vlist.append(self.bufferpool.read_value(page_id, slot))
        return vlist

    def get_tail_vals(self, pg, slot, start_col, num_cols):
        vlist = []
        for i in range(num_cols):
            page_id = self._page_id(True, pg, start_col + i)
            vlist.append(self.bufferpool.read_value(page_id, slot))
        return vlist

    def set_base_val(self, pg, slot, col, val):
        page_id = self._page_id(False, pg, col)
        page_obj = self.bufferpool.get_page(page_id)
        page_obj.write_at(slot, val)
        self.bufferpool.mark_dirty(page_id)
        self.bufferpool.unpin(page_id)

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
            rng_ix = len(self.page_ranges)
            self.page_ranges.append(PageRange(self.total_cols, table_name=self.name, range_idx=rng_ix, bufferpool=self.bufferpool))
            return rng_ix, self.page_ranges[-1]
        return len(self.page_ranges) - 1, last

    # takes updates from tail pages and merges latest back inot base
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

    def maybe_trigger_merge(self, range_idx):
        prange = self.page_ranges[range_idx]
        if prange.num_tail_records < MERGE_THRESHOLD:
            return
        if self.merge_thread is not None and self.merge_thread.is_alive():
            return
        self.merge_thread = threading.Thread(target=self.merge, args=(range_idx,), daemon=True)
        self.merge_thread.start()
