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

    def get_base_val(self, pg, slot, col):
        pid = self._page_id(False, pg, col)
        p = self.bufferpool.get_page(pid)
        v = p.read(slot)
        self.bufferpool.unpin(pid)
        return v

    def get_tail_val(self, pg, slot, col):
        pid = self._page_id(True, pg, col)
        p = self.bufferpool.get_page(pid)
        v = p.read(slot)
        self.bufferpool.unpin(pid)
        return v

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
        pr = self.page_ranges[range_idx]
        n_pages = (pr.num_base_records + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE
        for pg_idx in range(n_pages):
            max_tail = pr.tps.get(pg_idx, 0)
            n_slots = min(RECORDS_PER_PAGE, pr.num_base_records - pg_idx * RECORDS_PER_PAGE)
            for slot in range(n_slots):
                rid = pr.get_base_val(pg_idx, slot, RID_COLUMN)
                if rid not in self.page_directory:
                    continue
                indir = pr.get_base_val(pg_idx, slot, INDIRECTION_COLUMN)
                if indir == NULL_RID or indir <= max_tail:
                    continue
                if indir not in self.page_directory:
                    continue
                tloc = self.page_directory[indir]
                tri, _, tpg, tslot = tloc
                tpr = self.page_ranges[tri]
                for col in range(NUM_META_COLS, self.total_cols):
                    v = tpr.get_tail_val(tpg, tslot, col)
                    pr.set_base_val(pg_idx, slot, col, v)
                max_tail = max(max_tail, indir)
            pr.tps[pg_idx] = max_tail

    def maybe_trigger_merge(self, range_idx):
        pr = self.page_ranges[range_idx]
        if pr.num_tail_records < MERGE_THRESHOLD:
            return
        if self.merge_thread is not None and self.merge_thread.is_alive():
            return
        self.merge_thread = threading.Thread(target=self.merge, args=(range_idx,), daemon=True)
        self.merge_thread.start()
