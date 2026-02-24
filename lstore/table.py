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
    """
    A page range holds base pages and tail pages, routed through a BufferPool.
    Each logical "page" is one physical Page per column in columnar storage.
    """

    def __init__(self, num_cols, table_name=None, range_idx=0, bufferpool=None):
        self.num_cols = num_cols
        self.table_name = table_name
        self.range_idx = range_idx
        self.bufferpool = bufferpool
        self.num_base_records = 0
        self.num_tail_records = 0
        self.tps = {}  # base page index -> last merged tail RID

    def _page_id(self, is_tail, page_idx, col_idx):
        return (self.table_name, self.range_idx, is_tail, page_idx, col_idx)

    def has_capacity(self):
        return self.num_base_records < RECORDS_PER_PAGE_RANGE

    def add_base_record(self, vals):
        pg = self.num_base_records // RECORDS_PER_PAGE
        slot = self.num_base_records % RECORDS_PER_PAGE
        for c in range(self.num_cols):
            pid = self._page_id(False, pg, c)
            page = self.bufferpool.get_page(pid)
            page.write_at(slot, vals[c])
            if page.num_records <= slot:
                page.num_records = slot + 1
            self.bufferpool.mark_dirty(pid)
            self.bufferpool.unpin(pid)
        self.num_base_records += 1
        return pg, slot

    def add_tail_record(self, vals):
        pg = self.num_tail_records // RECORDS_PER_PAGE
        slot = self.num_tail_records % RECORDS_PER_PAGE
        for c in range(self.num_cols):
            pid = self._page_id(True, pg, c)
            page = self.bufferpool.get_page(pid)
            page.write_at(slot, vals[c])
            if page.num_records <= slot:
                page.num_records = slot + 1
            self.bufferpool.mark_dirty(pid)
            self.bufferpool.unpin(pid)
        self.num_tail_records += 1
        return pg, slot

    def get_base_val(self, pg, slot, col):
        pid = self._page_id(False, pg, col)
        page = self.bufferpool.get_page(pid)
        val = page.read(slot)
        self.bufferpool.unpin(pid)
        return val

    def get_tail_val(self, pg, slot, col):
        pid = self._page_id(True, pg, col)
        page = self.bufferpool.get_page(pid)
        val = page.read(slot)
        self.bufferpool.unpin(pid)
        return val

    def set_base_val(self, pg, slot, col, val):
        pid = self._page_id(False, pg, col)
        page = self.bufferpool.get_page(pid)
        page.write_at(slot, val)
        self.bufferpool.mark_dirty(pid)
        self.bufferpool.unpin(pid)


class Table:
    """
    Main Table class. All columns store 64-bit integers.
    4 metadata columns (indirection, rid, timestamp, schema encoding) are prepended.
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

    def new_rid(self):
        r = self.next_rid
        self.next_rid += 1
        return r

    def _current_range(self):
        if len(self.page_ranges) == 0 or not self.page_ranges[-1].has_capacity():
            ri = len(self.page_ranges)
            self.page_ranges.append(PageRange(
                self.total_cols,
                table_name=self.name,
                range_idx=ri,
                bufferpool=self.bufferpool
            ))
        return len(self.page_ranges) - 1, self.page_ranges[-1]

    def merge(self, range_idx):
        pr = self.page_ranges[range_idx]
        num_base_pages = (pr.num_base_records + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE

        for pg_idx in range(num_base_pages):
            max_tail_rid = pr.tps.get(pg_idx, 0)
            num_slots = min(RECORDS_PER_PAGE, pr.num_base_records - pg_idx * RECORDS_PER_PAGE)

            for slot in range(num_slots):
                rid = pr.get_base_val(pg_idx, slot, RID_COLUMN)
                if rid not in self.page_directory:
                    continue  # deleted

                indir = pr.get_base_val(pg_idx, slot, INDIRECTION_COLUMN)
                if indir == NULL_RID:
                    continue
                if indir <= max_tail_rid:
                    continue  # already merged

                # read latest values from the tail the base points to
                if indir not in self.page_directory:
                    continue
                tloc = self.page_directory[indir]
                tri, _, tpg, tslot = tloc
                tpr = self.page_ranges[tri]
                for col in range(NUM_META_COLS, self.total_cols):
                    val = tpr.get_tail_val(tpg, tslot, col)
                    pr.set_base_val(pg_idx, slot, col, val)

                max_tail_rid = max(max_tail_rid, indir)

            pr.tps[pg_idx] = max_tail_rid

    def maybe_trigger_merge(self, range_idx):
        pr = self.page_ranges[range_idx]
        if pr.num_tail_records >= MERGE_THRESHOLD:
            if self.merge_thread is None or not self.merge_thread.is_alive():
                self.merge_thread = threading.Thread(
                    target=self.merge,
                    args=(range_idx,),
                    daemon=True
                )
                self.merge_thread.start()
