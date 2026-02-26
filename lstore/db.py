import os
import json
from lstore.table import Table, PageRange
from lstore.bufferpool import BufferPool
from lstore.config import BUFFERPOOL_CAPACITY, NUM_META_COLS


"""
# The Database class is the top level thing that manages all the tables
# handles creating/dropping tables and saving/loading everything to disk
# each database gets one shared bufferpool that all tables use
"""
class Database:
    def __init__(self):
        self.tables = {}
        self.path = None
        self.bufferpool = BufferPool(BUFFERPOOL_CAPACITY)

    # loads up a database from disk, reads the metadata json and rebuilds all the tables
    def open(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)
        self.bufferpool.db_path = path
        meta_pth = os.path.join(path, 'db_meta.json')
        if not os.path.exists(meta_pth):
            return None
        f = open(meta_pth, 'r')
        meta = json.load(f)
        f.close()
        for tn, info in meta['tables'].items():
            tbl = Table(info['name'], info['num_columns'], info['key'], bufferpool=self.bufferpool)
            tmeta_pth = os.path.join(path, tn, 'table_meta.json')
            if os.path.exists(tmeta_pth):
                fp2 = open(tmeta_pth, 'r')
                tmeta = json.load(fp2)
                fp2.close()
                tbl.next_rid = tmeta['next_rid']
                for rid_str, locn in tmeta['page_directory'].items():
                    tbl.page_directory[int(rid_str)] = tuple(locn)
                tbl.page_ranges = []
                for i, pm in enumerate(tmeta['page_ranges']):
                    prange = PageRange(tbl.total_cols, table_name=tn, range_idx=i, bufferpool=self.bufferpool)
                    prange.num_base_records = pm['num_base_records']
                    prange.num_tail_records = pm['num_tail_records']
                    if 'tps' in pm:
                        prange.tps = {int(k): v for k, v in pm['tps'].items()}
                    tbl.page_ranges.append(prange)
            self.tables[tn] = tbl
            self._rebuild_indexes(tbl)

    # saves everything to disk, flushes dirty pages and writes out all the metadata json files
    def close(self):
        if self.path is None:
            return
        for tbl in self.tables.values():
            if tbl.merge_thread is not None and tbl.merge_thread.is_alive():
                tbl.merge_thread.join()
        self.bufferpool.flush_all()
        meta = {'tables': {}}
        for tn, tbl in self.tables.items():
            meta['tables'][tn] = {'name': tbl.name, 'num_columns': tbl.num_columns, 'key': tbl.key}
        meta_pth = os.path.join(self.path, 'db_meta.json')
        f = open(meta_pth, 'w')
        json.dump(meta, f)
        f.close()
        for tn, tbl in self.tables.items():
            tdir = os.path.join(self.path, tn)
            os.makedirs(tdir, exist_ok=True)
            pdir = {}
            for k, v in tbl.page_directory.items():
                pdir[str(k)] = list(v)
            prlist = []
            for prange in tbl.page_ranges:
                prlist.append({
                    'num_base_records': prange.num_base_records,
                    'num_tail_records': prange.num_tail_records,
                    'tps': {str(k): v for k, v in prange.tps.items()}
                })
            tmeta = {'next_rid': tbl.next_rid, 'page_directory': pdir, 'page_ranges': prlist}
            tmeta_pth = os.path.join(tdir, 'table_meta.json')
            f = open(tmeta_pth, 'w')
            json.dump(tmeta, f)
            f.close()

    # rebuild inddex from page directory (only key colum needed)
    def _rebuild_indexes(self, tbl):
        kcol = tbl.key
        for rid, locn in tbl.page_directory.items():
            rng_ix, is_tail, pgnum, sl = locn
            if is_tail:
                continue
            prange = tbl.page_ranges[rng_ix]
            kv = prange.get_base_val(pgnum, sl, NUM_META_COLS + kcol)
            tbl.index.insert_entry(kcol, kv, rid)

    # creates a new table if table isn't already made
    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            return self.tables[name]
        tbl = Table(name, num_columns, key_index, bufferpool=self.bufferpool)
        self.tables[name] = tbl
        return tbl


    # removes a table
    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
            return True
        return False

    # returns the desired table
    def get_table(self, name):
        return self.tables.get(name, None)
