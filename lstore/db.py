import os
import json
from lstore.table import Table, PageRange
from lstore.bufferpool import BufferPool
from lstore.config import BUFFERPOOL_CAPACITY


class Database:
    """
    # initializes the database object
    """
    def __init__(self):
        self.tables = {}
        self.path = None
        self.bufferpool = BufferPool(BUFFERPOOL_CAPACITY)


    """
    # opens the database from path with the table's data
    :param path: string     # the directory path to the database files
    """
    def open(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)
        self.bufferpool.db_path = path
        meta_path = os.path.join(path, 'db_meta.json')
        if not os.path.exists(meta_path):
            return None
        f = open(meta_path, 'r')
        db_meta = json.load(f)
        f.close()
        for tname, tinfo in db_meta['tables'].items():
            t = Table(tinfo['name'], tinfo['num_columns'], tinfo['key'], bufferpool=self.bufferpool)
            table_meta_path = os.path.join(path, tname, 'table_meta.json')
            if os.path.exists(table_meta_path):
                f2 = open(table_meta_path, 'r')
                table_meta = json.load(f2)
                f2.close()
                t.next_rid = table_meta['next_rid']
                for rid_str, loc in table_meta['page_directory'].items():
                    t.page_directory[int(rid_str)] = tuple(loc)
                t.page_ranges = []
                for i, pr_meta in enumerate(table_meta['page_ranges']):
                    pr = PageRange(t.total_cols, table_name=tname, range_idx=i, bufferpool=self.bufferpool)
                    pr.num_base_records = pr_meta['num_base_records']
                    pr.num_tail_records = pr_meta['num_tail_records']
                    if 'tps' in pr_meta:
                        pr.tps = {int(k): v for k, v in pr_meta['tps'].items()}
                    t.page_ranges.append(pr)
            self.tables[tname] = t
            self._rebuild_indexes(t)


    """
    # saves data and closes the database path, effectively flushing data to disk
    """
    def close(self):
        if self.path is None:
            return
        for t in self.tables.values():
            if t.merge_thread is not None and t.merge_thread.is_alive():
                t.merge_thread.join()
        self.bufferpool.flush_all()
        db_meta = {'tables': {}}
        for tname, t in self.tables.items():
            db_meta['tables'][tname] = {'name': t.name, 'num_columns': t.num_columns, 'key': t.key}
        meta_path = os.path.join(self.path, 'db_meta.json')
        f = open(meta_path, 'w')
        json.dump(db_meta, f)
        f.close()
        for tname, t in self.tables.items():
            table_dir = os.path.join(self.path, tname)
            os.makedirs(table_dir, exist_ok=True)
            pd = {}
            for k, v in t.page_directory.items():
                pd[str(k)] = list(v)
            pr_list = []
            for pr in t.page_ranges:
                pr_list.append({
                    'num_base_records': pr.num_base_records,
                    'num_tail_records': pr.num_tail_records,
                    'tps': {str(k): v for k, v in pr.tps.items()}
                })
            table_meta = {'next_rid': t.next_rid, 'page_directory': pd, 'page_ranges': pr_list}
            table_meta_path = os.path.join(table_dir, 'table_meta.json')
            f = open(table_meta_path, 'w')
            json.dump(table_meta, f)
            f.close()


    """
    # rebuilds an index using the page directory 
    :param t: Table         # the table whose index is being rebuilt
    """
    def _rebuild_indexes(self, t):
        from lstore.query import Query
        q = Query(t)
        for rid, loc in t.page_directory.items():
            (_, is_tail, _, _) = loc
            if is_tail:
                continue
            vals = q._get_record_values(rid)
            t.index.insert_entry(t.key, vals[t.key], rid)



    """
    # Creates a new table
    :param name: string             #Table name
    :param num_columns: int         #Number of Columns: all columns are integer
    :param key_index: int           #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            return self.tables[name]
        t = Table(name, num_columns, key_index, bufferpool=self.bufferpool)
        self.tables[name] = t
        return t


    """
    # Deletes the specified table
    :param name: string       # name of the table
    """
    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
            return True
        return False


    """
    # Returns table with the passed name
    :param name: string      # name of the table
    """
    def get_table(self, name):
        return self.tables.get(name, None)
