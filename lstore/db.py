import os
import json
from lstore.table import Table, PageRange
from lstore.bufferpool import BufferPool
from lstore.config import BUFFERPOOL_CAPACITY


class Database():

    def __init__(self):
        self.tables = {}
        self.path = None
        self.bufferpool = BufferPool(BUFFERPOOL_CAPACITY)

    def open(self, path):
        self.path = path
        os.makedirs(path, exist_ok=True)
        self.bufferpool.db_path = path

        meta_path = os.path.join(path, 'db_meta.json')
        if not os.path.exists(meta_path):
            return

        with open(meta_path, 'r') as f:
            db_meta = json.load(f)

        for tname, tinfo in db_meta['tables'].items():
            table = Table(
                tinfo['name'],
                tinfo['num_columns'],
                tinfo['key'],
                bufferpool=self.bufferpool
            )

            table_meta_path = os.path.join(path, tname, 'table_meta.json')
            if os.path.exists(table_meta_path):
                with open(table_meta_path, 'r') as f:
                    table_meta = json.load(f)

                table.next_rid = table_meta['next_rid']

                for rid_str, loc in table_meta['page_directory'].items():
                    table.page_directory[int(rid_str)] = tuple(loc)

                table.page_ranges = []
                for i, pr_meta in enumerate(table_meta['page_ranges']):
                    pr = PageRange(
                        table.total_cols,
                        table_name=tname,
                        range_idx=i,
                        bufferpool=self.bufferpool
                    )
                    pr.num_base_records = pr_meta['num_base_records']
                    pr.num_tail_records = pr_meta['num_tail_records']
                    if 'tps' in pr_meta:
                        pr.tps = {int(k): v for k, v in pr_meta['tps'].items()}
                    table.page_ranges.append(pr)

            self.tables[tname] = table
            self._rebuild_indexes(table)

    def close(self):
        if self.path is None:
            return

        # join any active merge threads
        for table in self.tables.values():
            if table.merge_thread is not None and table.merge_thread.is_alive():
                table.merge_thread.join()

        # flush all dirty pages to disk
        self.bufferpool.flush_all()

        # write db_meta.json
        db_meta = {'tables': {}}
        for tname, table in self.tables.items():
            db_meta['tables'][tname] = {
                'name': table.name,
                'num_columns': table.num_columns,
                'key': table.key
            }

        meta_path = os.path.join(self.path, 'db_meta.json')
        with open(meta_path, 'w') as f:
            json.dump(db_meta, f)

        # write table_meta.json for each table
        for tname, table in self.tables.items():
            table_dir = os.path.join(self.path, tname)
            os.makedirs(table_dir, exist_ok=True)

            pd = {}
            for k, v in table.page_directory.items():
                pd[str(k)] = list(v)

            pr_list = []
            for pr in table.page_ranges:
                pr_list.append({
                    'num_base_records': pr.num_base_records,
                    'num_tail_records': pr.num_tail_records,
                    'tps': {str(k): v for k, v in pr.tps.items()}
                })

            table_meta = {
                'next_rid': table.next_rid,
                'page_directory': pd,
                'page_ranges': pr_list
            }

            table_meta_path = os.path.join(table_dir, 'table_meta.json')
            with open(table_meta_path, 'w') as f:
                json.dump(table_meta, f)

    def _rebuild_indexes(self, table):
        from lstore.query import Query
        q = Query(table)
        for rid, loc in table.page_directory.items():
            _, is_tail, _, _ = loc
            if is_tail:
                continue
            vals = q._get_record_values(rid)
            key_val = vals[table.key]
            table.index.insert_entry(table.key, key_val, rid)

    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            return self.tables[name]

        table = Table(name, num_columns, key_index, bufferpool=self.bufferpool)
        self.tables[name] = table
        return table

    def drop_table(self, name):
        if name in self.tables:
            del self.tables[name]
            return True
        return False

    def get_table(self, name):
        return self.tables.get(name, None)
