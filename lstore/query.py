from lstore.table import Record
from lstore.config import *
from time import time


class Query:

    def __init__(self, table):
        self.table = table

    def _locate(self, column, value):
        """Locate RIDs by column value, falling back to full scan if no index."""
        if self.table.index.indices[column] is not None:
            return self.table.index.locate(column, value)
        # full scan over all base records
        rids = []
        for rid, loc in self.table.page_directory.items():
            _, is_tail, _, _ = loc
            if is_tail:
                continue
            vals = self._get_record_values(rid)
            if vals[column] == value:
                rids.append(rid)
        return rids

    def _locate_range(self, begin, end, column):
        """Locate RIDs in a key range, falling back to full scan if no index."""
        if self.table.index.indices[column] is not None:
            return self.table.index.locate_range(begin, end, column)
        rids = []
        for rid, loc in self.table.page_directory.items():
            _, is_tail, _, _ = loc
            if is_tail:
                continue
            vals = self._get_record_values(rid)
            if begin <= vals[column] <= end:
                rids.append(rid)
        return rids

    def _get_record_values(self, base_rid, version=0):
        loc = self.table.page_directory[base_rid]
        ri, _, pg, slot = loc
        pr = self.table.page_ranges[ri]

        indir = pr.get_base_val(pg, slot, INDIRECTION_COLUMN)
        if indir == NULL_RID:
            vals = []
            for i in range(self.table.num_columns):
                vals.append(pr.get_base_val(pg, slot, NUM_META_COLS + i))
            return vals

        cur = indir
        steps = abs(version)
        for _ in range(steps):
            tloc = self.table.page_directory[cur]
            tri, _, tpg, tslot = tloc
            tpr = self.table.page_ranges[tri]
            prev = tpr.get_tail_val(tpg, tslot, INDIRECTION_COLUMN)
            if prev == NULL_RID:
                vals = []
                for i in range(self.table.num_columns):
                    vals.append(pr.get_base_val(pg, slot, NUM_META_COLS + i))
                return vals
            cur = prev

        tloc = self.table.page_directory[cur]
        tri, _, tpg, tslot = tloc
        tpr = self.table.page_ranges[tri]
        vals = []
        for i in range(self.table.num_columns):
            vals.append(tpr.get_tail_val(tpg, tslot, NUM_META_COLS + i))
        return vals

    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            rid = rids[0]

            # get current values to remove from all active indexes
            vals = self._get_record_values(rid)
            for i in range(self.table.num_columns):
                if self.table.index.indices[i] is not None:
                    self.table.index.delete_entry(i, vals[i], rid)

            if rid in self.table.page_directory:
                del self.table.page_directory[rid]
            return True
        except:
            return False

    def insert(self, *columns):
        try:
            if len(columns) != self.table.num_columns:
                return False
            if None in columns:
                return False

            key_val = columns[self.table.key]
            if self.table.index.locate(self.table.key, key_val):
                return False

            rid = self.table.new_rid()
            ri, pr = self.table._current_range()

            row = [0] * self.table.total_cols
            row[INDIRECTION_COLUMN] = NULL_RID
            row[RID_COLUMN] = rid
            row[TIMESTAMP_COLUMN] = int(time())
            row[SCHEMA_ENCODING_COLUMN] = 0
            for i in range(self.table.num_columns):
                row[NUM_META_COLS + i] = columns[i]

            pg, slot = pr.add_base_record(row)
            self.table.page_directory[rid] = (ri, False, pg, slot)

            # update all active indexes
            for i in range(self.table.num_columns):
                if self.table.index.indices[i] is not None:
                    self.table.index.insert_entry(i, columns[i], rid)

            return True
        except:
            return False

    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            rids = self._locate(search_key_index, search_key)
            if not rids:
                return []

            results = []
            for rid in rids:
                if rid not in self.table.page_directory:
                    continue

                all_vals = self._get_record_values(rid)
                cols = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        cols.append(all_vals[i])
                    else:
                        cols.append(None)

                results.append(Record(rid, search_key, cols))
            return results
        except:
            return False

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            rids = self._locate(search_key_index, search_key)
            if not rids:
                return []

            results = []
            for rid in rids:
                if rid not in self.table.page_directory:
                    continue

                all_vals = self._get_record_values(rid, relative_version)

                cols = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        cols.append(all_vals[i])
                    else:
                        cols.append(None)

                results.append(Record(rid, search_key, cols))
            return results
        except:
            return False

    def update(self, primary_key, *columns):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            base_rid = rids[0]
            if base_rid not in self.table.page_directory:
                return False

            ri, _, pg, slot = self.table.page_directory[base_rid]
            pr = self.table.page_ranges[ri]

            if columns[self.table.key] is not None:
                new_pk = columns[self.table.key]
                if new_pk != primary_key:
                    existing = self.table.index.locate(self.table.key, new_pk)
                    if existing:
                        return False

            old_indir = pr.get_base_val(pg, slot, INDIRECTION_COLUMN)
            cur_vals = self._get_record_values(base_rid)

            new_vals = list(cur_vals)
            schema = 0
            for i in range(self.table.num_columns):
                if columns[i] is not None:
                    new_vals[i] = columns[i]
                    schema |= (1 << i)

            tail_rid = self.table.new_rid()
            tail_row = [0] * self.table.total_cols
            tail_row[INDIRECTION_COLUMN] = old_indir
            tail_row[RID_COLUMN] = tail_rid
            tail_row[TIMESTAMP_COLUMN] = int(time())
            tail_row[SCHEMA_ENCODING_COLUMN] = schema
            for i in range(self.table.num_columns):
                tail_row[NUM_META_COLS + i] = new_vals[i]

            tpg, tslot = pr.add_tail_record(tail_row)
            self.table.page_directory[tail_rid] = (ri, True, tpg, tslot)

            pr.set_base_val(pg, slot, INDIRECTION_COLUMN, tail_rid)
            old_schema = pr.get_base_val(pg, slot, SCHEMA_ENCODING_COLUMN)
            pr.set_base_val(pg, slot, SCHEMA_ENCODING_COLUMN, old_schema | schema)

            # update all active indexes for changed columns
            for i in range(self.table.num_columns):
                if columns[i] is not None and self.table.index.indices[i] is not None:
                    if cur_vals[i] != new_vals[i]:
                        self.table.index.update_entry(i, cur_vals[i], new_vals[i], base_rid)

            self.table.maybe_trigger_merge(ri)
            return True
        except:
            return False

    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            rids = self._locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False

            total = 0
            for rid in rids:
                if rid not in self.table.page_directory:
                    continue
                vals = self._get_record_values(rid)
                total += vals[aggregate_column_index]
            return total
        except:
            return False

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            rids = self._locate_range(start_range, end_range, self.table.key)
            if not rids:
                return False

            total = 0
            for rid in rids:
                if rid not in self.table.page_directory:
                    continue
                vals = self._get_record_values(rid, relative_version)
                total += vals[aggregate_column_index]
            return total
        except:
            return False

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
