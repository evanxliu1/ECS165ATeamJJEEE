from lstore.table import Record
from lstore.config import *
from time import time

class Query:
    def __init__(self, table):
        self.table = table

    # find all base record RIDs where colum equals value, use inddex or full scan
    def _locate(self, column, value):
        if self.table.index.indices[column] is not None:
            return self.table.index.locate(column, value)
        rid_list = []
        for rid, locn in self.table.page_directory.items():
            _, tail_flag, _, _ = locn
            if tail_flag:
                continue
            vls = self._get_record_values(rid)
            if vls[column] == value:
                rid_list.append(rid)
        return rid_list

    # same but for a range of values
    def _locate_range(self, begin, end, column):
        if self.table.index.indices[column] is not None:
            return self.table.index.locate_range(begin, end, column)
        rid_list = []
        for rid, locn in self.table.page_directory.items():
            _, tail_flag, _, _ = locn
            if tail_flag:
                continue
            vls = self._get_record_values(rid)
            if begin <= vls[column] <= end:
                rid_list.append(rid)
        return rid_list

    # get colum values for a record, follows tail chain if needed
    def _get_record_values(self, base_rid, version=0):
        locn = self.table.page_directory[base_rid]
        rng_ix, _, pgnum, sl = locn
        prange = self.table.page_ranges[rng_ix]

        ind = prange.get_base_val(pgnum, sl, INDIRECTION_COLUMN)
        if ind == NULL_RID:
            return prange.get_base_vals(pgnum, sl, NUM_META_COLS, self.table.num_columns)

        if version == 0:
            tps_v = prange.tps.get(pgnum, 0)
            if ind <= tps_v:
                return prange.get_base_vals(pgnum, sl, NUM_META_COLS, self.table.num_columns)

        curr = ind
        nsteps = abs(version)
        for _ in range(nsteps):
            tl = self.table.page_directory[curr]
            ti, _, tpg, tslot = tl
            tp = self.table.page_ranges[ti]
            prev_ind = tp.get_tail_val(tpg, tslot, INDIRECTION_COLUMN)
            if prev_ind == NULL_RID:
                return prange.get_base_vals(pgnum, sl, NUM_META_COLS, self.table.num_columns)
            curr = prev_ind

        tl = self.table.page_directory[curr]
        ti, _, tpg, tslot = tl
        tp = self.table.page_ranges[ti]
        return tp.get_tail_vals(tpg, tslot, NUM_META_COLS, self.table.num_columns)

    def delete(self, primary_key):
        try:
            rid_list = self.table.index.locate(self.table.key, primary_key)
            if not rid_list:
                return False
            rid = rid_list[0]
            vls = self._get_record_values(rid)
            for i in range(self.table.num_columns):
                if self.table.index.indices[i] is not None:
                    self.table.index.delete_entry(i, vls[i], rid)
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

            kv = columns[self.table.key]
            if self.table.index.locate(self.table.key, kv):
                return False

            rid = self.table.new_rid()
            rng_ix, prange = self.table._current_range()

            row = [0] * self.table.total_cols
            row[INDIRECTION_COLUMN] = NULL_RID
            row[RID_COLUMN] = rid
            row[TIMESTAMP_COLUMN] = int(time())
            row[SCHEMA_ENCODING_COLUMN] = 0
            for i in range(self.table.num_columns):
                row[NUM_META_COLS + i] = columns[i]

            pgnum, sl = prange.add_base_record(row)
            self.table.page_directory[rid] = (rng_ix, False, pgnum, sl)

            for i in range(self.table.num_columns):
                if self.table.index.indices[i] is not None:
                    self.table.index.insert_entry(i, columns[i], rid)

            return True
        except:
            return False

    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            rid_list = self._locate(search_key_index, search_key)
            if not rid_list:
                return []

            res = []
            for rid in rid_list:
                if rid not in self.table.page_directory:
                    continue

                avals = self._get_record_values(rid)
                out_cols = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        out_cols.append(avals[i])
                    else:
                        out_cols.append(None)

                res.append(Record(rid, search_key, out_cols))
            return res
        except:
            return False

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            rid_list = self._locate(search_key_index, search_key)
            if not rid_list:
                return []

            res = []
            for rid in rid_list:
                if rid not in self.table.page_directory:
                    continue

                avals = self._get_record_values(rid, relative_version)

                out_cols = []
                for i in range(self.table.num_columns):
                    if projected_columns_index[i] == 1:
                        out_cols.append(avals[i])
                    else:
                        out_cols.append(None)

                res.append(Record(rid, search_key, out_cols))
            return res
        except:
            return False

    def update(self, primary_key, *columns):
        try:
            rid_list = self.table.index.locate(self.table.key, primary_key)
            if not rid_list:
                return False

            br = rid_list[0]
            if br not in self.table.page_directory:
                return False

            rng_ix, _, pgnum, sl = self.table.page_directory[br]
            prange = self.table.page_ranges[rng_ix]

            if columns[self.table.key] is not None:
                new_pk = columns[self.table.key]
                if new_pk != primary_key:
                    return False

            old_ind = prange.get_base_val(pgnum, sl, INDIRECTION_COLUMN)
            if old_ind == NULL_RID:
                cur_vals = prange.get_base_vals(pgnum, sl, NUM_META_COLS, self.table.num_columns)
            else:
                tps_v = prange.tps.get(pgnum, 0)
                if old_ind <= tps_v:
                    cur_vals = prange.get_base_vals(pgnum, sl, NUM_META_COLS, self.table.num_columns)
                else:
                    tl = self.table.page_directory[old_ind]
                    ti, _, tpg, tslot = tl
                    tp = self.table.page_ranges[ti]
                    cur_vals = tp.get_tail_vals(tpg, tslot, NUM_META_COLS, self.table.num_columns)

            new_vals = list(cur_vals)
            schema = 0
            for i in range(self.table.num_columns):
                if columns[i] is not None:
                    new_vals[i] = columns[i]
                    schema |= (1 << i)

            tail_rid = self.table.new_rid()
            tail_row = [0] * self.table.total_cols
            tail_row[INDIRECTION_COLUMN] = old_ind
            tail_row[RID_COLUMN] = tail_rid
            tail_row[TIMESTAMP_COLUMN] = int(time())
            tail_row[SCHEMA_ENCODING_COLUMN] = schema
            for i in range(self.table.num_columns):
                tail_row[NUM_META_COLS + i] = new_vals[i]

            tpg, tslot = prange.add_tail_record(tail_row)
            self.table.page_directory[tail_rid] = (rng_ix, True, tpg, tslot)

            prange.set_base_val(pgnum, sl, INDIRECTION_COLUMN, tail_rid)
            old_schema = prange.get_base_val(pgnum, sl, SCHEMA_ENCODING_COLUMN)
            prange.set_base_val(pgnum, sl, SCHEMA_ENCODING_COLUMN, old_schema | schema)

            for i in range(self.table.num_columns):
                if columns[i] is not None and self.table.index.indices[i] is not None:
                    if cur_vals[i] != new_vals[i]:
                        self.table.index.update_entry(i, cur_vals[i], new_vals[i], br)

            self.table.maybe_trigger_merge(rng_ix)
            return True
        except:
            return False

    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            rid_list = self._locate_range(start_range, end_range, self.table.key)
            if not rid_list:
                return False

            tot = 0
            for rid in rid_list:
                if rid not in self.table.page_directory:
                    continue
                vls = self._get_record_values(rid)
                tot = tot + vls[aggregate_column_index]
            return tot
        except:
            return False

    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            rid_list = self._locate_range(start_range, end_range, self.table.key)
            if not rid_list:
                return False

            tot = 0
            for rid in rid_list:
                if rid not in self.table.page_directory:
                    continue
                vls = self._get_record_values(rid, relative_version)
                tot = tot + vls[aggregate_column_index]
            return tot
        except:
            return False

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            upd_cols = [None] * self.table.num_columns
            upd_cols[column] = r[column] + 1
            u = self.update(key, *upd_cols)
            return u
        return False
