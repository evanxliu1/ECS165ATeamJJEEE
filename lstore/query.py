from lstore.table import Record
from lstore.config import *
from time import time

class Query:
    """
    # Initializes the table
    :param table: Table      # the table object that will store the data
    """
    def __init__(self, table):
        self.table = table

    """
    # Finds all base record RIDs where a column equals a value
    # if theres an index on that column we use it, otherwise we do a full scan
    :param column: int       # the number column in the database
    :param value: int        # the value we are looking for
    """
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

    """
    # Same idea as _locate but for a range of values instead of an exact match
    :param begin: int        # lower bound of the range
    :param end: int          # upper bound of the range
    :param column: int       # the number column in the database
    """
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


    """
    # Returns the column values for a record identified by base_rid, if the base record's
    # pointer points to no tails then it returns the values from the base records, otherwise
    # it follows the chain of pointers to the tail record
    :param base_rid: int     # permanent ID of a record
    :param version: int      # controls how many tail records you follow
    """
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


    """
    # Takes a primary key and deletes the record from all indexes and the page directory
    :param primary_key: int  # the main unique identifier for a record
    """
    # deletes a value using the primary key
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


    """
    # Inserts a brand new record to the table as a base record and includes where to store and what is stored
    :param *columns: tuple   # takes any number of values and shoves it all into a tuple
    """
    #inserts a value into a column if it's not already there
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


    """
    # Finds all records whose values correspond to the search_key and returns only the requested columns
    # uses _locate to find matching rids (index or scan) then applies the projection
    :param search_key: int           # value we are searching for
    :param search_key_index: int     # which column to search in
    :param projected_columns_index: list  # a list of 0s and 1s indicating which columns to return
    """
    # searches for a value using search keys, and then selects it
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

    """
    # Same as select except this function allows us to search for previous tail records
    :param search_key: int           # value we are searching for
    :param search_key_index: int     # which column to search in
    :param projected_columns_index: list  # a list of 0s and 1s indicating which columns to return
    :param relative_version: int     # how many tail records you look back
    """
    # searches for a version of a value using search keys, then selects it
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

    """
    # Finds a record with the primary key and creates a new tail record with updated values
    # then updates the base records pointer to point to this new tail record
    :param primary_key: int  # the main unique identifier for a record
    :param *columns: tuple   # whatever you want to insert into the new tail record
    """
    #updates a value using the primary key and column to find it
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

    """
    # Computes the sum of one column over all records who fall in a certain range
    :param start_range: int              # lower bound of primary key range
    :param end_range: int                # upper bound of primary key range
    :param aggregate_column_index: int   # the column index to sum
    """
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

    """
    # Same as sum but allows you to look back at tail records
    :param start_range: int              # lower bound of primary key range
    :param end_range: int                # upper bound of primary key range
    :param aggregate_column_index: int   # the column index to sum
    :param relative_version: int         # how many tail records you look back
    """
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
    """
    # Adds 1 to a single column of the record identified by the primary key
    :param key: int          # primary key value of the record you want to update
    :param column: int       # the column index to increment
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            upd_cols = [None] * self.table.num_columns
            upd_cols[column] = r[column] + 1
            u = self.update(key, *upd_cols)
            return u
        return False
