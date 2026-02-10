from lstore.table import Table, Record
from lstore.config import *
from time import time


class Query:
    """
    This is where all the action happens. The Query class is how we actually communicate with the table. 
    It handles insert, select, update, delete, and sum.
    If something goes wrong, we return False. If it works, we return the result or True. 
    We wrap everything in try/except so a crash never  bubbles up and it just returns False instead.
    """
    def __init__(self, table):
        """
        #Initializes the table
        :param table: dictionary
        """
        self.table = table
        
    def _get_record_values(self, base_rid, version=0):
        """
        #Returns the column values for a record which is identified by base_rid, if the base record's pointer points to
        #no tails then it returns the values from the base records, otherwise it follows the chain of pointers to the tail record
        :param base_rid: int #permanent ID of a record
        :param version: int #controls how many tail records you follow
        """
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

        # read from whatever tail we ended up at
        tloc = self.table.page_directory[cur]
        tri, _, tpg, tslot = tloc
        tpr = self.table.page_ranges[tri]
        vals = []
        for i in range(self.table.num_columns):
            vals.append(tpr.get_tail_val(tpg, tslot, NUM_META_COLS + i))
        return vals


    # deletes a record by its primary key. We look it up in the index, remove it from there and then remove it from the page directory so nobody can find it anymore.
    def delete(self, primary_key):
        """
        #Takes a primary key and deletes the record that corresponds to the primary key from the index and page directory
        :param primary_key: int #the main unique identifier for a record
        """
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            rid = rids[0]
            self.table.index.delete_entry(self.table.key, primary_key, rid)
            if rid in self.table.page_directory:
                del self.table.page_directory[rid]
            return True
        except:
            return False
    
    def insert(self, *columns):
        """
        # inserts a brand new record to the table as a base record and includes where to store and what is stored
        :param *columns: tuple #takes any number of values and shoves it all into a tuple
        """
        try:
            if len(columns) != self.table.num_columns:
                return False
            if None in columns:
                return False

            key_val = columns[self.table.key]
            # this dont allow duplicate keys
            if self.table.index.locate(self.table.key, key_val):
                return False
            rid = self.table.new_rid()
            ri, pr = self.table._current_range()
            # building the full row with metadata in front
            row = [0] * self.table.total_cols
            row[INDIRECTION_COLUMN] = NULL_RID
            row[RID_COLUMN] = rid
            row[TIMESTAMP_COLUMN] = int(time())
            row[SCHEMA_ENCODING_COLUMN] = 0
            for i in range(self.table.num_columns):
                row[NUM_META_COLS + i] = columns[i]
            pg, slot = pr.add_base_record(row)
            self.table.page_directory[rid] = (ri, False, pg, slot)
            self.table.index.insert_entry(self.table.key, key_val, rid)
            return True
        except:
            return False

    
    # we use the index to find matching rids, grab the current values for each one, and then apply the projection 
    def select(self, search_key, search_key_index, projected_columns_index):
        """
        #finds all records whose values correspond to the search_key and returns only the requested columns
        :param search_key: value we are searching for
        :param search_key_index: which column to search in
        :param projected_columns_index: out of all the columns, a list of 0s and 1s indicating which columns to return
        """
        try:
            rids = self.table.index.locate(search_key_index, search_key)
            if not rids:
                return []

            results = []
            for rid in rids:
                if rid not in self.table.page_directory:
                    continue

                all_vals = self._get_record_values(rid)
                # apply the projection (1 = include, 0 = None)
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

    
    # same idea as select, but we can ask for an older version of the record.
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        """
        #finds all records whose values correspond to the search_key and returns only the requested columns 
        #except this function allows us to search for previous tail records
        :param search_key: value we are searching for
        :param search_key_index: which column to search in
        :param projected_columns_index: out of all the columns, a list of 0s and 1s indicating which columns to return
        :param relative_version: how many tail records you look back 
        """
        try:
            rids = self.table.index.locate(search_key_index, search_key)
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

    
    # here we find the base record by primary key, read the current values, merge in whatever new values were passed
    # we also update the schema encoding bits and fix the index if the primary key itself changed
    def update(self, primary_key, *columns):
        """
        #finds a record with the primary key and creates a new tail record with updated value and then updates base records
        #pointer to point to this new tail record
        :param primary_key: the main unique identifier for a record
        :param *columns: whatever you want to insert into the new tail record
        """
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False

            base_rid = rids[0]
            if base_rid not in self.table.page_directory:
                return False

            ri, _, pg, slot = self.table.page_directory[base_rid]
            pr = self.table.page_ranges[ri]
            # reject if they're trying to change the PK to one that already exists
            if columns[self.table.key] is not None:
                new_pk = columns[self.table.key]
                if new_pk != primary_key:
                    existing = self.table.index.locate(self.table.key, new_pk)
                    if existing:
                        return False
            old_indir = pr.get_base_val(pg, slot, INDIRECTION_COLUMN)
            cur_vals = self._get_record_values(base_rid)
            # merge
            new_vals = list(cur_vals)
            schema = 0
            for i in range(self.table.num_columns):
                if columns[i] is not None:
                    new_vals[i] = columns[i]
                    schema |= (1 << i)

            tail_rid = self.table.new_rid()
            tail_row = [0] * self.table.total_cols
            tail_row[INDIRECTION_COLUMN] = old_indir   # prev tail
            tail_row[RID_COLUMN] = tail_rid
            tail_row[TIMESTAMP_COLUMN] = int(time())
            tail_row[SCHEMA_ENCODING_COLUMN] = schema
            for i in range(self.table.num_columns):
                tail_row[NUM_META_COLS + i] = new_vals[i]

            tpg, tslot = pr.add_tail_record(tail_row)
            self.table.page_directory[tail_rid] = (ri, True, tpg, tslot)
            # newtail
            pr.set_base_val(pg, slot, INDIRECTION_COLUMN, tail_rid)
            old_schema = pr.get_base_val(pg, slot, SCHEMA_ENCODING_COLUMN)
            pr.set_base_val(pg, slot, SCHEMA_ENCODING_COLUMN, old_schema | schema)
            # if they changed the primary key column we have fix the index
            if columns[self.table.key] is not None:
                if columns[self.table.key] != cur_vals[self.table.key]:
                    self.table.index.update_entry(
                        self.table.key,
                        cur_vals[self.table.key],
                        columns[self.table.key],
                        base_rid
                    )
            return True
        except:
            return False

    
    # adds up the values in one column for all records whose primary keyfalls in the given range
    def sum(self, start_range, end_range, aggregate_column_index):
        """
        #computes the sum of one column over all records who fall in a certain range
        :param start_range: lower bound of primary key range
        :param end_range: upper bound of primary key range
        :param aggregate_column_index: the column index to sum
        """
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
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

    
    # same as sum but lets you pick a specific version of each record to sum over
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """
        #computes the sum of one column over all records who fall in a certain range but allows you to look back at tail records
        :param start_range: lower bound of primary key range
        :param end_range: upper bound of primary key range
        :param aggregate_column_index: the column index to sum
        :relative_version: how many tail records you look back 
        """
        try:
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
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
        """
        #adds 1 to a single column of the record identified by the primary key
        :param key: primary key value of the record you want to update
        :param column: the column index to increment
        """
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
