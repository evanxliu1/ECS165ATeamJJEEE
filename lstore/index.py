from bisect import bisect_left, bisect_right, insort


class Index:
    """
    # Initializes the table
    :param table: Table        # the table object that will store the data
    """
    def __init__(self, table):
        self.table = table
        self.indices = [None] * table.num_columns
        self.sorted_keys = [None] * table.num_columns
        self.indices[table.key] = {}
        self.sorted_keys[table.key] = []

    
    """
    # Locates a specific value
    :param col: int      # the number column in the database
    :param val: int      # the value we are searching for
    """
    def locate(self, col, val):
        m = self.indices[col]
        if m is None:
            return []
        L = m.get(val, [])
        # returns list of RIDs
        return list(L)


    """
    # Locates a range of values
    :param begin: int         # beginning of the range
    :param end: int           # end of the range
    :param col: int           # the index of the column within indices
    """
    def locate_range(self, begin, end, col):
        m = self.indices[col]
        if m is None:
            return []
        keys = self.sorted_keys[col]
        lo = bisect_left(keys, begin)
        hi = bisect_right(keys, end)
        out = []
        for i in range(lo, hi):
            out.extend(m[keys[i]])
        return out

    
    """
    # Inserts a record into the index
    :param val: int        # the value of the record to be inserted
    :param rid: int        # the id of the record so we can trace back to it later
    :param col: int        # the number column in the database
    """
    def insert_entry(self, col, val, rid):
        if self.indices[col] is None:
            return
        m = self.indices[col]
        if val not in m:
            m[val] = []
            insort(self.sorted_keys[col], val)
        m[val].append(rid)


    """
    # Updates a preexisting record
    :param old_v: int       # the original value of the record
    :param new_v: int       # the updated value of the record
    :param rid: int         # the id of the record we are tracing back to
    :param col: int         # the number column in the database
    """
    def update_entry(self, col, old_v, new_v, rid):
        # removes the old value reference
        self.delete_entry(col, old_v, rid)
        # inserts the new value reference
        self.insert_entry(col, new_v, rid)
    # Updates a preexisting record

    
    """
    # Deletes a preexisting record
    :param val: int        # the value whose RID we are trying to delete from the index
    :param rid: int        # the id of the record we are tracing back to
    :param col: int        # the number column in the database
    """
    def delete_entry(self, col, val, rid):
        m = self.indices[col]
        if m is None:
            return
        if val not in m:
            return
        try:
            m[val].remove(rid)
        except ValueError:
            pass
        if len(m[val]) == 0:
            del m[val]
            keys = self.sorted_keys[col]
            idx = bisect_left(keys, val)
            if idx < len(keys) and keys[idx] == val:
                keys.pop(idx)


    """
    # Creates a brand new index
    :param col_num: int      # the column number of the index  
    """
    def create_index(self, col_num):
        if self.indices[col_num] is not None:
            return
        self.indices[col_num] = {}
        self.sorted_keys[col_num] = []
        self._populate_index(col_num)

    
    """
    # Removes the index of a certain column
    :param col_num: int      # the column number of the index  
    """
    # turn off indexing for a column
    def drop_index(self, col_num):
        self.indices[col_num] = None
        self.sorted_keys[col_num] = None

    
    """
    # Adds a table into the Index
    :param col_num: int      # the column number of the index  
    """
    def _populate_index(self, col_num):
        from lstore.query import Query
        q = Query(self.table)
        for rid, loc in self.table.page_directory.items():
            (_, is_tail, _, _) = loc
            if is_tail:
                continue
            vals = q._get_record_values(rid)
            self.insert_entry(col_num, vals[col_num], rid)
