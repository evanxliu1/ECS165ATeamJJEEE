from bisect import bisect_left, bisect_right, insort

class Index:
    def __init__(self, table):
        self.table = table
        self.indices = [None] * table.num_columns
        self.sorted_keys = [None] * table.num_columns
        self.indices[table.key] = {}
        self.sorted_keys[table.key] = []

    def locate(self, col, val):
        mp = self.indices[col]
        if mp is None:
            return []
        lst = mp.get(val, [])
        return list(lst)

    def locate_range(self, begin, end, col):
        mp = self.indices[col]
        if mp is None:
            return []
        klist = self.sorted_keys[col]
        low = bisect_left(klist, begin)
        high = bisect_right(klist, end)
        res = []
        for i in range(low, high):
            res.extend(mp[klist[i]])
        return res

    def insert_entry(self, col, val, rid):
        if self.indices[col] is None:
            return
        mp = self.indices[col]
        if val not in mp:
            mp[val] = []
            insort(self.sorted_keys[col], val)
        mp[val].append(rid)

    def update_entry(self, col, old_v, new_v, rid):
        self.delete_entry(col, old_v, rid)
        self.insert_entry(col, new_v, rid)

    def delete_entry(self, col, val, rid):
        mp = self.indices[col]
        if mp is None:
            return
        if val not in mp:
            return
        try:
            mp[val].remove(rid)
        except ValueError:
            pass
        if len(mp[val]) == 0:
            del mp[val]
            klist = self.sorted_keys[col]
            ix = bisect_left(klist, val)
            if ix < len(klist) and klist[ix] == val:
                klist.pop(ix)

    def create_index(self, col_num):
        if self.indices[col_num] is not None:
            return
        self.indices[col_num] = {}
        self.sorted_keys[col_num] = []
        self._populate_index(col_num)

    def drop_index(self, col_num):
        self.indices[col_num] = None
        self.sorted_keys[col_num] = None

    def _populate_index(self, col_num):
        from lstore.query import Query
        q = Query(self.table)
        for rid, locn in self.table.page_directory.items():
            (_, tail_flag, _, _) = locn
            if tail_flag:
                continue
            vls = q._get_record_values(rid)
            self.insert_entry(col_num, vls[col_num], rid)
