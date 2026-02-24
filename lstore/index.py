class Index:
    def __init__(self, table):
        self.table = table
        self.indices = [None] * table.num_columns
        self.indices[table.key] = {}

    def locate(self, col, val):
        m = self.indices[col]
        if m is None:
            return []
        L = m.get(val, [])
        return list(L)

    def locate_range(self, begin, end, col):
        m = self.indices[col]
        if m is None:
            return []
        out = []
        for v, rids in m.items():
            if begin <= v <= end:
                out.extend(rids)
        return out

    def insert_entry(self, col, val, rid):
        if self.indices[col] is None:
            return
        m = self.indices[col]
        if val not in m:
            m[val] = []
        m[val].append(rid)

    def update_entry(self, col, old_v, new_v, rid):
        self.delete_entry(col, old_v, rid)
        self.insert_entry(col, new_v, rid)

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

    def create_index(self, col_num):
        if self.indices[col_num] is not None:
            return
        self.indices[col_num] = {}
        self._populate_index(col_num)

    def drop_index(self, col_num):
        self.indices[col_num] = None

    def _populate_index(self, col_num):
        from lstore.query import Query
        q = Query(self.table)
        for rid, loc in self.table.page_directory.items():
            (_, is_tail, _, _) = loc
            if is_tail:
                continue
            vals = q._get_record_values(rid)
            self.insert_entry(col_num, vals[col_num], rid)
