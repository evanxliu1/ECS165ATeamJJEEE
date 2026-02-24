"""
Index class using hash maps for O(1) single-value lookups.
The primary key column is always indexed. Other columns can be indexed on demand.
"""


class Index:

    def __init__(self, table):
        self.table = table
        self.indices = [None] * table.num_columns
        self.indices[table.key] = {}

    def locate(self, column, value):
        if self.indices[column] is None:
            return []
        return list(self.indices[column].get(value, []))

    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            return []
        result = []
        for val, rids in self.indices[column].items():
            if begin <= val <= end:
                result.extend(rids)
        return result

    def insert_entry(self, column, value, rid):
        if self.indices[column] is None:
            return
        if value not in self.indices[column]:
            self.indices[column][value] = []
        self.indices[column][value].append(rid)

    def update_entry(self, column, old_val, new_val, rid):
        self.delete_entry(column, old_val, rid)
        self.insert_entry(column, new_val, rid)

    def delete_entry(self, column, value, rid):
        if self.indices[column] is None:
            return
        if value in self.indices[column]:
            try:
                self.indices[column][value].remove(rid)
            except ValueError:
                pass
            if len(self.indices[column][value]) == 0:
                del self.indices[column][value]

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = {}
            self._populate_index(column_number)

    def drop_index(self, column_number):
        self.indices[column_number] = None

    def _populate_index(self, column_number):
        from lstore.query import Query
        q = Query(self.table)
        for rid, loc in self.table.page_directory.items():
            _, is_tail, _, _ = loc
            if is_tail:
                continue
            vals = q._get_record_values(rid)
            self.insert_entry(column_number, vals[column_number], rid)
