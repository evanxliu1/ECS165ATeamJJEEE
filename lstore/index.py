"""
Index for the table columns. We use a dictionary (hash map) for O(1) lookups.
The key column always gets an index, other columns can be added later.
"""

class Index:

    def __init__(self, table):
        # one slot per column, None = not indexed
        self.indices = [None] * table.num_columns
        # always index the primary key
        self.indices[table.key] = {}

    # find all record ids that have this value in the given column
    def locate(self, column, value):
        if self.indices[column] is None:
            return []
        return list(self.indices[column].get(value, []))

    # find all rids where column value is between begin and end
    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            return []
        result = []
        for val, rids in self.indices[column].items():
            if begin <= val <= end:
                result.extend(rids)
        return result

    # add a new entry to the index
    def insert_entry(self, column, value, rid):
        if self.indices[column] is None:
            return
        if value not in self.indices[column]:
            self.indices[column][value] = []
        self.indices[column][value].append(rid)

    # when a value changes we need to move the rid
    def update_entry(self, column, old_val, new_val, rid):
        self.delete_entry(column, old_val, rid)
        self.insert_entry(column, new_val, rid)

    # remove a rid from the index
    def delete_entry(self, column, value, rid):
        if self.indices[column] is None:
            return
        if value in self.indices[column]:
            try:
                self.indices[column][value].remove(rid)
            except ValueError:
                pass
            # clean up empty lists
            if len(self.indices[column][value]) == 0:
                del self.indices[column][value]

    def create_index(self, column_number):
        if self.indices[column_number] is None:
            self.indices[column_number] = {}

    def drop_index(self, column_number):
        self.indices[column_number] = None
