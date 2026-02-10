"""
So this is our Index class. Basicaaly like the index found in a textbook. 
Instead of flipping through every page to find something, we use a hash map so lookups are basically instant, O(1).
The key column always gets an index by default, but we can index other columns too if we want faster searches on them.
"""

class Index:

    def __init__(self, table):
        # we make one slot per column, and None means "not indexed yet"
        self.indices = [None] * table.num_columns
        # the primary key column always gets indexed right away
        self.indices[table.key] = {}

    # if we dont have an index on that column, we just return an empty list
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

    # when we insert a new record, we call this to add its rid  to the right bucket in the index. if the column isnt indexed we skip it!
    def insert_entry(self, column, value, rid):
        if self.indices[column] is None:
            return
        if value not in self.indices[column]:
            self.indices[column][value] = []
        self.indices[column][value].append(rid)

    def update_entry(self, column, old_val, new_val, rid):
        self.delete_entry(column, old_val, rid)
        self.insert_entry(column, new_val, rid)

    # if that bucket ends up empty after removal, we clean it up so we dont leave empty lists hanging around
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

    # turn off indexing for a column
    def drop_index(self, column_number):
        self.indices[column_number] = None