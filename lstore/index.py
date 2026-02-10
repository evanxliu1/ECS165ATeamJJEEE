"""
So this is our Index class. Basically like the index found in a textbook. 
Instead of flipping through every page to find something, we use a hash map, so lookups are basically instant, O(1).
The key column always gets an index by default, but we can index other columns too if we want faster searches on them.
"""

class Index:
    """
    # Initializes the table
    :param table: Table        # the table object that will store the data
    """
    def __init__(self, table):
        # we make one slot per column, and None means "not indexed yet"
        self.indices = [None] * table.num_columns
        # the primary key column always gets indexed right away
        self.indices[table.key] = {}


    """
    # Locates a specific value
    :param column: int     # the number column in the database
    :param value: int      # the value we are searching for
    """
    # if we dont have an index on that column, we just return an empty list
    def locate(self, column, value):
        if self.indices[column] is None:
            return []
        # returns list of RIDs
        return list(self.indices[column].get(value, []))

    
    """
    # Locates a range of values
    :param begin: int         # beginning of the range
    :param end: int           # end of the range
    :param column: int        # the index of the column within indices
    """
    def locate_range(self, begin, end, column):
        if self.indices[column] is None:
            return []
        result = []
        for val, rids in self.indices[column].items():
            if begin <= val <= end:
                # returns list of RIDs
                result.extend(rids)
        return result


    """
    # Inserts a record into the index
    :param value: int         # the value of the record to be inserted
    :param rid: int           # the id of the record so we can trace back to it later
    :param column: int        # the number column in the database
    """
    # when we insert a new record, we call this to add its rid  to the right bucket in the index. if the column isnt indexed we skip it!
    def insert_entry(self, column, value, rid):
        if self.indices[column] is None:
            return
        if value not in self.indices[column]:
            self.indices[column][value] = []
        self.indices[column][value].append(rid)

    
    """
    # Updates a preexisting record
    :param old_val: int       # the original value of the record
    :param new_val: int       # the updated value of the record
    :param rid: int           # the id of the record we are tracing back to
    :param column: int        # the number column in the database
    """
    def update_entry(self, column, old_val, new_val, rid):
        # removes the old value reference
        self.delete_entry(column, old_val, rid)
        # inserts the new value reference
        self.insert_entry(column, new_val, rid)
    # Updates a preexisting record

    
    """
    # Deletes a preexisting record
    :param value: int         # the value whose RID we are trying to delete from the index
    :param rid: int           # the id of the record we are tracing back to
    :param column: int        # the number column in the database
    """
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


    """
    # Creates a brand new index
    :param column_number: int   # the column number of the index  
    """
    def create_index(self, column_number):
        # if there's no index with that column number, make a brand new one
        if self.indices[column_number] is None: 
            self.indices[column_number] = {}


    """
    # Removes the index of a certain column
    :param column_number: int   # the column number of the index  
    """
    # turn off indexing for a column
    def drop_index(self, column_number):
        self.indices[column_number] = None
