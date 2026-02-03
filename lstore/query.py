from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        try:
            if primary_key not in self.table.key_to_rid:
                return False # Return False if record doesn't exist or is locked due to 2PL
            rid = self.table.key_to_rid[primary_key]
            del self.table.page_directory[rid]
            del self.table.key_to_rid[primary_key]
            return True # Return True upon succesful deletion
        except:
            return False # Return False if delete fails for whatever reason
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        try:
            schema_encoding = '0' * self.table.num_columns
            # All columns should be passed a non-NULL value when inserting
            if len(columns) != self.table.num_columns or None in columns:
                return False
            primary_key = columns[self.table.key]
            if primary_key in self.table.key_to_rid:
                return False # Return False if duplicate primary key exists
            rid = self.table.rid_counter
            self.table.rid_counter += 1
            self.table.page_directory[rid] = list(columns)
            self.table.key_to_rid[primary_key] = rid
            return True # Return True upon succesful insertion
        except:
            return False # Return False if insert fails for whatever reason

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        try:
            results = []
            rid = self.table.key_to_rid[search_key]
            columns = self.table.page_directory[rid]
            projected = []
            for i in range(self.table.num_columns):
                if projected_columns_index[i] == 1:
                    projected.append(columns[i])
                else:
                    projected.append(None)
            results.append(Record(rid, search_key, projected))
            return results
        except:
            return False # Return False if record locked by TPL

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass

    
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        try:
            if primary_key not in self.table.key_to_rid:
                return False # Return False if no record exists with given key
            rid = self.table.key_to_rid[primary_key]
            record = self.table.page_directory[rid]
            for i in range(self.table.num_columns):
                if columns[i] is not None:
                    record[i] = columns[i]
            return True # Return True upon succesful update
        except:
            return False # Return False if update fails for whatever reason

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            total = 0
            found = False
            for key, rid in self.table.key_to_rid.items():
                if start_range <= key <= end_range:
                    total += self.table.page_directory[rid][aggregate_column_index]
                    found = True
            # Return False if no record exists in the given range
            if not found:
                return False
            return total
        except:
            return False

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
