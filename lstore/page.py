from struct import pack_into, unpack_from
from lstore.config import PAGE_SIZE, RECORD_SIZE, RECORDS_PER_PAGE

class Page:
    """
    This is our lowest-level storage unit. Each page is a 4KB chunk of memory
    that we use to store 64-bit integers. Since each int is 8 bytes, we can
    fit 512 of them in one page (4096 / 8 = 512).
    """

    def __init__(self):
    '''
      # Initializes a new empty page
    '''
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
    '''
    # Checks if we still have room to write another record
    '''
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
    '''
    # Adds a new value to the page, written at the next available slot
    # Returns the index where the value was written or -1 if the page is already full
    :param value: int    # Integer to be stored 
    :return: int         # Index where the value was written
    '''
        if not self.has_capacity():
            return -1
        spot = self.num_records
        pack_into('q', self.data, spot * RECORD_SIZE, value)
        self.num_records += 1
        return spot

    def write_at(self, idx, value):
    '''
    # Writes a value at a specific index on the page, primarily for updates where we overwrite an existing code
    :param idx: int        # Index of the record to overwrite
    :return: int           # New integer value
    '''
        pack_into('q', self.data, idx * RECORD_SIZE, value)

    def read(self, idx):
    '''
    # Return the integer stored at a given index in the page
    :param idx: int        # Index of the record to read
    :return: int           # Integer stored at the given index
    '''
        return unpack_from('q', self.data, idx * RECORD_SIZE)[0]
