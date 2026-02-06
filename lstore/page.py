from struct import pack_into, unpack_from
from lstore.config import PAGE_SIZE, RECORD_SIZE, RECORDS_PER_PAGE

class Page:
    # each page is a 4KB chunk that holds 64-bit ints
    # can fit 512 records max (4096 / 8)

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE

    # append value to the next open slot
    def write(self, value):
        if not self.has_capacity():
            return -1
        spot = self.num_records
        pack_into('q', self.data, spot * RECORD_SIZE, value)
        self.num_records += 1
        return spot

    # write at a specific index (used for updating metadata etc)
    def write_at(self, idx, value):
        pack_into('q', self.data, idx * RECORD_SIZE, value)

    # read the int stored at index
    def read(self, idx):
        return unpack_from('q', self.data, idx * RECORD_SIZE)[0]
