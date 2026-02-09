from struct import pack_into, unpack_from
from lstore.config import PAGE_SIZE, RECORD_SIZE, RECORDS_PER_PAGE

class Page:
    """
    This is our lowest-level storage unit. Each page is a 4KB chunk of memory
    that we use to store 64-bit integers. Since each int is 8 bytes, we can
    fit 512 of them in one page (4096 / 8 = 512).
    """

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    # just checks if we still have room to write another record
    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
        if not self.has_capacity():
            return -1
        spot = self.num_records
        pack_into('q', self.data, spot * RECORD_SIZE, value)
        self.num_records += 1
        return spot

    def write_at(self, idx, value):
        pack_into('q', self.data, idx * RECORD_SIZE, value)

    # here we read the 64-bit int stored at the given index
    def read(self, idx):
        return unpack_from('q', self.data, idx * RECORD_SIZE)[0]
