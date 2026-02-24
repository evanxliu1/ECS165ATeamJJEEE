from struct import pack, pack_into, unpack, unpack_from
from lstore.config import PAGE_SIZE, RECORD_SIZE, RECORDS_PER_PAGE

class Page:
    def __init__(self):
        self.num_records = 0
        # raw bytes for the page with fixed size from page size constant
        self.data = bytearray(PAGE_SIZE)
    def has_capacity(self):
        # checks if the page still has room for another record
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
        # here adds a new value to the end of the page and ives back the index where the value was stored or minus one if the page is full
        if not self.has_capacity():
            return -1
        pos = self.num_records
        pack_into('q', self.data, pos * RECORD_SIZE, value)
        self.num_records = pos + 1
        return pos

    def write_at(self, idx, value):
        # replaces the value at a specific record position on the page
        pack_into('q', self.data, idx * RECORD_SIZE, value)
    def read(self, idx):
        # reads the 64 bit integer stored at the given record position and returns it
        return unpack_from('q', self.data, idx * RECORD_SIZE)[0]
def write_page_to_disk(page, filepath):
    # first writes the number of records then writes the raw page bytes
    f = open(filepath, 'wb')
    f.write(pack('q', page.num_records))
    f.write(page.data)
    f.close()
def read_page_from_disk(filepath):
    # loads a page object back from disk that was saved with the helper above and reads the record count header and then the full page data bytes
    f = open(filepath, 'rb')
    header = f.read(8)
    data = f.read(PAGE_SIZE)
    f.close()
    num_records = unpack('q', header)[0]
    p = Page()
    p.num_records = num_records
    p.data = bytearray(data)
    return p
