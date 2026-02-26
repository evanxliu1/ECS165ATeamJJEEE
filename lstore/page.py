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
        # add value to end of page, retrun index or -1 if full
        if not self.has_capacity():
            return -1
        off = self.num_records
        pack_into('q', self.data, off * RECORD_SIZE, value)
        self.num_records = off + 1
        return off

    def write_at(self, idx, value):
        # overwrite value at that slot
        pack_into('q', self.data, idx * RECORD_SIZE, value)

    def read(self, idx):
        # reads the 64 bit integer stored at the given record position and returns it
        return unpack_from('q', self.data, idx * RECORD_SIZE)[0]

def write_page_to_disk(page, filepath):
    # first writes the number of records then writes the raw page bytes
    fp = open(filepath, 'wb')
    fp.write(pack('q', page.num_records))
    fp.write(page.data)
    fp.close()

def read_page_from_disk(filepath):
    # loads a page object back from disk that was saved with the helper above and reads the record count header and then the full page data bytes
    fp = open(filepath, 'rb')
    hdr = fp.read(8)
    dat = fp.read(PAGE_SIZE)
    fp.close()
    nr = unpack('q', hdr)[0]
    pg = Page()
    pg.num_records = nr
    pg.data = bytearray(dat)
    return pg
