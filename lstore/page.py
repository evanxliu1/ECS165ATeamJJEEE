from struct import pack, pack_into, unpack, unpack_from
from lstore.config import PAGE_SIZE, RECORD_SIZE, RECORDS_PER_PAGE


class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        return self.num_records < RECORDS_PER_PAGE

    def write(self, value):
        if not self.has_capacity():
            return -1
        pos = self.num_records
        pack_into('q', self.data, pos * RECORD_SIZE, value)
        self.num_records = pos + 1
        return pos

    def write_at(self, idx, value):
        pack_into('q', self.data, idx * RECORD_SIZE, value)

    def read(self, idx):
        return unpack_from('q', self.data, idx * RECORD_SIZE)[0]


def write_page_to_disk(page, filepath):
    f = open(filepath, 'wb')
    f.write(pack('q', page.num_records))
    f.write(page.data)
    f.close()


def read_page_from_disk(filepath):
    f = open(filepath, 'rb')
    header = f.read(8)
    data = f.read(PAGE_SIZE)
    f.close()
    num_records = unpack('q', header)[0]
    p = Page()
    p.num_records = num_records
    p.data = bytearray(data)
    return p
