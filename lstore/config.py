# config for teh db

PAGE_SIZE = 4096   # each page is 4KB
RECORD_SIZE = 8    # 64 bit ints so 8 bytes each
RECORDS_PER_PAGE = PAGE_SIZE // RECORD_SIZE  # 512 fits in one page

# how many records befor we need a new page range
RECORDS_PER_PAGE_RANGE = RECORDS_PER_PAGE * 128   # 65536

# these are the meta colums that go before user data
# so the actual user columns start at index 4
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
NUM_META_COLS = 4

# 0 means no indirection / null pointer basicaly
NULL_RID = 0

# bufferpool capcity
BUFFERPOOL_CAPACITY = 10000

# when to triger merge
MERGE_THRESHOLD = 100000
