from lstore.table import Table, Record
from lstore.index import Index

class Transaction:
    def __init__(self):
        self.queries = []
        pass

    # add a query to this transactoin
    def add_query(self, query, table, *args):
        self.queries.append((query, args))

    # retruns True if commit, False on abort
    def run(self):
        for query, args in self.queries:
            res = query(*args)
            if res == False:
                return self.abort()
        return self.commit()

    def abort(self):
        # TODO: roll-back and wahtever else
        return False

    def commit(self):
        # TODO: commit to db
        return True
