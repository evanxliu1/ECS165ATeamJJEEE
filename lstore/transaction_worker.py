from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    def add_transaction(self, t):
        self.transactions.append(t)

    def run(self):
        pass
        # create thread and call __run

    def join(self):
        pass

    def __run(self):
        for txn in self.transactions:
            self.stats.append(txn.run())
        # num that commited
        self.result = len(list(filter(lambda x: x, self.stats)))
