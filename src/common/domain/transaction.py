import functools

@functools.total_ordering
class Transaction:
    def __init__(self, date, from_bank, from_account, 
                 to_bank, to_account, amount, currency, format):
        self.date = date
        self.from_bank = from_bank
        self.from_account = from_account
        self.to_bank = to_bank
        self.to_account = to_account
        self.amount = amount
        self.currency = currency
        self.format = format
    
    def __lt__(self, other):
        if not isinstance(other, Transaction):
            return self.amount < other
        return self.amount < other.amount
    
    def __eq__(self, other):
        if not isinstance(other, Transaction):
            return self.amount == other
        return self.amount == other.amount
    
    def __str__(self):
        return f"""
        Transaction(date={self.date}, from_account={self.from_account}, 
        to_account={self.to_account}, amount={self.amount}, 
        currency={self.currency}, format={self.format})
        """
    
    def is_in_date_range(self, start_date, end_date):
        return start_date <= self.date <= end_date
    
    def get_payment_format(self):
        return self.format
    
    def get_currency(self):
        return self.currency
    
    def hash_by_payment_format(self, num_buckets):
        h = 0
        for c in self.format:
            h = (h * 31 + ord(c))
        return h % num_buckets
    