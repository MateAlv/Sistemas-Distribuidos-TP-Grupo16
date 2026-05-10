import struct
from src.common.domain.transaction import Transaction

class TransactionSerializer: 
    FORMAT = "!10s Q Q Q Q d 3s 10s"
    SIZE = struct.calcsize(FORMAT)
    
    @classmethod
    def serialize(cls, tx: Transaction) -> bytes:
        return struct.pack(
            cls.FORMAT,
            tx.date.encode('utf-8'),
            int(tx.from_bank),
            int(tx.from_account),
            int(tx.to_bank),
            int(tx.to_account),
            float(tx.amount),
            tx.currency.encode('utf-8'),
            tx.format.encode('utf-8')
        )
    
    @classmethod
    def deserialize(cls, data: bytes) -> Transaction:
        vals = struct.unpack(cls.FORMAT, data)
        return Transaction(
            date=vals[0].decode('utf-8').strip('\x00'),
            from_bank=vals[1],
            from_account=vals[2],
            to_bank=vals[3],
            to_account=vals[4],
            amount=vals[5],
            currency=vals[6].decode('utf-8').strip('\x00'),
            format=vals[7].decode('utf-8').strip('\x00')
        )

    @classmethod
    def serialize_batch(cls, transactions: list[Transaction]) -> bytes:
        return b"".join([cls.serialize(tx) for tx in transactions])

        
    @classmethod
    def deserialize_batch(cls, data: bytes) -> list[Transaction]:
        return [cls.deserialize(data[i:i+cls.SIZE]) 
                for i in range(0, len(data), cls.SIZE)]
