import struct
from common.domain.transaction import Transaction


class TransactionSerializer: 
    DATE_SIZE = 19
    BANK_SIZE = 16
    ACCOUNT_SIZE = 32
    CURRENCY_SIZE = 32
    FORMAT_SIZE = 32
    FORMAT = (
        f"!{DATE_SIZE}s{BANK_SIZE}s{ACCOUNT_SIZE}s"
        f"{BANK_SIZE}s{ACCOUNT_SIZE}s"
        f"d{CURRENCY_SIZE}s{FORMAT_SIZE}sQ"
    )
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, tx: Transaction) -> bytes:
        return struct.pack(
            cls.FORMAT,
            cls._encode_fixed(tx.date, cls.DATE_SIZE, "date"),
            cls._encode_fixed(tx.from_bank, cls.BANK_SIZE, "from_bank"),
            cls._encode_fixed(tx.from_account, cls.ACCOUNT_SIZE, "from_account"),
            cls._encode_fixed(tx.to_bank, cls.BANK_SIZE, "to_bank"),
            cls._encode_fixed(tx.to_account, cls.ACCOUNT_SIZE, "to_account"),
            float(tx.amount),
            cls._encode_fixed(tx.currency, cls.CURRENCY_SIZE, "currency"),
            cls._encode_fixed(tx.format, cls.FORMAT_SIZE, "format"),
            int(getattr(tx, "row_number", 0) or 0),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Transaction:
        vals = struct.unpack(cls.FORMAT, data)
        return Transaction(
            date=cls._decode_fixed(vals[0]),
            from_bank=cls._decode_fixed(vals[1]),
            from_account=cls._decode_fixed(vals[2]),
            to_bank=cls._decode_fixed(vals[3]),
            to_account=cls._decode_fixed(vals[4]),
            amount=vals[5],
            currency=cls._decode_fixed(vals[6]),
            format=cls._decode_fixed(vals[7]),
            row_number=vals[8],
        )

    @classmethod
    def serialize_batch(cls, transactions: list[Transaction]) -> bytes:
        return b"".join([cls.serialize(tx) for tx in transactions])

        
    @classmethod
    def deserialize_batch(cls, data: bytes) -> list[Transaction]:
        return [cls.deserialize(data[i:i+cls.SIZE]) 
                for i in range(0, len(data), cls.SIZE)]

    @staticmethod
    def _encode_fixed(value, size: int, field_name: str) -> bytes:
        encoded = str(value).encode("utf-8")
        if len(encoded) > size:
            raise ValueError(
                f"{field_name} exceeds {size} bytes: {value!r}"
            )
        return encoded

    @staticmethod
    def _decode_fixed(value: bytes) -> str:
        return value.decode("utf-8").rstrip("\x00")
