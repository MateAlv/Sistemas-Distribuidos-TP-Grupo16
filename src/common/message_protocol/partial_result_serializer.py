import struct

from common.constants import C_Q2, C_Q3
from common.domain.partial_result import Q2BankMaxPartial, Q3PaymentFormatPartial
from common.message_protocol.transaction_serializer import TransactionSerializer


class Q2BankMaxPartialSerializer:
    BANK_SIZE = TransactionSerializer.BANK_SIZE
    ACCOUNT_SIZE = TransactionSerializer.ACCOUNT_SIZE
    FORMAT = f"!{BANK_SIZE}s{ACCOUNT_SIZE}sd"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, partial: Q2BankMaxPartial) -> bytes:
        return struct.pack(
            cls.FORMAT,
            _encode_fixed(partial.bank_id, cls.BANK_SIZE, "bank_id"),
            _encode_fixed(partial.from_account, cls.ACCOUNT_SIZE, "from_account"),
            float(partial.amount),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Q2BankMaxPartial:
        bank_id, from_account, amount = struct.unpack(cls.FORMAT, data)
        return Q2BankMaxPartial(
            bank_id=_decode_fixed(bank_id),
            from_account=_decode_fixed(from_account),
            amount=amount,
        )


class Q3PaymentFormatPartialSerializer:
    PAYMENT_FORMAT_SIZE = TransactionSerializer.FORMAT_SIZE
    FORMAT = f"!{PAYMENT_FORMAT_SIZE}sdQ"
    SIZE = struct.calcsize(FORMAT)

    @classmethod
    def serialize(cls, partial: Q3PaymentFormatPartial) -> bytes:
        return struct.pack(
            cls.FORMAT,
            _encode_fixed(
                partial.payment_format,
                cls.PAYMENT_FORMAT_SIZE,
                "payment_format",
            ),
            float(partial.amount_sum),
            int(partial.count),
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Q3PaymentFormatPartial:
        payment_format, amount_sum, count = struct.unpack(cls.FORMAT, data)
        return Q3PaymentFormatPartial(
            payment_format=_decode_fixed(payment_format),
            amount_sum=amount_sum,
            count=count,
        )


def partial_serializer_for(configuration: str):
    if configuration == C_Q2:
        return Q2BankMaxPartialSerializer
    if configuration == C_Q3:
        return Q3PaymentFormatPartialSerializer
    raise ValueError(f"Invalid partial serializer configuration: {configuration}")


def _encode_fixed(value, size: int, field_name: str) -> bytes:
    encoded = str(value).encode("utf-8")
    if len(encoded) > size:
        raise ValueError(f"{field_name} exceeds {size} bytes: {value!r}")
    return encoded


def _decode_fixed(value: bytes) -> str:
    return value.decode("utf-8").rstrip("\x00")
