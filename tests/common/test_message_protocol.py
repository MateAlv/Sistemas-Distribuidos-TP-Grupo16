# pyrefly: ignore [missing-import]
import pytest
import uuid
from src.common.domain.transaction import Transaction
from src.common.message_protocol.transaction_serializer import TransactionSerializer
from src.common.message_protocol.internal import InternalProtocol

def test_transaction_serialization_integrity():
    tx_original = Transaction(
        date="2022-09-01",
        from_bank=123,
        from_account=456,
        to_bank=789,
        to_account=101,
        amount=50.5,
        currency="USD",
        format="Wire"
    )

    data = TransactionSerializer.serialize(tx_original)
    tx_recovered = TransactionSerializer.deserialize(data)

    assert tx_recovered.date == tx_original.date
    assert tx_recovered.amount == tx_original.amount
    assert tx_recovered.currency == tx_original.currency
    assert tx_recovered.from_bank == tx_original.from_bank

def test_batch_serialization():
    txs = [
        Transaction("2022-09-01", 1, 2, 3, 4, 10.0, "USD", "ACH"),
        Transaction("2022-09-02", 5, 6, 7, 8, 20.0, "EUR", "Wire")
    ]
    
    batch_data = TransactionSerializer.serialize_batch(txs)
    recovered_txs = TransactionSerializer.deserialize_batch(batch_data)
    
    assert len(recovered_txs) == 2
    assert recovered_txs[1].amount == 20.0

def test_internal_protocol_header():
    client_id = uuid.uuid4()
    payload = b"test_payload"
    msg_type = 1 
    
    packet = InternalProtocol.create_packet(msg_type, client_id.bytes, payload)
    res_type, res_id, res_payload = InternalProtocol.unpack_packet(packet)
    
    assert res_type == msg_type
    assert res_id == client_id.bytes
    assert res_payload == payload
