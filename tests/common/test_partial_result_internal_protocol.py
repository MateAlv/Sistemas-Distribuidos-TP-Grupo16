from common.domain.partial_result import Q2BankMaxPartial, Q3PaymentFormatPartial
from common.message_protocol.common import MessageType
from common.message_protocol.internal import InternalProtocol
from common.message_protocol.partial_result_serializer import (
    Q2BankMaxPartialSerializer,
    Q3PaymentFormatPartialSerializer,
)


def test_q2_partial_can_travel_as_internal_data_packet():
    client_id = 123
    partial = Q2BankMaxPartial(
        bank_id="001120",
        from_account="8006AA910",
        amount=592571.0,
    )
    payload = Q2BankMaxPartialSerializer.serialize(partial)

    packet = InternalProtocol.create_packet(
        msg_type=MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=payload,
    )

    msg_type, recovered_client_id, recovered_payload = (
        InternalProtocol.unpack_packet(packet)
    )

    assert msg_type == MessageType.DATA
    assert recovered_client_id == client_id
    assert Q2BankMaxPartialSerializer.deserialize(recovered_payload) == partial


def test_q3_partial_can_travel_as_internal_data_packet():
    client_id = 456
    partial = Q3PaymentFormatPartial(
        payment_format="Credit Card",
        amount_sum=15.75,
        count=3,
    )
    payload = Q3PaymentFormatPartialSerializer.serialize(partial)

    packet = InternalProtocol.create_packet(
        msg_type=MessageType.DATA,
        client_id_bytes=client_id.to_bytes(16, byteorder="big"),
        payload=payload,
    )

    msg_type, recovered_client_id, recovered_payload = (
        InternalProtocol.unpack_packet(packet)
    )

    assert msg_type == MessageType.DATA
    assert recovered_client_id == client_id
    assert Q3PaymentFormatPartialSerializer.deserialize(recovered_payload) == partial
