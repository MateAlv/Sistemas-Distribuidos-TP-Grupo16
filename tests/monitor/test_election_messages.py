import pytest

from monitor.election import ElectionMessage, ElectionMessageType


@pytest.mark.parametrize("message_type", list(ElectionMessageType))
def test_election_message_round_trip(
    message_type: ElectionMessageType,
) -> None:
    message = ElectionMessage(
        message_type=message_type,
        epoch=42,
        sender_id=3,
    )

    recovered = ElectionMessage.deserialize(message.serialize())

    assert recovered == message
    assert len(message.serialize()) == 4


def test_election_message_uses_network_byte_order() -> None:
    message = ElectionMessage(
        message_type=ElectionMessageType.COORDINATOR,
        epoch=0x1234,
        sender_id=0x56,
    )

    assert message.serialize() == b"\x02\x12\x34\x56"


@pytest.mark.parametrize("epoch", [-1, 2**16])
def test_rejects_epoch_outside_uint16(epoch: int) -> None:
    with pytest.raises(ValueError, match="epoch must be in range"):
        ElectionMessage(ElectionMessageType.ELECTION, epoch, sender_id=1)


@pytest.mark.parametrize(
    "sender_id",
    [-1, 2**8],
)
def test_rejects_sender_id_outside_uint8(sender_id: int) -> None:
    with pytest.raises(ValueError, match="sender_id must be in range"):
        ElectionMessage(ElectionMessageType.ELECTION, epoch=0, sender_id=sender_id)


@pytest.mark.parametrize("data", [b"", b"\x00", b"\x00\x00\x00", b"\x00" * 5])
def test_rejects_invalid_frame_size(data: bytes) -> None:
    with pytest.raises(ValueError, match="must be exactly 4 bytes"):
        ElectionMessage.deserialize(data)


def test_rejects_unknown_message_type() -> None:
    with pytest.raises(ValueError, match="unknown election message type"):
        ElectionMessage.deserialize(b"\xff\x00\x00\x01")
