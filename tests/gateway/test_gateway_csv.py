from common.domain.transaction import Transaction
from common.message_protocol.internal import TransactionSerializer
from gateway.gateway import Gateway


def test_q3_csv_lines_match_notebook_columns_and_bank_id():
    payload = TransactionSerializer.serialize_batch(
        [
            Transaction(
                date="2022/09/06 00:00",
                from_bank="011495",
                from_account="8008AD900",
                to_bank="2",
                to_account="dst",
                amount=10149.61,
                currency="US Dollar",
                format="ACH",
            )
        ]
    )

    assert list(Gateway._q3_csv_lines(payload)) == [
        "11495,8008AD900,ACH,10149.61",
    ]
