from common.domain.transaction import Transaction
from common.message_protocol.external.types import (
    FILE_TYPE_ACCOUNTS,
    FILE_TYPE_TRANSACTIONS,
    file_type_name,
)
from common.message_protocol.internal import LineBatch
from workers.file_ingestor.transaction_row_parser import TransactionRowParser


class LineBatchParser:
    @classmethod
    def parse(cls, batch: LineBatch) -> list[Transaction]:
        if batch.file_type == FILE_TYPE_ACCOUNTS:
            return []

        if batch.file_type != FILE_TYPE_TRANSACTIONS:
            raise ValueError(f"unknown line batch file_type: {batch.file_type}")

        parser = TransactionRowParser(batch.header)
        transactions = []
        for index, line in enumerate(batch.lines):
            try:
                transactions.append(parser.parse_line(line))
            except Exception as exc:
                line_number = batch.first_line_number + index
                raise ValueError(
                    "invalid transaction line batch "
                    f"(file_type={file_type_name(batch.file_type)}, "
                    f"path={batch.rel_path}, batch_id={batch.batch_id}, "
                    f"line_number={line_number})"
                ) from exc

        if len(transactions) != len(batch.lines):
            raise RuntimeError("line batch parser violated one-line-one-transaction")
        return transactions
