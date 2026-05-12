# pyrefly: ignore [missing-import]
import pytest
import csv
import sys
from pathlib import Path
from common.domain.transaction import Transaction
from common.message_protocol.transaction_serializer import TransactionSerializer
from common.message_protocol.internal import InternalProtocol



class TestQ1FlowIntegration:
    DATASET_DIR = Path("data/datasets/client-1/LI-Mini")
    TRANSACTIONS_FILE = DATASET_DIR / "LI-Mini_Trans.csv"
    ACCOUNTS_FILE = DATASET_DIR / "LI-Mini_accounts.csv"
    
    Q1_MAX_AMOUNT = 50
    USD_CURRENCY = "US Dollar"

    def test_q1_dataset_exists(self):
        """Verifica que el dataset Q1 existe"""
        assert self.DATASET_DIR.exists(), f"Dataset directory not found: {self.DATASET_DIR}"
        assert self.TRANSACTIONS_FILE.exists(), f"Transactions file not found: {self.TRANSACTIONS_FILE}"
        assert self.ACCOUNTS_FILE.exists(), f"Accounts file not found: {self.ACCOUNTS_FILE}"