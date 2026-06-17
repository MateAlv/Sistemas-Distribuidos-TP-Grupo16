from common.fault_tolerance.wal.input_applied import InputApplied
from common.fault_tolerance.wal.input_done import InputDone

WalRecord = InputApplied | InputDone
