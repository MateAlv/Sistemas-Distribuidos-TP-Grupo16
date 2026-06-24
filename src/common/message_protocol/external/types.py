# Client → gateway message types
HANDSHAKE = 1
FILE_CHUNK = 2
FINISH = 3
ACK = 4

# Gateway → file_ingestor message types
MSG_CHUNK = 1
MSG_EOF = 2
MSG_ABORT = 3
FILE_INGESTOR_ROUTING_KEY_PREFIX = "file_ingestor"

# FileChunk file types
FILE_TYPE_TRANSACTIONS = 1
FILE_TYPE_ACCOUNTS = 2
FILE_TYPE_NAMES = {
    FILE_TYPE_TRANSACTIONS: "transactions",
    FILE_TYPE_ACCOUNTS: "accounts",
}

# file_ingestor/results → gateway message types
RES_RESULT = 1
RES_EOF = 2


def file_ingestor_routing_key(partition: int) -> str:
    return f"{FILE_INGESTOR_ROUTING_KEY_PREFIX}.{int(partition)}"


def file_type_name(file_type: int) -> str:
    return FILE_TYPE_NAMES.get(int(file_type), f"unknown({file_type})")
