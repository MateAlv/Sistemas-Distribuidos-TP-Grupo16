# Client → gateway message types
HANDSHAKE = 1
FILE_CHUNK = 2
FINISH = 3
ACK = 4

# Gateway → file_ingestor message types
MSG_CHUNK = 1
MSG_EOF = 2
FILE_INGESTOR_ROUTING_KEY_PREFIX = "file_ingestor"

# file_ingestor/results → gateway message types
RES_RESULT = 1
RES_EOF = 2


def file_ingestor_routing_key(partition: int) -> str:
    return f"{FILE_INGESTOR_ROUTING_KEY_PREFIX}.{int(partition)}"
