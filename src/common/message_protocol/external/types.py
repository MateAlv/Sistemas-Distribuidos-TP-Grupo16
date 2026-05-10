# Client → gateway message types
HANDSHAKE = 1
FILE_CHUNK = 2
FINISH = 3
ACK = 4

# Gateway → message_handler message types
MSG_CHUNK = 1
MSG_EOF = 2

# message_handler → gateway (results) message types
RES_RESULT = 1
RES_EOF = 2
