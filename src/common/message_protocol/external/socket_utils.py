import socket


def ensure_socket(sock: socket.socket | None) -> None:
    if sock is None:
        raise RuntimeError("socket is not connected")


def sendall(sock: socket.socket, data: bytes) -> None:
    ensure_socket(sock)

    remaining = len(data)
    view = memoryview(data)

    while remaining > 0:
        sent = sock.send(view[len(data) - remaining:])
        if sent == 0:
            raise OSError("socket send returned 0 bytes")
        remaining -= sent


def recv_exact(sock: socket.socket, nbytes: int) -> bytes:
    ensure_socket(sock)

    remaining = nbytes
    chunks = []

    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            raise OSError(f"connection closed early; remaining={remaining}")
        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)
