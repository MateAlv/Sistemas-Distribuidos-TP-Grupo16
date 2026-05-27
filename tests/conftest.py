"""Shared pytest fixtures and fakes for the test suite.

Centralizes two things every service test needs:

1. Path setup so ``common.*`` (workers/gateway) and ``src.common.*``
   (rates_service) both import without a configured PYTHONPATH.
2. A reusable ``pika_env`` fixture that swaps in a fake ``pika`` module so the
   middleware imports and constructs without a real broker, plus fakes for the
   two flavours of SIGTERM test we run across services:

   - ``FakeChannel`` / ``FakeConnection``: drive the *real* middleware to assert
     it schedules stops thread-safely.
   - ``BlockingFakeConsumer``: stand in for a middleware endpoint so a service's
     run loop can be started and then released by a shutdown request.

As we add graceful-shutdown tests for more services, reuse ``pika_env`` rather
than re-deriving the fake-pika/reimport boilerplate per file.
"""

import importlib
import sys
import threading
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src"
for _path in (str(REPO_ROOT), str(SRC_ROOT)):
    if _path not in sys.path:
        sys.path.insert(0, _path)


_MIDDLEWARE_MODULES = (
    "common.middleware",
    "common.middleware.middleware_rabbitmq",
    "src.common.middleware",
    "src.common.middleware.middleware_rabbitmq",
)


def make_fake_pika(blocking_connection=None):
    """Build a stand-in ``pika`` module.

    ``blocking_connection`` is the connection ``pika.BlockingConnection(...)``
    yields — pass a ``FakeConnection`` to exercise the real middleware. When
    omitted, each call returns a fresh ``FakeConnection`` so middleware still
    constructs cleanly even for tests that later inject their own fake endpoint.
    """

    class AMQPConnectionError(Exception):
        pass

    class AMQPChannelError(Exception):
        pass

    class StreamLostError(Exception):
        pass

    def _blocking_connection(*_args, **_kwargs):
        return blocking_connection if blocking_connection is not None else FakeConnection()

    return types.SimpleNamespace(
        exceptions=types.SimpleNamespace(
            AMQPConnectionError=AMQPConnectionError,
            AMQPChannelError=AMQPChannelError,
            StreamLostError=StreamLostError,
        ),
        BasicProperties=lambda *args, **kwargs: types.SimpleNamespace(**kwargs),
        BlockingConnection=_blocking_connection,
        ConnectionParameters=lambda *args, **kwargs: None,
    )


class FakeChannel:
    def __init__(self):
        self.is_open = True
        self.start_consuming_calls = 0
        self.stop_consuming_calls = 0

    def queue_declare(self, *args, **kwargs):
        return types.SimpleNamespace(method=types.SimpleNamespace(queue="fake"))

    def basic_qos(self, *args, **kwargs):
        pass

    def basic_consume(self, *args, **kwargs):
        pass

    def basic_publish(self, *args, **kwargs):
        pass

    def basic_ack(self, *args, **kwargs):
        pass

    def start_consuming(self):
        self.start_consuming_calls += 1

    def stop_consuming(self):
        self.stop_consuming_calls += 1

    def close(self):
        self.is_open = False


class FakeConnection:
    def __init__(self, channel=None):
        self._channel = channel or FakeChannel()
        self.is_open = True
        self.callbacks = []

    def channel(self):
        return self._channel

    def add_callback_threadsafe(self, callback):
        # Record only; tests invoke callbacks explicitly to model the ioloop
        # draining them on the owning thread.
        self.callbacks.append(callback)

    def close(self):
        self.is_open = False


class BlockingFakeConsumer:
    """Reusable stand-in for any middleware consumer endpoint.

    Blocks in ``start`` / ``start_consuming`` until a stop is requested, so a
    service's main loop can be exercised and then released the way SIGTERM
    releases it. Works for RPC servers (``start``/``stop``) and queue/exchange
    consumers (``start_consuming``/``stop_consuming``/``request_stop_consuming``)
    alike, and records lifecycle calls for assertions.
    """

    def __init__(self, *args, block_timeout=2.0, **kwargs):

        self._block_timeout = block_timeout
        self.connected = False
        self.closed = False
        self.start_calls = 0
        self.stop_calls = 0
        self.sent = []
        self.started = threading.Event()
        self._stopped = threading.Event()

    def send(self, message, *args, **kwargs):
        self.sent.append(message)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def connect(self):
        self.connected = True

    def _block(self, *_args, **_kwargs):
        self.start_calls += 1
        self.started.set()
        if not self._stopped.wait(timeout=self._block_timeout):
            raise AssertionError("consumer was not stopped within timeout")

    start = _block
    start_consuming = _block

    def _request_stop(self, *_args, **_kwargs):
        if self._stopped.is_set():
            return
        self.stop_calls += 1
        self._stopped.set()

    stop = _request_stop
    stop_consuming = _request_stop
    request_stop_consuming = _request_stop

    def close(self):
        self.closed = True

    def wait_started(self, timeout=1.0):
        return self.started.wait(timeout)


class PikaEnv:
    """Helper handed to tests via the ``pika_env`` fixture."""

    FakeChannel = FakeChannel
    FakeConnection = FakeConnection
    BlockingFakeConsumer = BlockingFakeConsumer

    def __init__(self, monkeypatch):
        self._monkeypatch = monkeypatch
        self._imported = set()

    def install(self, connection=None):
        """Install the fake ``pika`` and drop cached middleware modules."""
        self._monkeypatch.setitem(sys.modules, "pika", make_fake_pika(connection))
        for name in _MIDDLEWARE_MODULES:
            sys.modules.pop(name, None)
            self._imported.add(name)

    def import_fresh(self, module_name, connection=None):
        """Install fake pika, then import ``module_name`` freshly against it."""
        self.install(connection)
        sys.modules.pop(module_name, None)
        self._imported.add(module_name)
        return importlib.import_module(module_name)

    def cleanup(self):
        """Drop fake-pika-built modules so the suite stays order-independent.

        ``monkeypatch`` restores ``sys.modules['pika']`` itself; we only evict
        the middleware/service modules we reimported against the fake so a later
        test rebuilds them against the real pika.
        """
        for name in self._imported:
            sys.modules.pop(name, None)


@pytest.fixture
def pika_env(monkeypatch):
    env = PikaEnv(monkeypatch)
    yield env
    env.cleanup()
