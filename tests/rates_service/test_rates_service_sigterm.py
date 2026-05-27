"""Graceful SIGTERM shutdown of the exchange-rate service.

Behavioral tests: they assert what the operator observes on SIGTERM — the run
loop returns, the RPC endpoint is stopped and closed, no error escapes — rather
than the service's internal wiring. The thread-safety of the stop itself lives
in tests/common/test_rabbitmq_rpc_sigterm.py.
"""

import json
import threading


def _make_service(pika_env, tmp_path):
    """Build a service with a pre-seeded rates cache and a fake RPC endpoint."""
    module = pika_env.import_fresh("src.rates_service.main")
    cache_path = tmp_path / "rates.json"
    cache_path.write_text(
        json.dumps({"2022-09-01": {"EUR": 1.0}}), encoding="utf-8"
    )
    service = module.ExchangeRateService(
        rabbit_host="rabbitmq", cache_path=str(cache_path)
    )
    server = pika_env.BlockingFakeConsumer()
    service._rpc_server = server
    return module, service, server


def test_sigterm_stops_and_closes_rpc_server(pika_env, tmp_path):
    _module, service, server = _make_service(pika_env, tmp_path)

    errors = []

    def run():
        try:
            service.run()
        except BaseException as exc:  # noqa: BLE001 - surface anything to the test
            errors.append(exc)

    runner = threading.Thread(target=run)
    runner.start()
    assert server.wait_started(timeout=1.0), "RPC server never started consuming"

    # Two requests model a duplicate signal; shutdown must stay idempotent.
    service.request_shutdown()
    service.request_shutdown()
    runner.join(timeout=1.0)

    assert not runner.is_alive(), "run() did not return after shutdown"
    assert errors == []
    assert server.connected
    assert server.closed
    assert server.stop_calls == 1


def test_shutdown_before_run_skips_blocking_consume(pika_env, tmp_path):
    _module, service, server = _make_service(pika_env, tmp_path)

    # Signal arrives before run() starts consuming (e.g. during bootstrap).
    service.request_shutdown()
    service.run()  # must not block

    assert server.start_calls == 0, "should not enter consume after shutdown"
    assert server.closed


def test_sigterm_handler_requests_service_shutdown(pika_env, monkeypatch):
    module = pika_env.import_fresh("src.rates_service.main")
    installed = {}

    class FakeService:
        def __init__(self):
            self.shutdown_calls = 0

        def request_shutdown(self):
            self.shutdown_calls += 1

    monkeypatch.setattr(
        module.signal, "signal", lambda signum, handler: installed.__setitem__(signum, handler)
    )
    service = FakeService()

    module.install_signal_handlers(service)
    installed[module.signal.SIGTERM](module.signal.SIGTERM, None)

    assert service.shutdown_calls == 1
