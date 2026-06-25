from pathlib import Path

import scripts.run_full_test as runner


def test_load_client_inputs_from_compose_short_syntax(tmp_path, monkeypatch):
    compose = tmp_path / "docker-compose.test.yaml"
    compose.write_text(
        """
services:
  rabbitmq: {}
  client_0:
    environment:
      - CLIENT_ID=4
      - DATA_DIR=/data/input
      - TRANSACTIONS_FILE=LI-Mini_Trans.csv
    volumes:
      - ./data/datasets/LI-Mini:/data/input:ro
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(runner, "COMPOSE_FILE", str(compose))

    assert runner.load_client_inputs() == [
        runner.ClientInput(
            client_id=4,
            dataset_dir=(tmp_path / "data/datasets/LI-Mini").resolve(),
            trans_name="LI-Mini_Trans.csv",
        )
    ]


def test_load_client_inputs_from_compose_dict_syntax_with_nested_file(tmp_path, monkeypatch):
    compose = tmp_path / "docker-compose.test.yaml"
    compose.write_text(
        """
services:
  client_9:
    environment:
      CLIENT_ID: 9
      DATA_DIR: /input
      TRANSACTIONS_FILE: LI-Small/LI-Small_Trans.csv
    volumes:
      - type: bind
        source: ./data/datasets/client-1
        target: /input
""",
        encoding="utf-8",
    )

    monkeypatch.setattr(runner, "COMPOSE_FILE", str(compose))

    assert runner.load_client_inputs() == [
        runner.ClientInput(
            client_id=9,
            dataset_dir=(tmp_path / "data/datasets/client-1/LI-Small").resolve(),
            trans_name="LI-Small_Trans.csv",
        )
    ]
