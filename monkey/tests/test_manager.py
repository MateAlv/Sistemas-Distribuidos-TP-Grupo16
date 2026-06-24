import unittest
from unittest.mock import patch, MagicMock
import subprocess
import sys
import os

# Add parent directory to path to import manager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from manager import (
    MonkeyManager,
    excluded_from_env,
    included_from_env,
    DEFAULT_EXCLUDED,
)

DEMO_KILL_LIST = [
    "file_ingestor",
    "file_splitter",
    "q4_filter",
    "q4_joiner",
    "q4_sum",
    "q4_aggregator",
    "q4_deduper",
]

class TestMonkeyManager(unittest.TestCase):
    def setUp(self):
        self.manager = MonkeyManager()

    @patch('subprocess.run')
    def test_get_running_containers_success(self, mock_run):
        mock_result = MagicMock()
        mock_result.stdout = "container1\ncontainer2\n"
        mock_run.return_value = mock_result

        containers = self.manager.get_running_containers()

        self.assertEqual(containers, ["container1", "container2"])
        mock_run.assert_called_once_with(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )

    @patch('subprocess.run')
    def test_get_running_containers_failure(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, "docker ps")

        containers = self.manager.get_running_containers()

        self.assertEqual(containers, [])

    @patch('manager.MonkeyManager.get_running_containers')
    def test_get_valid_targets_excludes_protected(self, mock_get_containers):
        mock_get_containers.return_value = [
            "grupo16-filter_usd_0-1",
            "grupo16-rabbitmq-1",
            "grupo16-gateway-1",
            "grupo16-rates_service-1",
            "grupo16-client_0-1",
            "grupo16-monkey-1",
            "grupo16-q4_joiner_0-1",
        ]

        targets = self.manager.get_valid_targets()

        self.assertEqual(
            targets,
            ["grupo16-filter_usd_0-1", "grupo16-q4_joiner_0-1"],
        )

    @patch('subprocess.run')
    def test_kill_container_success(self, mock_run):
        result = self.manager.kill_container("target_container")

        self.assertEqual(result, "target_container")
        mock_run.assert_called_once_with(
            ["docker", "kill", "target_container"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    @patch('subprocess.run')
    def test_kill_container_failure(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, "docker kill")

        result = self.manager.kill_container("target_container")

        self.assertIsNone(result)


class TestMonkeyManagerKillList(unittest.TestCase):
    @patch('manager.MonkeyManager.get_running_containers')
    def test_kill_list_kills_only_listed_workers(self, mock_get_containers):
        manager = MonkeyManager(included_containers=DEMO_KILL_LIST)
        mock_get_containers.return_value = [
            # allowed
            "file_ingestor_4", "file_splitter_0",
            "q4_filter_0", "q4_joiner_0", "q4_sum_0", "q4_aggregator_4",
            "q4_deduper_0",
            # protected (must survive)
            "sum_q2_1", "sum_q3_5", "q3_barrier_0", "q2_bank_name_joiner_0",
            "filter_q5_usd_2", "filter_usd_0", "filter_q1_1", "filter_date_2",
            "filter_q5_format_0", "aggregation_q2_0", "aggregation_q3_1",
            "aggregation_q5_0", "join_q2_0", "join_q3_0", "join_q5_0",
            "gateway", "rabbitmq", "rates_service", "client_0",
            "monitor_0", "monkey",
        ]

        targets = manager.get_valid_targets()

        self.assertEqual(
            sorted(targets),
            sorted([
                "file_ingestor_4", "file_splitter_0",
                "q4_filter_0", "q4_joiner_0", "q4_sum_0", "q4_aggregator_4",
                "q4_deduper_0",
            ]),
        )

    @patch('manager.MonkeyManager.get_running_containers')
    def test_kill_list_does_not_leak_to_lookalike_services(self, mock_get_containers):
        # filter_q5_usd must not match filter_usd / filter_q5_format;
        # sum_q2 must not match aggregation_q2; q4_aggregator must not match
        # aggregation_q2.
        manager = MonkeyManager(included_containers=DEMO_KILL_LIST)
        mock_get_containers.return_value = [
            "filter_usd_0", "filter_q5_format_0", "aggregation_q2_0",
        ]
        self.assertEqual(manager.get_valid_targets(), [])

    @patch('manager.MonkeyManager.get_running_containers')
    def test_empty_kill_list_falls_back_to_exclude_only(self, mock_get_containers):
        manager = MonkeyManager()  # no kill list
        mock_get_containers.return_value = ["filter_usd_0", "rabbitmq"]
        self.assertEqual(manager.get_valid_targets(), ["filter_usd_0"])


class TestMonitorLeader(unittest.TestCase):
    @patch('manager.MonkeyManager.get_running_containers')
    def test_monitor_leader_is_highest_numbered(self, mock_get_containers):
        manager = MonkeyManager()
        mock_get_containers.return_value = [
            "monitor_1", "monitor_3", "monitor_2", "q4_sum_0", "gateway",
        ]
        self.assertEqual(manager.monitor_leader(), "monitor_3")

    @patch('manager.MonkeyManager.get_running_containers')
    def test_monitor_leader_none_when_absent(self, mock_get_containers):
        manager = MonkeyManager()
        mock_get_containers.return_value = ["q4_sum_0", "gateway"]
        self.assertIsNone(manager.monitor_leader())

    @patch('subprocess.run')
    @patch('manager.MonkeyManager.get_running_containers')
    def test_kill_monitor_leader_targets_leader(self, mock_get_containers, mock_run):
        manager = MonkeyManager(included_containers=DEMO_KILL_LIST)
        mock_get_containers.return_value = ["monitor_1", "monitor_2", "monitor_3"]
        result = manager.kill_monitor_leader()
        self.assertEqual(result, "monitor_3")
        mock_run.assert_called_once_with(
            ["docker", "kill", "monitor_3"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )


class TestIncludedFromEnv(unittest.TestCase):
    def test_empty_when_unset(self):
        self.assertEqual(included_from_env({}), [])

    def test_parses_monkey_include(self):
        result = included_from_env({"MONKEY_INCLUDE": "file_ingestor, q4_sum"})
        self.assertEqual(result, ["file_ingestor", "q4_sum"])

    def test_ignores_blank_entries(self):
        self.assertEqual(included_from_env({"MONKEY_INCLUDE": " , ,"}), [])


class TestExcludedFromEnv(unittest.TestCase):
    def test_defaults_when_unset(self):
        self.assertEqual(excluded_from_env({}), DEFAULT_EXCLUDED)

    def test_extends_with_monkey_exclude(self):
        result = excluded_from_env({"MONKEY_EXCLUDE": "metrics, dashboard"})
        self.assertEqual(result, DEFAULT_EXCLUDED + ["metrics", "dashboard"])

    def test_ignores_blank_entries(self):
        result = excluded_from_env({"MONKEY_EXCLUDE": " , ,"})
        self.assertEqual(result, DEFAULT_EXCLUDED)


if __name__ == '__main__':
    unittest.main()
