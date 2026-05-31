import unittest
from unittest.mock import patch, MagicMock
import subprocess
import sys
import os

# Add parent directory to path to import manager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from manager import ChaosManager, excluded_from_env, DEFAULT_EXCLUDED

class TestChaosManager(unittest.TestCase):
    def setUp(self):
        self.manager = ChaosManager()

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

    @patch('manager.ChaosManager.get_running_containers')
    def test_get_valid_targets_excludes_protected(self, mock_get_containers):
        mock_get_containers.return_value = [
            "grupo16-filter_usd_0-1",
            "grupo16-rabbitmq-1",
            "grupo16-gateway-1",
            "grupo16-rates_service-1",
            "grupo16-client_0-1",
            "grupo16-chaos_monkey-1",
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


class TestExcludedFromEnv(unittest.TestCase):
    def test_defaults_when_unset(self):
        self.assertEqual(excluded_from_env({}), DEFAULT_EXCLUDED)

    def test_extends_with_chaos_exclude(self):
        result = excluded_from_env({"CHAOS_EXCLUDE": "metrics, dashboard"})
        self.assertEqual(result, DEFAULT_EXCLUDED + ["metrics", "dashboard"])

    def test_ignores_blank_entries(self):
        result = excluded_from_env({"CHAOS_EXCLUDE": " , ,"})
        self.assertEqual(result, DEFAULT_EXCLUDED)


if __name__ == '__main__':
    unittest.main()
