import unittest
from unittest.mock import patch, MagicMock
import subprocess
import sys
import os

# Add parent directory to path to import manager
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from manager import ChaosManager

class TestChaosManager(unittest.TestCase):
    def setUp(self):
        self.manager = ChaosManager()

    @patch('subprocess.run')
    def test_get_running_containers_success(self, mock_run):
        # Setup mock
        mock_result = MagicMock()
        mock_result.stdout = "container1\ncontainer2\n"
        mock_run.return_value = mock_result

        # Execute
        containers = self.manager.get_running_containers()

        # Assert
        self.assertEqual(containers, ["container1", "container2"])
        mock_run.assert_called_once_with(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            check=True
        )

    @patch('subprocess.run')
    def test_get_running_containers_failure(self, mock_run):
        # Setup mock to raise error
        mock_run.side_effect = subprocess.CalledProcessError(1, "docker ps")

        # Execute
        containers = self.manager.get_running_containers()

        # Assert
        self.assertEqual(containers, [])

    @patch('manager.ChaosManager.get_running_containers')
    def test_get_valid_targets(self, mock_get_containers):
        # Setup mock
        mock_get_containers.return_value = [
            "valid_worker",
            "rabbitmq",          # Should be excluded
            "chaos_monkey",      # Should be excluded
            "client_1",          # Should be excluded
            "another_worker"
        ]

        # Execute
        targets = self.manager.get_valid_targets()

        # Assert
        expected_targets = ["valid_worker", "another_worker"]
        self.assertEqual(targets, expected_targets)

    @patch('subprocess.run')
    def test_kill_container_success(self, mock_run):
        # Execute
        result = self.manager.kill_container("target_container")

        # Assert
        self.assertEqual(result, "target_container")
        mock_run.assert_called_once_with(
            ["docker", "kill", "target_container"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    @patch('subprocess.run')
    def test_kill_container_failure(self, mock_run):
        # Setup mock to raise error
        mock_run.side_effect = subprocess.CalledProcessError(1, "docker kill")

        # Execute
        result = self.manager.kill_container("target_container")

        # Assert
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
