import unittest
from unittest.mock import MagicMock, patch

from dashboard.dashboard_server import DashboardAPI


class DashboardSnapshotTests(unittest.TestCase):
    def test_snapshot_unwraps_consume_payloads_for_display(self):
        api = DashboardAPI(host="localhost", port=9092)
        mock_client = MagicMock()
        mock_client.metrics.return_value = {
            "partition_offsets": {"orders": {"0": 2}},
            "topics": 1,
            "partitions": 1,
            "active_groups": 0,
            "active_consumers": 0,
            "messages_stored": 2,
            "committed_offsets": {},
        }
        mock_client.consume.return_value = {
            "status": "success",
            "messages": [{"offset": 0, "key": "user-1", "value": "hello"}],
        }

        with patch.object(DashboardAPI, "_client", return_value=mock_client):
            snapshot = api.snapshot()

        self.assertEqual(snapshot["status"], "success")
        partition = snapshot["topics"][0]["partitions"][0]
        # message_count is the real log size (next_offset), not the sample length.
        self.assertEqual(partition["message_count"], 2)
        self.assertEqual(partition["sample_count"], 1)
        self.assertEqual(partition["sample_messages"], [{"offset": 0, "key": "user-1", "value": "hello"}])

    def test_snapshot_samples_the_newest_messages(self):
        api = DashboardAPI(host="localhost", port=9092)
        mock_client = MagicMock()
        mock_client.metrics.return_value = {"partition_offsets": {"orders": {"0": 12}}, "committed_offsets": {}}
        mock_client.consume.return_value = {"status": "success", "messages": []}

        with patch.object(DashboardAPI, "_client", return_value=mock_client):
            api.snapshot()

        # Tail of the log: offsets 7..11, not 0..4.
        mock_client.consume.assert_called_once_with("orders", 0, 7, 5)


if __name__ == "__main__":
    unittest.main()
