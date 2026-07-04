"""Small tests for stable identities, replay content, schemas, and upsert filters."""

import tempfile
import unittest
from pathlib import Path

from src.common.hashing import file_sha256, make_metadata_id
from src.common.schemas import MetadataEvent
from src.spark_jobs.metadata_to_mongodb import metadata_upsert_filter
from src.verification.replay_one_file import set_probe_state


class CoreBehaviorTests(unittest.TestCase):
    def test_metadata_id_is_stable(self) -> None:
        self.assertEqual(
            make_metadata_id("repo", "src/example.py"),
            make_metadata_id("repo", "src/example.py"),
        )

    def test_upsert_filter_prefers_metadata_id(self) -> None:
        document = {"metadata_id": "stable", "repo_name": "repo", "file_path": "x.py"}
        self.assertEqual(metadata_upsert_filter(document), {"metadata_id": "stable"})

    def test_upsert_filter_falls_back_to_repo_and_path(self) -> None:
        document = {"repo_name": "repo", "file_path": "x.py"}
        self.assertEqual(
            metadata_upsert_filter(document),
            {"repo_name": "repo", "file_path": "x.py"},
        )

    def test_controlled_modification_changes_hash(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "probe.py"
            set_probe_state(path, modified=False)
            baseline_hash = file_sha256(path)
            set_probe_state(path, modified=True)
            self.assertNotEqual(baseline_hash, file_sha256(path))

    def test_event_contract_has_version_and_time(self) -> None:
        event = MetadataEvent(
            schema_version="1.0",
            event_time="2026-07-04T00:00:00Z",
            repo_name="repo",
            file_path="x.py",
            metadata_id="id",
            file_hash="hash",
            line_count=1,
            function_count=0,
            class_count=0,
            import_count=0,
            node_count=1,
            edge_count=0,
        )
        self.assertEqual(event.schema_version, "1.0")
        self.assertTrue(event.event_time.endswith("Z"))


if __name__ == "__main__":
    unittest.main()
