"""Small tests for stable identities, replay content, schemas, and upsert filters."""

import tempfile
import unittest
from pathlib import Path

from src.common.hashing import file_sha256, make_metadata_id
from src.common.schemas import MetadataEvent
from src.spark_jobs.metadata_to_mongodb import build_metadata_schema, mongodb_write_options
from src.verification.replay_one_file import set_probe_state


class CoreBehaviorTests(unittest.TestCase):
    def test_metadata_id_is_stable(self) -> None:
        self.assertEqual(
            make_metadata_id("repo", "src/example.py"),
            make_metadata_id("repo", "src/example.py"),
        )

    def test_metadata_schema_contains_required_fields(self) -> None:
        self.assertTrue(
            {
                "schema_version",
                "event_time",
                "repo_name",
                "file_path",
                "metadata_id",
                "file_hash",
                "line_count",
                "function_count",
                "class_count",
                "import_count",
                "node_count",
                "edge_count",
            }.issubset(build_metadata_schema().fieldNames())
        )

    def test_mongodb_connector_options_are_complete_and_idempotent(self) -> None:
        options = mongodb_write_options()
        self.assertTrue(options["connection.uri"])
        self.assertTrue(options["database"])
        self.assertTrue(options["collection"])
        self.assertEqual(options["idFieldList"], "metadata_id")
        self.assertEqual(options["operationType"], "replace")
        self.assertEqual(options["upsertDocument"], "true")

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
