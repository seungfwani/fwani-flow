"""ExecutionStatusPresenter: present / expand_filter_values 대칭 및 버킷."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.domain.enums import FlowExecutionStatus
from models.domain.execution_status_presenter import DefaultExecutionStatusPresenter


class TestDefaultExecutionStatusPresenter(unittest.TestCase):
    def setUp(self):
        self.p = DefaultExecutionStatusPresenter()

    def test_present_expand_symmetry_for_enum_values(self):
        for member in FlowExecutionStatus:
            raw = member.value
            bucket = self.p.present(raw)
            self.assertIsNotNone(bucket, msg=raw)
            expanded = self.p.expand_filter_values({bucket})
            self.assertIn(raw, expanded, msg=f"{raw} -> {bucket} -> {expanded}")

    def test_present_none(self):
        self.assertIsNone(self.p.present(None))

    def test_expand_failed_includes_all_failed_bucket(self):
        exp = self.p.expand_filter_values({"failed"})
        for m in (
            FlowExecutionStatus.FAILED,
            FlowExecutionStatus.ERROR,
            FlowExecutionStatus.KILLED,
            FlowExecutionStatus.CANCELED,
            FlowExecutionStatus.SKIPPED,
        ):
            self.assertIn(m.value, exp)

    def test_expand_unknown_yields_empty(self):
        self.assertEqual(self.p.expand_filter_values({"not-a-bucket"}), set())


if __name__ == "__main__":
    unittest.main()
