import json
from pathlib import Path
import unittest

from collector.domain import *


class DomainTests(unittest.TestCase):
    def setUp(self):
        self.payload = json.loads(Path('fixtures/sample.json').read_text())

    def test_domain_behavior(self):
        self.assertTrue(intersects([0, 0, 2, 2], [1, 1, 3, 3]))
        self.assertEqual(instant('2026-01-01T08:00:00+08:00').hour, 0)
        with self.assertRaises(ValueError):
            instant('2026-01-01T00:00:00')

    def test_deterministic_observations(self):
        self.assertEqual(collect(self.payload), collect(self.payload))
