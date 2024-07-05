import unittest
from batch_processor import BatchProcessor


class TestBatchProcessor(unittest.TestCase):

    def setUp(self):
        self.processor = BatchProcessor()

    def test_empty_records(self):
        batches = self.processor.split_into_batches([])
        self.assertEqual(batches, [])

    def test_records_with_order(self):
        records = ["a" * 100, "b" * 200, "c" * 300]
        batches = self.processor.split_into_batches(records)
        self.assertEqual(len(batches), 1)
        self.assertEqual(len(batches[0]), 3)
        # Check the order
        self.assertEqual(batches[0], records)

    def test_records_split_across_batches(self):
        size = self.processor.max_record_size - 1
        records = ["a" * size, "b" * size, "c" * size, "d" * size, "e" * size, "f" * size]
        batches = self.processor.split_into_batches(records)
        self.assertEqual(len(batches), 2)
        self.assertEqual(len(batches[0]), 5)
        self.assertEqual(len(batches[1]), 1)

    def test_max_records_per_batch(self):
        records = ["a"] * (self.processor.max_records_per_batch + 1)
        batches = self.processor.split_into_batches(records)
        self.assertEqual(len(batches), 2)
        self.assertEqual(len(batches[0]), self.processor.max_records_per_batch)
        self.assertEqual(len(batches[1]), 1)
