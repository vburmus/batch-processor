import random
from typing import List


class BatchProcessor:
    ONE_MB = 1048576

    def __init__(self, max_record_size=ONE_MB, max_batch_size=5 * ONE_MB, max_records_per_batch=500):
        self.max_record_size = max_record_size
        self.max_batch_size = max_batch_size
        self.max_records_per_batch = max_records_per_batch

    def init_new_batch(self):
        return [], 0

    def split_into_batches(self, records: List[str]) -> List[List[str]]:
        batches = []
        current_batch, current_batch_size = self.init_new_batch()
        for record in records:
            # Record size is measured with len(record.encode('utf-8')) to get an actual size of a string in bytes
            record_size = len(record.encode('utf-8'))

            if record_size > self.max_record_size:
                continue

            if (current_batch_size + record_size > self.max_batch_size or
                    len(current_batch) >= self.max_records_per_batch):
                batches.append(current_batch)
                current_batch, current_batch_size = self.init_new_batch()

            current_batch.append(record)
            current_batch_size += record_size

        if current_batch:
            batches.append(current_batch)
        return batches


def generate_test_records(num_records: int, max_record_length: int = 21000) -> List[str]:
    return ["8" * random.randint(1, max_record_length) for _ in range(num_records)]


if __name__ == "__main__":
    test_records = generate_test_records(100000)
    processor = BatchProcessor()
    result_batches = processor.split_into_batches(test_records)
    for i, batch in enumerate(result_batches):
        print(f"Batch {i + 1}: {len(batch)} records, {sum(len(r) for r in batch)} bytes")