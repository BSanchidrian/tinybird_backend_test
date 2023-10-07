import os
import unittest
import csv
import uuid
from app import CsvWriter

class CsvWriterTests(unittest.TestCase):

    def test_write_csv_with_valid_jsons(self):
        sut = CsvWriter()
        test_jsons = [
            '{"vendorid": 2, "tpep_pickup_datetime": "2017-01-01 00:00:00", "trip_distance": 0.02, "total_amount": 52.8}',
            '{"vendorid": 2, "tpep_pickup_datetime": "2017-01-01 00:00:02", "trip_distance": 7.75, "total_amount": 27.96}',
            '{"vendorid": 1, "tpep_pickup_datetime": "2017-01-01 00:00:02", "trip_distance": 0.5, "total_amount": 5.3}',
            '{"vendorid": 1, "tpep_pickup_datetime": "2017-01-01 00:00:03", "trip_distance": 0.8, "total_amount": 8.75}'
        ]

        csv_filename = f'csv/{str(uuid.uuid4())}.csv'
        records_valid, records_invalid = sut.write_csv(test_jsons, csv_filename)

        self.assertEqual(records_valid, len(test_jsons))
        self.assertEqual(records_invalid, 0)

        expected_lines = [
            "2,2017-01-01 00:00:00,0.02,52.8",
            "2,2017-01-01 00:00:02,7.75,27.96",
            "1,2017-01-01 00:00:02,0.5,5.3",
            "1,2017-01-01 00:00:03,0.8,8.75",
        ]

        with open(csv_filename, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            csv_lines = [','.join(row) for row in reader]

        self.assertEqual(csv_lines, expected_lines)
        os.unlink(csv_filename)

if __name__ == '__main__':
    unittest.main()