import os
import unittest
import csv
import uuid
from app import CsvWriter

class CsvWriterTests(unittest.TestCase):

    def test_write_csv_with_valid_jsons(self):
        sut = CsvWriter()
        test_jsons = [
            '{"vendorid":2,"tpep_pickup_datetime":"2017-01-01 00:00:00","tpep_dropoff_datetime":"2017-01-01 00:00:00","passenger_count":1,"trip_distance":0.02,"ratecodeid":2,"store_and_fwd_flag":"N","pulocationid":249,"dolocationid":234,"payment_type":2,"fare_amount":52,"extra":0,"mta_tax":0.5,"tip_amount":0,"tolls_amount":0,"improvement_surcharge":0.3,"total_amount":52.8}',
            '{"vendorid":2,"tpep_pickup_datetime":"2017-01-01 00:00:02","tpep_dropoff_datetime":"2017-01-01 00:39:22","passenger_count":4,"trip_distance":7.75,"ratecodeid":1,"store_and_fwd_flag":"N","pulocationid":186,"dolocationid":36,"payment_type":1,"fare_amount":22,"extra":0.5,"mta_tax":0.5,"tip_amount":4.66,"tolls_amount":0,"improvement_surcharge":0.3,"total_amount":27.96}',
            '{"vendorid":1,"tpep_pickup_datetime":"2017-01-01 00:00:02","tpep_dropoff_datetime":"2017-01-01 00:03:50","passenger_count":1,"trip_distance":0.5,"ratecodeid":1,"store_and_fwd_flag":"N","pulocationid":48,"dolocationid":48,"payment_type":2,"fare_amount":4,"extra":0.5,"mta_tax":0.5,"tip_amount":0,"tolls_amount":0,"improvement_surcharge":0.3,"total_amount":5.3}',
            '{"vendorid":1,"tpep_pickup_datetime":"2017-01-01 00:00:03","tpep_dropoff_datetime":"2017-01-01 00:06:58","passenger_count":1,"trip_distance":0.8,"ratecodeid":1,"store_and_fwd_flag":"N","pulocationid":162,"dolocationid":161,"payment_type":1,"fare_amount":6,"extra":0.5,"mta_tax":0.5,"tip_amount":1.45,"tolls_amount":0,"improvement_surcharge":0.3,"total_amount":8.75}'
        ]
        combined_json = '\n'.join(test_jsons)

        csv_filename = f'csv/{str(uuid.uuid4())}.csv'
        records_valid, records_invalid = sut.write_csv(str.encode(combined_json), csv_filename)

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