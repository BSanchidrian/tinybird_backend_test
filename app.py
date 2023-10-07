import csv
import io
import json
import sys
import tornado.ioloop
import tornado.web

from datetime import date

class CsvWriter():
    def write_csv(self, decoded_body, csv_name):
        records_valid = 0
        records_invalid = 0
        max_rows = 100
        with open(csv_name, 'a') as fw:
            fieldnames = [
                'vendorid',
                'tpep_pickup_datetime',
                'trip_distance',
                'total_amount',
            ]
            writer = csv.DictWriter(fw, fieldnames, extrasaction='ignore')
            rows = []

            for record in decoded_body:
                try:
                    row = json.loads(record)
                    rows.append(row)

                    if len(rows) >= max_rows:
                        writer.writerows(rows)
                        records_valid += len(rows)
                        rows = []
                except Exception:
                    records_invalid += 1

            if rows:
                writer.writerows(rows)
                records_valid += len(rows)

        return records_valid, records_invalid
    
    # Leaving this here at purpose for profiling. Currently using the version above.
    def write_csv_old(self, request_body, csv_name):
        records_valid = 0
        records_invalid = 0
        with open(csv_name, 'a') as fw:
            fieldnames = [
                'vendorid',
                'tpep_pickup_datetime',
                'trip_distance',
                'total_amount',
            ]
            writer = csv.DictWriter(fw, fieldnames, extrasaction='ignore')
            fr = io.BytesIO(request_body)
            for record in fr.readlines():
                try:
                    row = json.loads(record)
                    writer.writerow(row)
                    records_valid += 1
                except Exception:
                    records_invalid += 1

        return records_valid, records_invalid

@tornado.web.stream_request_body
class DataReceiverHandler(tornado.web.RequestHandler):
    def initialize(self):
        self.csv_writer = CsvWriter()
        self.request_body = ''
        self.total_size = 0
        self.records_valid = 0
        self.records_invalid = 0
        self.csv_file_path = f"./csv/nyc_taxi-{date.today()}.csv"
        self.buffer_size = 1024 * 4 # 4kb buffer

    async def data_received(self, chunk):
        self.request_body += chunk.decode('utf-8')
        self.total_size += len(chunk)

        if len(self.request_body) >= self.buffer_size:
            decoded_body = [line for line in self.request_body.split('\n') if line]
            if decoded_body[-1].endswith('}'):
                self.request_body = ''
            else:
                self.request_body = decoded_body.pop()

            valid, invalid = self.csv_writer.write_csv(decoded_body, self.csv_file_path)
            self.records_valid += valid
            self.records_invalid += invalid

    async def post(self):
        if self.request_body:
            decoded_body = [line for line in self.request_body.split('\n') if line]
            valid, invalid = self.csv_writer.write_csv(decoded_body, self.csv_file_path)
            self.records_valid += valid
            self.records_invalid += invalid

        result = {
            'result': {
                'status': 'ok',
                'stats': {
                    'bytes': self.total_size,
                    'records': {
                        'valid': self.records_valid,
                        'invalid': self.records_invalid,
                        'total': self.records_valid + self.records_invalid,
                    },
                }
            }
        }
        self.write(result)

def run():
    port = 8888
    address = '0.0.0.0'
    debug = bool(sys.flags.debug)
    processes = 8
    run_multiple_processes(port, address, processes, debug)

    print(f"server listening at {address}:{port} debug={debug}")

def run_multiple_processes(port, address, processes, debug):
    sockets = tornado.netutil.bind_sockets(port, address)
    tornado.process.fork_processes(processes)

    application = create_application(debug)
    server = tornado.httpserver.HTTPServer(application, max_buffer_size=100 * 1024 * 1024 * 1024)  # 100G
    server.add_sockets(sockets)

    print(f"server listening at {address}:{port} debug={debug}")

    tornado.ioloop.IOLoop.current().start()

def run_single_thread(port, address, debug):
    application = create_application(debug)
    server = tornado.httpserver.HTTPServer(application, max_buffer_size=100 * 1024 * 1024 * 1024)  # 100G
    server.listen(port, address)
    
    print(f"server listening at {address}:{port} debug={debug}")

    tornado.ioloop.IOLoop.current().start()

def create_application(debug):
    handlers = [
        (r"/", DataReceiverHandler),
    ]
    settings = {
        'debug': debug
    }
    
    application = tornado.web.Application(handlers, **settings)
    return application

if __name__ == "__main__":
    debug = bool(sys.flags.debug)
    run_single_thread(8888, '0.0.0.0', debug)
