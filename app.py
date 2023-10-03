import csv
import io
import json
import sys
import tornado.ioloop
import tornado.web

from line_profiler import profile

from datetime import date

class CsvWriter():
    @profile
    def write_csv(self, request_body, csv_name):
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
            # I'm basically decoding the byte array, spliting by '\n' and removing the last empty entry which is ''
            decoded_body = [line for line in request_body.decode('utf-8').split('\n') if line]
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
    @profile
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
        self.request_body = b''

    def data_received(self, chunk):
        self.request_body += chunk

    def post(self):
        records_valid, records_invalid = self.csv_writer.write_csv(self.request_body, f"csv/nyc_taxi-{date.today()}.csv")
        result = {
            'result': {
                'status': 'ok',
                'stats': {
                    'bytes': len(self.request_body),
                    'records': {
                        'valid': records_valid,
                        'invalid': records_invalid,
                        'total': records_valid + records_invalid,
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
    server = tornado.httpserver.HTTPServer(application)
    server.add_sockets(sockets)

    print(f"server listening at {address}:{port} debug={debug}")

    tornado.ioloop.IOLoop.current().start()

def run_single_thread(port, address, debug):
    application = create_application(debug)
    application.listen(port, address)
    
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
