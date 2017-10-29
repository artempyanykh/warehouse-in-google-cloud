#!/usr/bin/env python
import os
import sys
import apache_beam as b
from apache_beam.io import filebasedsink
from apache_beam.io import textio
from apache_beam.io import filesystem
from apache_beam.coders import coders
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue
from apache_beam.io.gcp import bigquery
import csv
from StringIO import StringIO
from datetime import datetime

sys.path.append(os.path.dirname(__file__))

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(
    os.path.join(os.path.sep, os.path.dirname(os.path.abspath(__file__)), 'config/credentials.json'))


class CsvSource(textio._TextSource):
    DEFAULT_READ_BUFFER_SIZE = 8192

    def __init__(self,
                 file_pattern,
                 min_bundle_size=0,
                 compression_type=filesystem.CompressionTypes.AUTO,
                 strip_trailing_newlines=True,
                 coder=coders.StrUtf8Coder(),
                 buffer_size=DEFAULT_READ_BUFFER_SIZE,
                 validate=True):
        super(self.__class__, self).__init__(
            file_pattern, min_bundle_size, compression_type, strip_trailing_newlines, coder,
            buffer_size, validate, 1)

    def _read_line(self, file_to_read, read_buffer):
        """Skip num_lines from file_to_read, return num_lines+1 start position."""
        if file_to_read.tell() > 0:
            file_to_read.seek(0)
        position = 0
        _, num_bytes_to_next_record = self._read_record(file_to_read, read_buffer)
        if num_bytes_to_next_record > 0:
            position += num_bytes_to_next_record
        return position

    def read_records(self, file_name, range_tracker):
        with self.open_file(file_name) as file_to_read:
            read_buffer = textio._TextSource.ReadBuffer('', 0)
            record, _ = self._read_record(file_to_read, read_buffer)
            header_string = self._coder.decode(record)

        header = list(csv.reader(([header_string])))[0]

        for record in super(self.__class__, self).read_records(file_name, range_tracker):
            elem = self._coder.decode(record)
            parsed = list(csv.DictReader([elem], header))[0]
            yield parsed


class CsvSink(filebasedsink.FileBasedSink):
    def __init__(self,
                 file_path_prefix,
                 header,
                 file_name_suffix='',
                 num_shards=0,
                 shard_name_template=None,
                 coder=coders.PickleCoder(),
                 compression_type=filesystem.CompressionTypes.AUTO):
        super(CsvSink, self).__init__(
            file_path_prefix,
            file_name_suffix=file_name_suffix,
            num_shards=num_shards,
            shard_name_template=shard_name_template,
            coder=coder,
            mime_type='text/csv',
            compression_type=compression_type)
        self._header = header

    def open(self, temp_path):
        file_handle = super(self.__class__, self).open(temp_path)
        output = StringIO()
        csv.DictWriter(output, self._header).writeheader()
        file_handle.write(output.getvalue())

        return file_handle

    def write_encoded_record(self, file_handle, encoded_value):
        output = StringIO()
        value = self.coder.decode(encoded_value)
        writer = csv.DictWriter(output, self._header)
        writer.writerow(value)
        file_handle.write(output.getvalue())


def main(argv):
    print('===== Start summary =====')
    print("Using credentials from file: %s" % os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    print("\n")

    opts = PipelineOptions()
    p = b.Pipeline(options=opts)

    basedir = 'sampledata/generated'

    fines = p | 'read_fines' >> b.io.Read(CsvSource("%s/fines.csv" % basedir))
    rents = p | 'read_rents' >> b.io.Read(CsvSource("%s/rents.csv" % basedir))
    cars = p | 'read_cars' >> b.io.Read(CsvSource("%s/cars.csv" % basedir))
    users = p | 'read_users' >> b.io.Read(CsvSource("%s/users.csv" % basedir))

    def parse_fine(raw_fine):
        raw_fine['registered_on'] = datetime.strptime(raw_fine['registered_at'], '%Y-%m-%d %H:%M:%S').date()
        return raw_fine

    def parse_rent(raw_rent):
        raw_rent['rented_on'] = datetime.strptime(raw_rent['rented_on'], '%Y-%m-%d').date()
        return raw_rent

    class CombineRentWithCarsAndUsers(b.DoFn):
        def process(self, element, users, cars):
            user = users[element['user_id']]
            car = cars[element['car_id']]

            out = dict(element)
            out['rent_id'] = out['id']
            del(out['id'])
            out['car_reg_number'] = car['reg_number']
            out['car_make'] = car['make']
            out['car_model'] = car['model']
            out['user_name'] = user['name']
            out['user_passport_no'] = user['passport_no']
            out['user_birth_date'] = user['birth_date']
            out['user_driving_permit_since'] = user['driving_permit_since']

            return [out]

    def enrich_with_fine_data(key_rents_fine):
        _, (rents, fines) = key_rents_fine
        rent = rents[0]

        for fine in fines:
            out = dict(rent)
            out['fine_id'] = fine['id']
            out['fine_registered_at'] = fine['registered_at']
            out['fine_amount'] = fine['fine_amount']
            yield out

    users_by_id = users | b.Map(lambda x: (x['id'], x))
    cars_by_id = cars | b.Map(lambda x: (x['id'], x))

    rich_rents = rents | b.Map(parse_rent) | b.ParDo(
        CombineRentWithCarsAndUsers(),
        users=pvalue.AsDict(users_by_id),
        cars=pvalue.AsDict(cars_by_id)
    )

    rents_by_reg_no = rich_rents | b.Map(lambda x: ((x['car_reg_number'], x['rented_on']), x))
    fines_by_reg_no = fines | b.Map(parse_fine) | b.Map(lambda x: ((x['car_reg_number'], x['registered_on']), x))

    combined = (rents_by_reg_no, fines_by_reg_no) | b.CoGroupByKey() | b.FlatMap(enrich_with_fine_data)

    header = ['']
    combined | 'write_result' >> b.io.Write(CsvSink('gs://warehouse-in-gcs-store/glob/rich_fines/data.csv'))
    p.run()


if __name__ == "__main__":
    main(sys.argv[1:])


# lines = p | 'ReadFile' >> b.io.Read(CsvSource('gs://warehouse-in-gcs-store/2017-10-26/users.csv'))
# users_schema = 'id:INTEGER,name:STRING,passport_no:STRING,birth_date:DATE,driving_permit_since:DATE'
# lines | 'WriteToFile' >> b.io.Write(
#     CsvSink('gs://warehouse-in-gcs-store/test/users/', ['id', 'name', 'passport_no', 'birth_date', 'driving_permit_since']))
# output = lines | 'WriteFile' >> b.io.WriteToBigQuery('artem-pyanykh-warehouse-in-gcp:cloud_data.users2',
#                                                      schema=users_schema, write_disposition=bigquery.BigQueryDisposition.WRITE_TRUNCATE)
