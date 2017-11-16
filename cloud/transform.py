from cloud import *
from cloud.util import date_dir
from cloud.beam_util import CsvSource, CsvSink, WriteWithSuccessFile, pipeline_options

import apache_beam as b
from apache_beam import pvalue
from datetime import datetime

from collections import OrderedDict

import luigi
from luigi.contrib.gcs import GCSTarget


class ProcessFines(luigi.Task):
    on = luigi.DateParameter()

    def requires(self):
        return {
            'users': ExtractUsers(self.on),
            'cars': ExtractCars(self.on),
            'rents': ExtractRents(self.on),
            'fines': ExtractFines(self.on),
        }

    def output(self):
        return GCSTarget('%s/%s' % (date_dir(self.on), 'rich_fines'))

    def run(self):
        p = b.Pipeline(options=pipeline_options())
        fines_path = self.input()['fines'].path
        rents_path = self.input()['rents'].path
        cars_path = self.input()['cars'].path
        users_path = self.input()['users'].path

        fines = p | 'ReadFines' >> b.io.Read(CsvSource(fines_path))
        rents = p | 'ReadRents' >> b.io.Read(CsvSource(rents_path))
        cars = p | 'ReadCars' >> b.io.Read(CsvSource(cars_path))
        users = p | 'ReadUsers' >> b.io.Read(CsvSource(users_path))

        users_by_id = users | b.Map(lambda x: (x['id'], x))
        cars_by_id = cars | b.Map(lambda x: (x['id'], x))

        rich_rents = rents | b.Map(parse_rent) | b.Map(
            combine_with_cars_and_users,
            users=pvalue.AsDict(users_by_id),
            cars=pvalue.AsDict(cars_by_id)
        )

        rents_by_reg_no = rich_rents | b.Map(lambda x: ((x['car_reg_number'], x['rented_on']), x))
        fines_by_reg_no = fines | b.Map(parse_fine) | b.Map(lambda x: ((x['car_reg_number'], x['registered_on']), x))

        rich_fines = (rents_by_reg_no, fines_by_reg_no) | b.CoGroupByKey() | b.FlatMap(enrich_with_fine_data)

        header = list(self.final_schema.keys())

        (rich_fines
         | 'WriteRichFines' >> WriteWithSuccessFile(CsvSink('%s/%s' % (self.output().path, 'data.csv'), header)))

        run_result = p.run().wait_until_finish()
        return run_result

    final_schema = OrderedDict([
        ('fine_id', 'INTEGER'),
        ('fine_amount', 'FLOAT'),
        ('fine_registered_at', 'DATETIME'),
        ('rent_id', 'INTEGER'),
        ('rented_on', 'DATE'),
        ('car_id', 'INTEGER'),
        ('car_reg_number', 'STRING'),
        ('car_make', 'STRING'),
        ('car_model', 'STRING'),
        ('user_id', 'INTEGER'),
        ('user_name', 'STRING'),
        ('user_passport_no', 'STRING'),
        ('user_birth_date', 'DATE'),
        ('user_driving_permit_since', 'DATE')
    ])


def parse_fine(raw_fine):
    raw_fine['registered_on'] = datetime.strptime(raw_fine['registered_at'], '%Y-%m-%d %H:%M:%S').date()
    return raw_fine


def parse_rent(raw_rent):
    raw_rent['rented_on'] = datetime.strptime(raw_rent['rented_on'], '%Y-%m-%d').date()
    return raw_rent


def combine_with_cars_and_users(element, users, cars):
    user = users[element['user_id']]
    car = cars[element['car_id']]

    out = dict(element)
    out['rent_id'] = out['id']
    del (out['id'])
    out['car_reg_number'] = car['reg_number']
    out['car_make'] = car['make']
    out['car_model'] = car['model']
    out['user_name'] = user['name']
    out['user_passport_no'] = user['passport_no']
    out['user_birth_date'] = user['birth_date']
    out['user_driving_permit_since'] = user['driving_permit_since']

    return out


def enrich_with_fine_data(key_rent_fines):
    _, (rents, fines) = key_rent_fines
    rent = rents[0]

    for fine in fines:
        out = dict(rent)
        out['fine_id'] = fine['id']
        out['fine_registered_at'] = fine['registered_at']
        out['fine_amount'] = fine['fine_amount']
        yield out
