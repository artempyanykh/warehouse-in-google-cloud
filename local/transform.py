from local import *
from local.util import date_dir

import luigi
from luigi import LocalTarget

import pandas as pd


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
        return LocalTarget('%s/%s' % (date_dir(self.on), 'rich_fines.csv'))

    def run(self):
        fines = pd.read_csv(self.input()['fines'].path)
        rents = pd.read_csv(self.input()['rents'].path)
        cars = pd.read_csv(self.input()['cars'].path)
        users = pd.read_csv(self.input()['users'].path)

        rich_rents = (rents
                      .rename(columns={'id': 'rent_id'})
                      .assign(rented_on=pd.to_datetime(rents.rented_on).dt.date)
                      .merge(users.rename(columns={'id': 'user_id'}), on='user_id')
                      .merge(cars.rename(columns={'id': 'car_id'}), on='car_id'))

        fines['registered_on'] = pd.to_datetime(fines['registered_at']).dt.date

        rich_fines = (fines
                      .merge(rich_rents,
                             left_on=['car_reg_number', 'registered_on'],
                             right_on=['reg_number', 'rented_on']))

        current_cols = [
            'id',
            'fine_amount',
            'registered_at',
            'rent_id',
            'rented_on',
            'car_id',
            'car_reg_number',
            'make',
            'model',
            'user_id',
            'name',
            'passport_no',
            'birth_date',
            'driving_permit_since'
        ]

        desired_schema_cols = [x[0] for x in self.final_schema]

        mapping = dict(zip(current_cols, desired_schema_cols))

        final_df = rich_fines.rename(columns=mapping)[desired_schema_cols]

        with self.output().open('w') as buf:
            final_df.to_csv(buf, index=False)

        return

    final_schema = [
        ('fine_id', 'INTEGER'),
        ('fine_amount', 'FLOAT'),
        ('fine_registered_at', 'TIMESTAMP'),
        ('rent_id', 'INTEGER'),
        ('rented_on', 'DATE'),
        ('car_id', 'INTEGER'),
        ('car_reg_number', 'VARCHAR'),
        ('car_make', 'VARCHAR'),
        ('car_model', 'VARCHAR'),
        ('user_id', 'INTEGER'),
        ('user_name', 'VARCHAR'),
        ('user_passport_no', 'VARCHAR'),
        ('user_birth_date', 'DATE'),
        ('user_driving_permit_since', 'DATE')
    ]
