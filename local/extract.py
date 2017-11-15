import luigi
from luigi import LocalTarget
from local.util import root_dir
from local.util import date_dir
from local.util import abspath_join


class SampleDataLoad(luigi.Task):
    on = luigi.DateParameter()

    @property
    def sample_file_name(self):
        raise NotImplementedError()

    def run(self):
        input_filename = abspath_join(root_dir, 'sampledata/generated/%s' % self.sample_file_name)

        with open(input_filename) as input_file:
            with self.output().open('w') as local_file:
                local_file.writelines(input_file.readlines())

    def output(self):
        return LocalTarget('%s/%s' % (date_dir(self.on), self.sample_file_name))


class ExtractUsers(SampleDataLoad):
    sample_file_name = 'users.csv'


class ExtractCars(SampleDataLoad):
    sample_file_name = 'cars.csv'


class ExtractRents(SampleDataLoad):
    sample_file_name = 'rents.csv'


class ExtractFines(SampleDataLoad):
    sample_file_name = 'fines.csv'
