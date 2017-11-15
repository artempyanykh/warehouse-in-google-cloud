import luigi
from local import ProcessFines
from luigi.contrib import postgres
from local.util import database_config


class LoadRichFines(postgres.CopyToTable):
    on = luigi.DateParameter()

    host = database_config['host']
    database = database_config['name']
    user = database_config['user']
    password = database_config['password']

    table = 'rich_fines'
    columns = ProcessFines.final_schema
    column_separator = ','

    def requires(self):
        return ProcessFines(self.on)

    def rows(self):
        generator = super(LoadRichFines, self).rows()
        generator.next()  # skip header

        return generator

    def init_copy(self, connection):
        query = 'TRUNCATE TABLE {table}'.format(table=self.table)
        connection.cursor().execute(query)
