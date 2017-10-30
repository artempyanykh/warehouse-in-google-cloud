import luigi
from luigi.contrib import bigquery
from cloud import ProcessFines
from cloud.util import project_id, dataset_id


class LoadRichFines(bigquery.BigQueryLoadTask):
    on = luigi.DateParameter()

    source_format = bigquery.SourceFormat.CSV
    write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    @property
    def schema(self):
        schema_def = [{'name': field, 'type': typ} for field, typ in ProcessFines.final_schema.iteritems()]
        return schema_def

    skip_leading_rows = 1

    def requires(self):
        return ProcessFines(self.on)

    def source_uris(self):
        uri = '%s/%s' % (self.input().path, 'data.csv*')
        return [uri]

    def output(self):
        return bigquery.BigQueryTarget(project_id, dataset_id, 'rich_fines')
