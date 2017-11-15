from apache_beam.io import filebasedsink, textio, filesystem, Write
from apache_beam.coders import coders
from apache_beam.transforms import ptransform, core, window
from apache_beam import pvalue
import csv
from StringIO import StringIO
from apache_beam.io.filesystems import FileSystems
import os
from apache_beam.options.pipeline_options import PipelineOptions
from cloud.util import beam_runner, beam_temp_location, project_id, root_dir, abspath_join


def pipeline_options():
    options = PipelineOptions.from_dictionary(
        {
            'runner': beam_runner,
            'project': project_id,
            'temp_location': beam_temp_location,
            'setup_file': abspath_join(root_dir, 'setup.py')
        }
    )

    return options


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


class WriteWithSuccessFile(ptransform.PTransform):
    def __init__(self, sink):
        super(WriteWithSuccessFile, self).__init__()
        self.sink = sink

    def expand(self, pcoll):
        do_once = pcoll.pipeline | 'DoOnceSuccess' >> core.Create([None])
        main_write_result = pcoll | 'MainWrite' >> Write(self.sink)

        return (do_once
                | 'SuccessWrite' >> core.FlatMap(self._success_write, pvalue.AsIter(main_write_result)))

    def _success_write(self, _, outputs):
        """
        Writes a success file to the final dir
        :param sink: apache_beam.io.filebasedsink.FileBasedSink
        :return:
        """
        main_dir = os.path.dirname(self.sink.file_path_prefix.get())
        success_filename = '/'.join([main_dir, '_SUCCESS'])
        FileSystems.create(success_filename, 'text/plain').close()

        for v in outputs:
            yield v

        yield window.TimestampedValue(success_filename, window.MAX_TIMESTAMP)
