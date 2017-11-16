#!/usr/bin/env python
import os
import IPython
from google.cloud import storage
import ConfigParser


def abspath_join(*args):
    return os.path.abspath(os.path.join(os.sep, *args))


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(
    os.path.join(os.path.sep, os.path.dirname(os.path.abspath(__file__)), 'config/credentials.json'))

config_file = abspath_join(os.path.abspath(__file__), '../config/config.ini')
config = ConfigParser.ConfigParser()
config.read(config_file)

bucket_name = config.get('CLOUD', 'target_dir')[5:]

client = storage.Client()
bucket = client.bucket(bucket_name)


def gls(path=''):
    return list(bucket.list_blobs(prefix=path))


def gcat(path):
    blob = bucket.get_blob(path)
    contents = blob.download_as_string()
    for x in contents.split('\n'):
        print x

    return


ns = {
    'client': storage.Client(),
    'bucket': bucket,
    'gls': gls,
    'gcat': gcat
}

IPython.start_ipython(user_ns=ns)
