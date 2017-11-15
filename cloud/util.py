import ConfigParser
import os


def abspath_join(*args):
    return os.path.abspath(os.path.join(os.sep, *args))


root_dir = abspath_join(__file__, '../../')
config_file = os.path.join(root_dir, 'config/config.ini')

config = ConfigParser.ConfigParser()
config.read(config_file)

target_dir = config.get('CLOUD', 'target_dir')
project_id = config.get('CLOUD', 'project_id')
dataset_id = config.get('CLOUD', 'dataset_id')

beam_runner = config.get('CLOUD', 'runner')
beam_temp_location = config.get('CLOUD', 'temp_location')


def date_dir(dat):
    return '/'.join([target_dir, dat.strftime('%Y-%m-%d')])
