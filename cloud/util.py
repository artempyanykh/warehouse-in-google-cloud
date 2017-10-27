import ConfigParser
import os


def abspath_join(*args):
    return os.path.abspath(os.path.join(os.sep, *args))


root_dir = abspath_join(__file__, '../../')
config_file = os.path.join(root_dir, 'config/config.ini')

config = ConfigParser.ConfigParser()
config.read(config_file)

target_dir = config.get('CLOUD', 'target_dir')


def date_dir(dat):
    return '/'.join([target_dir, dat.strftime('%Y-%m-%d')])
