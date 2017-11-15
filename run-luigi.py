#!/usr/bin/env python

import os
import sys

import luigi.cmdline

# sys.path.append(os.path.dirname(__file__))

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(
    os.path.join(os.path.sep, os.path.dirname(os.path.abspath(__file__)), 'config/credentials.json'))


def main(argv):
    print('===== Start summary =====')
    print("Using credentials from file: %s" % os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
    print("\n")

    luigi.cmdline.luigi_run(argv)


if __name__ == '__main__':
    main(sys.argv[1:])
