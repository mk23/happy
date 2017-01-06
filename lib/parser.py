import argparse
import multiprocessing
import os
import sys
import textwrap

from . import VERSION


def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='hdfs directory sync', version=VERSION, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-u', '--hdfs-url', required=True,
                        help='full hdfs url to sync')
    parser.add_argument('-d', '--dest-dir', required=True,
                        help='destination directory')
    parser.add_argument('-i', '--includes', default=[], action='append', nargs='*',
                        help='explicit file globs instead of all')
    parser.add_argument('-t', '--temp-dir', default='/tmp',
                        help='where to put the temporary directory for downloads')
    parser.add_argument('-s', '--sync-dir', default='mirror',
                        help='relative directory to mirror sources into')
    parser.add_argument('-e', '--arch-dir', default='unpack',
                        help='relative directory to unpack archives into')
    parser.add_argument('-c', '--conf-dir',
                        help='directory of dataset configurations')
    parser.add_argument('-p', '--run-port', default=2311, type=int,
                        help='lock loopback port number')
    parser.add_argument('-l', '--log-conf',
                        help='logger destination url')
    parser.add_argument('-m', '--manifest', default='.%s.idx' % os.path.splitext(os.path.basename(sys.argv[0]))[0],
                        help='manifest index file name')
    parser.add_argument('-w', '--workers', type=int, default=multiprocessing.cpu_count(),
                        help='number of download threads')
    parser.add_argument('-n', '--dry-run', default=False, action='store_true',
                        help='show actions to be performed')
    parser.add_argument('-o', '--timeout', default=4, type=float,
                        help='request timeout in seconds')
    parser.epilog = textwrap.dedent('''
        supported logger formats:
          console://?level=LEVEL
          file://PATH?level=LEVEL
          syslog+tcp://HOST:PORT/?facility=FACILITY&level=LEVEL
          syslog+udp://HOST:PORT/?facility=FACILITY&level=LEVEL
          syslog+unix://PATH?facility=FACILITY&level=LEVEL
    ''')

    return parser.parse_args()
