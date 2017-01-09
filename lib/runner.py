import datetime
import errno
import getpass
import happy.state
import itertools
import logging
import multiprocessing.pool
import os
import pickle
import socket
import sys
import time
import urlparse
import webhdfs


LOG = logging.getLogger()

def setup_socket(port, sock=socket.socket()):
    try:
        sock.bind(('127.0.0.1', port))
        sock.listen(1)
        LOG.debug('bound local listening socket for mutual exclusion on port: %d', port)
    except socket.error as e:
        if e.errno == errno.EADDRINUSE:
            LOG.err('another %s process is already running', sys.argv[0])
        else:
            happy.log_error(e)
        sys.exit(1)


def setup_runner(args):
    setup_socket(args.run_port)

    dest_dir = os.path.abspath(args.dest_dir)
    temp_dir = os.path.abspath(args.temp_dir)
    sync_dir = os.path.normpath('%s/%s' % (dest_dir, args.sync_dir))
    arch_dir = os.path.normpath('%s/%s' % (dest_dir, args.arch_dir))

    if os.stat(dest_dir).st_dev != os.stat(temp_dir).st_dev:
        LOG.err('destination and temp directores are cross-device')
        sys.exit(1)

    hdfs_url = urlparse.urlparse(args.hdfs_url)
    hdfs_dir = hdfs_url.path
    hdfs_api = webhdfs.WebHDFSClient(hdfs_url._replace(path='').geturl(), user=getpass.getuser(), wait=args.timeout)
    includes = set(itertools.chain.from_iterable(args.includes)) or ['*']
    start_ts = datetime.datetime.now()

    try:
        if not os.path.exists(sync_dir):
            if not args.dry_run:
                os.makedirs(sync_dir)
                LOG.info('created mirror path: %s', sync_dir)
            else:
                LOG.info('creating mirror path: %s', sync_dir)
        if not os.path.exists(arch_dir):
            if not args.dry_run:
                os.makedirs(arch_dir)
                LOG.info('created unpack path: %s', arch_dir)
            else:
                LOG.info('creating unpack path: %s', arch_dir)

        index = os.path.normpath('%s/%s' % (dest_dir, os.path.basename(args.manifest)))
        local = happy.state.setup_local(index, sync_dir, arch_dir)
        avail = happy.state.setup_avail(hdfs_api, hdfs_dir, includes, sync_dir, arch_dir)
        check = happy.state.setup_check(args.conf_dir)
        procs = multiprocessing.pool.ThreadPool(processes=args.workers)
        xfers = {}

        for key, val in avail.items():
            if (key not in local or not val.equal(local[key])):
                xfers[key] = procs.apply_async(val.fetch, (hdfs_api, temp_dir, args.dry_run))

        procs.close()
        procs.join()

        for key, val in xfers.items():
            if val.get():
                local[key] = avail[key]
            else:
                LOG.err('failed to fetch %s', key)

        for key, val in local.items():
            if key not in avail and val.purge(args.dry_run):
                del(local[key])

        for key, val in local.items():
            if val.remote.full in check:
                val.check(check[val.remote.full], os.stat(index).st_mtime if os.path.exists(index) else time.mktime(datetime.datetime.min.timetuple()), args.dry_run)

        if not args.dry_run:
            pickle.dump(local, open(index, 'w'))
            LOG.info('saved %d items to index: %s', len(local), index)

        happy.state.clean_local(index, local, sync_dir, arch_dir, args.dry_run)

        LOG.info('execution completed in %ds', (datetime.datetime.now() - start_ts).total_seconds())
    except Exception as e:
        happy.log_error(e)
