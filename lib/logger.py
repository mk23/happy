import logging
import os
import socket
import traceback
import urlparse

LOG = logging.getLogger()


def setup_logger(logger=None):
    arg = {}
    log = logging.NullHandler()
    fmt = '%(asctime)s.%(msecs)03d%(module)20s:%(lineno)-3d %(threadName)-12s %(levelname)8s: %(message)s'

    if logger:
        url = urlparse.urlparse(logger)
        arg = urlparse.parse_qs(url.query)

        if url.scheme in ('file', '') and url.path:
            log = logging.handlers.WatchedFileHandler(url.path)
        elif url.scheme.startswith('syslog'):
            fmt = '%(module)s:%(lineno)d - %(threadName)s - %(message)s'
            if url.scheme == 'syslog+tcp':
                log = logging.handlers.SysLogHandler(address=(url.hostname or 'localhost', url.port or logging.handlers.SYSLOG_TCP_PORT), facility=arg.get('facility', ['user'])[0].lower(), socktype=socket.SOCK_STREAM)
            elif url.scheme == 'syslog+udp':
                log = logging.handlers.SysLogHandler(address=(url.hostname or 'localhost', url.port or logging.handlers.SYSLOG_UDP_PORT), facility=arg.get('facility', ['user'])[0].lower(), socktype=socket.SOCK_DGRAM)
            elif url.scheme == 'syslog+unix':
                log = logging.handlers.SysLogHandler(address=url.path or '/dev/log', facility=arg.get('facility', ['user'])[0].lower())
        elif url.scheme == 'console':
            log = logging.StreamHandler()

    log.setFormatter(logging.Formatter(fmt, '%Y-%m-%d %H:%M:%S'))

    LOG.addHandler(log)
    LOG.setLevel(getattr(logging, arg.get('level', ['info'])[0].upper()))

    LOG.info('logging started')


def log_error(e, msg=None):
    if msg:
        LOG.error('%s: %s', msg, e)
    else:
        LOG.error(e)

    for line in traceback.format_exc().split('\n'):
        LOG.debug('  %s', line)


def log_fatal(item, prio='error', exit=1):
    if isinstance(item, Exception):
        log_error(item)
    else:
        vars(LOG).get(prio, 'error')(item)

    if exit is not None:
        os._exit(exit)
