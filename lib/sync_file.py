import errno
import logging
import happy
import json
import os
import shlex
import shutil
import stat
import subprocess
import tempfile
import time

LOG = logging.getLogger()


class SyncFile(object):
    _archive_suffixes = {
        '.zip':     'unzip',
        '.txz':     'tarxz',
        '.tar.xz':  'tarxz',
        '.tgz':     'targz',
        '.tar.gz':  'targz',
        '.tbz2':    'tarbz2',
        '.tar.bz2': 'tarbz2',
    }
    _archive_commands = {
        'unzip':  ['unzip', '-qq'],
        'targz':  ['tar', '-xzf'],
        'tarxz':  ['tar', '-xJf'],
        'tarbz2': ['tar', '-xjf'],
    }

    def __init__(self, remote, source, mirror, unpack):
        self.remote = remote
        self.source = source
        self.mirror = mirror
        self.unpack = unpack

    @property
    def fullname(self):
        return self.mirror + self.remote.full[len(self.source):]

    @property
    def zip_path(self):
        for e, t in self._archive_suffixes.items():
            if self.fullname.endswith(e):
                return self.unpack + self.remote.full[len(self.source):-len(e)]

    @property
    def zip_exec(self):
        for e, t in self._archive_suffixes.items():
            if self.fullname.endswith(e):
                return self._archive_commands[t]

    @property
    def filetime(self, other=None):
        return time.mktime(self.remote.date.timetuple())

    @property
    def modified(self):
        return not os.path.exists(self.fullname) or self.filetime < os.stat(self.fullname).st_mtime

    def equal(self, item):
        return self.remote.size == item.remote.size and self.remote.date == item.remote.date

    def mkdir(self, path=None):
        try:
            os.makedirs(path)
            LOG.info('created local directory: %s', path)
        except OSError as e:
            if e.errno != errno.EEXIST or not os.path.exists(path):
                raise

    def rmdir(self, path):
        limit = max(os.path.commonprefix(i) for i in [[self.mirror, path], [self.unpack, path]])

        if not limit:
            LOG.error('request to purge untracked path: %s', path)

        while True:
            path = os.path.dirname(path)
            if path == limit:
                break

            try:
                os.rmdir(path)
                LOG.info('purged local empty directory: %s', path)
            except OSError as e:
                if e.errno == errno.ENOTEMPTY:
                    break
                else:
                    raise

    def purge(self, skip=False):
        if skip:
            LOG.info('purging local file: %s', self.fullname)
            return True

        try:
            if self.zip_path:
                shutil.rmtree(self.zip_path)
                LOG.info('purged local unpacked directory: %s', self.zip_path)

                self.rmdir(self.zip_path)

            os.unlink(self.fullname)
            LOG.info('purged local file: %s', self.fullname)

            self.rmdir(self.fullname)
            return True
        except Exception as e:
            if isinstance(e, OSError) and e.errno != errno.ENOENT:
                happy.log_error(e)

    def unzip(self, temp):
        path = self.zip_path
        if not path:
            return

        data = tempfile.mkdtemp(dir=temp)
        save = '%s.__%s__' % (path, os.path.basename(data))
        try:
            LOG.debug('created temporary unpack path: %s', data)

            os.chdir(data)
            subprocess.check_call(self.zip_exec + [self.fullname])
            LOG.debug('unpacked %s into %s', self.fullname, data)

            self.mkdir(path)

            os.rename(path, save)
            os.rename(data, path)
            shutil.rmtree(save)

            LOG.info('moved unpacked path %s to %s', data, path)
        except Exception as e:
            happy.log_error(e)

    def fetch(self, hdfs, temp=tempfile.gettempdir(), skip=False):
        if skip:
            LOG.info('fetching hdfs file: %s', self.remote.full)
            return True

        try:
            with tempfile.NamedTemporaryFile(dir=temp, delete=False) as data:
                LOG.debug('created temp file: %s', data.name)

                hdfs.get(self.remote.full, data=data)
                LOG.info('fetched hdfs file: %s', self.remote.full)

            self.mkdir(os.path.dirname(self.fullname))

            os.chmod(data.name, os.stat(data.name).st_mode|stat.S_IRGRP|stat.S_IROTH)
            os.utime(data.name, (self.filetime, self.filetime))
            os.rename(data.name, self.fullname)

            self.unzip(temp)

            LOG.debug('renamed temp file from %s to %s', data.name, self.fullname)
            return True
        except Exception as e:
            happy.log_error(e)
            if os.path.exists(data.name):
                try:
                    os.remove(data.name)
                except Exception as e:
                    happy.log_error(e)

    def check(self, cmds, last, skip=False):
        if skip:
            LOG.info('processing dataset manifest: %s', self.fullname)

        try:
            data = json.load(open(self.fullname))
            LOG.info('processing %d items from manifest: %s', len(data['files']), self.fullname)

            for path, paths, files in os.walk(os.path.dirname(self.fullname)):
                for name in files:
                    full = '%s/%s' % (path, name)
                    part = full[len(os.path.dirname(self.fullname))+1:]
                    info = os.stat(full)

                    if full == self.fullname or stat.S_ISLNK(info.st_mode):
                        continue

                    if part not in data['files']:
                        LOG.warning('  file not found in manifest, skipping: %s', full)
                    else:
                        data['files'][part]['stat'] = os.stat(full)


            find = list(i for i, j in data['files'].items() if 'stat' not in j)
            if find:
                LOG.warning('  manifest is missing %d file(s), aborting:', len(find))
                for item in find:
                    LOG.warning('    %s', item)
                return False

            find = list(i for i, j in data['files'].items() if j['size'] != j['stat'].st_size)
            if find:
                LOG.warning('  manifest has %d invalid file(s), aborting:', len(find))
                for item in find:
                    LOG.warning('    %s (expected: %d bytes, observed: %d bytes)', item, data['files'][item]['size'], data['files'][item]['stat'].st_size)
                return False

            find = list(i for i, j in data['files'].items() if j['stat'].st_mtime > last)
            if not find and os.stat(self.fullname).st_mtime < last:
                LOG.info( '  manifest has no updates, skipping')
                return True

            os.chdir(os.path.dirname(self.fullname))
            for item in cmds:
                try:
                    subprocess.check_call([item, self.fullname])
                    LOG.info('  executed dataset manifest command: %s %s', item, self.fullname)
                except (AttributeError, TypeError, subprocess.CalledProcessError) as e:
                    # FIXME: do something when command fails to allow retries
                    happy.log_error(e)
            return True
        except Exception as e:
            happy.log_error(e)
