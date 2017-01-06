import errno
import fnmatch
import happy.sync_file
import logging
import os
import pickle
import shutil

LOG = logging.getLogger()


def setup_local(index, mirror, unpack):
#    import sys
#    setattr(sys.modules['__main__'], 'SyncFile', happy.sync_file.SyncFile)

    local = {}

    LOG.debug('reading local index: %s', index)
    try:
        for key, val in pickle.load(open(index)).items():
            if val.mirror != mirror:
                LOG.warning('detected mirror directory move from %s to %s', val.mirror, mirror)
                val.mirror = mirror
            if val.unpack != unpack:
                LOG.warning('detected unpack directory move from %s to %s', val.unpack, unpack)
                val.unpack = unpack

            if not val.modified:
                local[key] = val
            else:
                LOG.warning('file changed or disappeared: %s', key)
    except Exception as e:
        if getattr(e, 'errno', None) == errno.ENOENT:
            LOG.warning('no local index available, performing full fetch')
        else:
            happy.log_error(e)

    LOG.info('loaded local index containing %d items', len(local))
    return local


def setup_avail(client, source, filter, mirror, unpack):
    avail = {}

    LOG.debug('fetching file list')
    for item in client.ls(source, recurse=True):
        if item.name.endswith('_COPYING_'):
            LOG.debug('skipping transferring hdfs object: %s', item.full)
            continue
        if item.is_dir():
            LOG.debug('skipping directory: %s', item.full)
            continue

        try:
            for find in filter:
                if fnmatch.fnmatch(item.full[len(source) + 1:], find):
                    LOG.info('queueing hdfs object: %s', item.full)
                    avail[item.full] = happy.sync_file.SyncFile(item, source, mirror, unpack)
                    break
            else:
                LOG.debug('skipping excluded hdfs object: %s', item.full)
        except Exception as e:
            happy.log_error(e)

    LOG.info('read remote list containing %d items', len(avail))
    return avail


def clean_local(index, local, mirror, unpack, skip=False):
    mirrored = list(i.fullname for i in local.values())
    unpacked = list(i.zip_path for i in local.values() if i.zip_path)

    for path, paths, files in os.walk(mirror, topdown=False):
        for name in files:
            full = '%s/%s' % (path, name)
            if full not in mirrored:
                LOG.info('removing orphaned local file: %s', full)
                if not skip:
                    os.unlink(full)

        if not skip:
            for name in paths:
                full = '%s/%s' % (path, name)
                try:
                    os.rmdir(full)
                    LOG.info('removed orphaned local empty directory: %s', full)
                except OSError as e:
                    if e.errno != errno.ENOTEMPTY:
                        raise e

    for path, paths, files in os.walk(unpack, topdown=False):
        for name in paths:
            full = '%s/%s' % (path, name)
            for arch in unpacked:
                same = os.path.commonprefix([full, arch])
                if same == arch or same == full:
                    paths.remove(name)
                    break
            else:
                LOG.info('removing orphaned unpacked local directory: %s', full)
                if not skip:
                    shutil.rmtree(full)
