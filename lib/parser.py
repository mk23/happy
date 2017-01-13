import csv
import json
import logging
import os
import sys
import xml.etree.cElementTree as et
import yaml

LOG = logging.getLogger()

def parse(name):
    kind = os.path.splitext(name)[-1][1:].lower()
    func = getattr(sys.modules[__name__], 'parse_%s' % kind)

    if func:
        LOG.debug('parsing %s using %s', name, kind)
        return func(name)
    else:
        raise RuntimeError('%s: unknown dataset format')

def parse_json(name):
    return json.load(open(name)).get('files', [])

def parse_yaml(name):
    return yaml.load(open(name)).get('files', [])

def parse_yml(name):
    return yaml.load(open(name)).get('files', [])

def parse_xml(name):
    return dict((i.attrib['name'], {'size': int(i.attrib['size'])}) for i in et.parse(name).findall('.//files/file'))

def parse_csv(name):
    return dict((i['name'], {'size': int(i['size'])}) for i in csv.DictReader(open(name)))

def parse_tsv(name):
    return dict((i['name'], {'size': int(i['size'])}) for i in csv.DictReader(open(name), dialect='excel-tab'))
