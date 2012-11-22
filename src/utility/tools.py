#!/usr/bin/env python

import os,yaml, glob, re

def parserMap(mapfile):
    """Parse the map file and return a list that contains plans."""
    if mapfile is None or not os.path.isfile(mapfile):
        sys.exit("ERROR: No such file: ' %s'."%mapfile )
    try:
        f = open(mapfile)
        plans = yaml.load(f)
        f.close()
    except IOError:
        sys.exit("ERROR: Open map file: '%s' failed."%mapfile)
    return plans

def set_search_path(schema, sqlDir):
    for template in glob.glob(sqlDir + '*.sql.template'):
        fp = open(template)
        statements = fp.read()
        fp.close()
        fp = open(template[:template.find('.template')], 'w')
        statements = re.sub(r'\$\$\$benchmark\$\$\$','%s'%schema ,statements)
        fp.write(statements)
        fp.close()
