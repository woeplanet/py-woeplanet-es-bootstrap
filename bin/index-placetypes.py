#!/usr/bin/env python

import json
import logging
import os
import sys

from elasticsearch import Elasticsearch, NotFoundError, helpers

class PlaceTypesIndexer(object):
    def __init__(self, opts):
        self.opts = opts
        self.es = Elasticsearch([self.opts.es])
        self.index = 'placetypes'

    def purge(self):
        logging.info("Purging/deleting index: %s" % self.index)
        if self.es.indices.exits(index=self.index):
            sef.es.indices.delete(index=self.index)

    def index_placetypes(self, path):
        datadir = "%s/data" % path
        logging.debug("Indexing %s" % datadir)

        docs = []
        counter = 0
        for root, dirs, files in os.walk(datadir):
            for file in files:
                if '.json' in file:
                    placefile = os.path.join(root, file)
                    logging.info("Loading %s" % placefile)
                    with open(placefile) as pf:
                        doc = json.load(pf)
                        doc['_index'] = self.index
                        doc['_id'] = doc['id']
                        docs.append(doc)
                        counter = counter + 1

        if len(docs):
            helpers.bulk(self.es, docs)

        logging.info("Placetypes added %s docs" % counter)
        logging.info("Finished indexing %s" % datadir)

if __name__ == '__main__':
    import optparse

    parser = optparse.OptionParser("""index-placetypes.py --options <woeplanet-data-placetypes path>""")
    parser.add_option("-e", "--elasticsearch", dest="es", help="your ES endpoint; default is localhost:9200", default='localhost:9200')
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)
    parser.add_option("--purge", dest="purge", action="store_true", help="...", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    pti = PlaceTypesIndexer(opts)

    if opts.purge:
        pti.purge()

    if len(args) == 0:
        logging.error('You forgot to point to the woeplanet-data-placetypes repo!')
        sys.exit()

    for path in args:
        logging.info("Processing %s" %path)
        pti.index_placetypes(path)
