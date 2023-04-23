#!/usr/bin/env python
"""
Index and load Yahoo! GeoPlanet Place Types into Elasticsearch
"""

import json
import logging
import os
import sys

from elasticsearch import Elasticsearch, helpers


class PlaceTypesIndexer:
    """
    GeoPlanet Place Types to Elasticsearch indexer thing
    """
    def __init__(self, config):
        self.config = config
        self.esclient = Elasticsearch([self.config.es])
        self.index = 'placetypes'

    def purge(self):
        """
        Purge indices
        """

        logging.info("Purging/deleting index: %s", self.index)
        if self.esclient.indices.exists(index=self.index):
            self.esclient.indices.delete(index=self.index)

    def index_placetypes(self, file_path):
        """
        Parse and index placetypes
        """

        datadir = f'{file_path}/data'
        logging.debug("Indexing %s", datadir)

        docs = []
        counter = 0
        for root, _, files in os.walk(datadir):
            for file in files:
                if '.json' in file:
                    placefile = os.path.join(root, file)
                    logging.info("Loading %s", placefile)
                    with open(placefile, encoding='UTF-8') as ifh:
                        doc = json.load(ifh)
                        doc['_index'] = self.index
                        doc['_id'] = doc['id']
                        docs.append(doc)
                        counter = counter + 1

        if docs:
            helpers.bulk(self.esclient, docs)

        logging.info("Placetypes added %s docs", counter)
        logging.info("Finished indexing %s", datadir)


if __name__ == '__main__':
    import optparse

    parser = optparse.OptionParser(
        """index-placetypes.py --options <woeplanet-data-placetypes path>"""
    )
    parser.add_option(
        "-e",
        "--elasticsearch",
        dest="es",
        help="your ES endpoint; default is localhost:9200",
        default='localhost:9200'
    )
    parser.add_option(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        help="enable chatty logging; default is false",
        default=False
    )
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
        logging.info("Processing %s", path)
        pti.index_placetypes(path)
