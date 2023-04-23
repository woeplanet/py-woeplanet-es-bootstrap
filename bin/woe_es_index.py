#!/usr/bin/env python
"""
Load and index WOE GeoJSON files or directories in Elasticsearch
"""

import json
import logging
import os
import sys

from elasticsearch import Elasticsearch, helpers


class Indexer:
    """
    GeoJSON to Elasticsearch indexer thing
    """
    def __init__(self, config):
        self.config = config
        self.esclient = Elasticsearch(
            [self.config.es],
            timeout=30,
            max_retries=10,
            retry_on_timeout=True
        )
        self.update_count = config.batch_size
        self.index = config.index
        # self.placetypes = PlaceTypes(self.es)
        # self.source = None
        self.docs = []
        self.counter = 0
        # self.version = '0.0.0'
        # self.provider = None

        logger = logging.getLogger('elasticsearch')
        logger.setLevel(logging.ERROR)

    def do_index(self, path):
        """
        Recursively index a path's GeoJSON files
        """
        if not os.path.exists(path):
            raise RuntimeError(f'{path}: no such file or directory')

        elif os.path.isdir(path):
            for root, _, files in os.walk(path):
                for file in files:
                    if '.geojson' in file:
                        srcfile = os.path.join(root, file)
                        self.index_geojson(srcfile)

        else:
            self.index_geojson(path)

        if self.docs:
            logging.info("places %s final counter @ %s", path, self.counter)
            self.counter += len(self.docs)
            self._add(self.docs)

    def index_geojson(self, path):
        """
        Index a GeoJSON file
        """

        with open(path, 'r', encoding='UTF-8') as reader:
            raw = json.load(reader)
            if 'properties' in raw:
                doc = raw['properties']
                woeid = int(doc['woe:id'])
                if 'geometry' in raw:
                    doc['geometry'] = raw['geometry']
                    if doc['geometry']['type'] not in ['Point', 'Polygon', 'MultiPolygon']:
                        logging.warning(
                            '%s: unexpected geometry type %s in %s',
                            woeid,
                            doc['geometry']['type'],
                            path
                        )
                        if doc['geometry']['type'] == 'point':
                            doc['geometry']['type'] = 'Point'

                self.docs.append(
                    {
                        '_index': self.index,
                        '_id': woeid,
                        '_op_type': 'update',
                        'doc': doc,
                        'doc_as_upsert': True
                    }
                )

            else:
                logging.warning('missing properties in %s', path)

        if len(self.docs) == self.update_count:
            logging.info("places %s counter @ %s", path, self.counter)
            self.counter += len(self.docs)
            self._add(self.docs)

    def _add(self, docs):
        helpers.bulk(self.esclient, docs, request_timeout=60)
        self.docs = []


def main():
    """
    Script main entry point
    """

    parser = argparse.ArgumentParser(
        prog='woe-es-index',
        description='Load and index WOE GeoJSON files or directories in Elasticsearch'
    )
    parser.add_argument(
        "-e",
        "--elasticsearch",
        dest="es",
        help="your ES endpoint; default is localhost:9200",
        default='localhost:9200'
    )
    parser.add_argument(
        '-i',
        '--index',
        dest='index',
        action='store_true',
        help='WoePlanet index name; default is woeplanet',
        default='woeplanet'
    )
    parser.add_argument(
        '-b',
        '--batch-size',
        dest='batch_size',
        type=int,
        help='set indexing batchsize; default is 10,000',
        default=10000
    )
    parser.add_argument(
        '-v',
        '--verbose',
        dest='verbose',
        action='store_true',
        help='enable chatty logging; default is false',
        default=False
    )
    parser.add_argument('paths', nargs='*')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if len(args.paths) == 0:
        logging.error('You forgot to point to one or more source directories or files!')
        sys.exit()

    indexer = Indexer(args)
    for path in args.paths:
        try:
            indexer.do_index(path)

        except Exception as exc:
            logging.error(exc)

    # try:
    #     for path in args.paths:
    #         if not os.path.exists(path):
    #             logging.error('%s: no such file or directory' % path)

    #         elif os.path.isdir(path):
    #             for root, _, files in os.walk(path):
    #                 for file in files:
    #                     if '.geojson' in file:
    #                         srcfile = os.path.join(root, file)
    #                         index_geojson(srcfile, args)

    #         else:
    #             index_geojson(path, args)

    # except Exception as e:
    #     logging.error(e)


# def index_geojson(path, args):
#     pass

if __name__ == '__main__':
    import argparse

    main()
