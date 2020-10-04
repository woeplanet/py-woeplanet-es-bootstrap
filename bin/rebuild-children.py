#!/usr/bin/env python

import datetime
import json
import logging
import os
import sys

from elasticsearch import Elasticsearch, NotFoundError, helpers

class RelationshipBuilder(object):
    def __init__(self, args):
        self.args = args
        self.es = Elasticsearch([self.args.es], timeout=30, max_retries=10, retry_on_timeout=True)
        self.index = 'woeplanet'
        self.scroll_size = 10000
        self.update_count = self.args.batch_size
        self.docs = []
        self.counter = 0

        ll = logging.getLogger('elasticsearch')
        ll.setLevel(logging.ERROR)

        ll = logging.getLogger('urllib3.connectionpool')
        ll.setLevel(logging.ERROR)

    def build(self):
        query = {
            'query': {
                'bool': {
                    'must_not': [
                        {
                            'exists': {
                                'field': 'woe:superseded_by'
                            }
                        }
                    ]
                }
            }
        }

        rsp = helpers.scan(self.es, query=query, index=self.index, scroll='5m', size=self.scroll_size)
        for _, hit in enumerate(rsp):
            doc = hit['_source']
            woeid = int(doc['woe:id'])
            logging.debug('woeid:%s: finding children ...' % woeid)
            children = self.get_children(woeid)
            if children:
                doc['woe:children'] = children
                doc['meta:updated'] = str(datetime.datetime.utcnow().isoformat())

                self.docs.append({
                    '_index': self.index,
                    '_id': woeid,
                    '_op_type': 'update',
                    'doc': doc,
                    'doc_as_upsert': True
                })

            if len(self.docs) == self.update_count:
                logging.info('Updating relationships @ %s' % self.counter)
                self.counter += len(self.docs)
                self._add()

        if len(self.docs):
            logging.info('Updating relationships @ %s' % self.counter)
            self.counter += len(self.docs)
            self._add()

    def get_children(self, woeid):
        query = {
            '_source': ['woe:id', 'woe:placetype_name'],
            'query': {
                'bool': {
                    'must': [{
                        'match': {
                            'woe:parent_id': woeid
                        }
                    }],
                    'must_not': [{
                        'exists': {
                            'field': 'woe:superseded_by'
                        }
                    }]
                }
            }
        }

        children = {}

        rsp = helpers.scan(self.es, query=query, index=self.index, scroll='5m', size=self.scroll_size)
        count = 0
        for num, doc in enumerate(rsp):
            source = doc['_source']
            # logging.debug('%d: checking child %s' % (num, source['woe:id']))
            # if 'woe:superseded_by' in source and source['woe:superseded_by'] != 0:
            #     logging.debug('%s: skipping doc with superseded_by' % source['woe:id'])
            #     pass

            # elif not 'woe:name' in source:
            #     logging.warning('%s: no woe:name property' % source['woe:id'])

            # else:
                # logging.debug('woeid %s: child %s (%s/%s)' % (woeid, source['woe:id'], source['woe:name'], source['woe:placetype_name']))
            placetype = source['woe:placetype_name'].lower()
            cpt = children.get(placetype, [])
            cpt.append(int(source['woe:id']))
            count += 1
            children[placetype] = cpt

        if count:
            logging.debug('woeid:%s: found %d children' % (woeid, count))

        return children

    def _add(self):
        helpers.bulk(self.es, self.docs, request_timeout=60)
        self.docs = []

def main():
    parser = argparse.ArgumentParser(prog='rebuild-children', description='Rebuild WoePlanet parent/child relationships')
    parser.add_argument("-e", "--elasticsearch", dest="es", help="your ES endpoint; default is localhost:9200", default='localhost:9200')
    parser.add_argument('-i', '--index', dest='index', action='store_true', help='WoePlanet index name; default is woeplanet', default='woeplanet')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='enable chatty logging; default is false', default=False)
    parser.add_argument('-b', '--batch-size', dest='batch_size', type=int, help='set indexing batchsize; default is 10,000', default=10000)

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    builder = RelationshipBuilder(args)
    builder.build()

if __name__ == '__main__':
    import argparse

    main()