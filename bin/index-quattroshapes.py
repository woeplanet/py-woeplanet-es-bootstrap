#!/usr/bin/env python

import fiona
import json
import logging
import os
import sys

from elasticsearch import Elasticsearch, NotFoundError, helpers

import woeplanet.elasticsearch

class QuattroshapesIndexer(object):
    def __init__(self, opts):
        self.opts = opts
        self.es = Elasticsearch([self.opts.es])
        self.search = woeplanet.elasticsearch.Search(es=self.es)
        self.index = 'woeplanet'
        self.source = None
        self.provider = None

        ll = logging.getLogger('elasticsearch')
        ll.setLevel(logging.ERROR)

    def index_qs(self, path):
        file_list = []

        for root, _, files in os.walk(path):
            for file in files:
                if '.zip' in file:
                    file_list.append(os.path.join(root, file))

        admin0 = '%s/qs_adm0.zip' % path
        admin1_region = '%s/qs_adm1_region.zip' % path
        admin1 = '%s/qs_adm1.zip' % path
        admin2_region = '%s/qs_adm2_region.zip' % path
        admin2 = '%s/qs_adm2.zip' % path
        localadmin = '%s/qs_localadmin.zip' % path
        localities = '%s/gn-qs_localities.zip' % path
        neighbourhoods = '%s/qs_neighborhoods.zip' % path
        gazetteer = '%s/gazetteer/quattroshapes_gazetteer_gn_then_gp.zip' %path

        if admin0 in file_list:
            self.parse_shapefile(admin0)

        if admin1_region in file_list:
            self.parse_shapefile(admin1_region)
        
        if admin1 in file_list:
            self.parse_shapefile(admin1)
        
        if admin2_region in file_list:
            self.parse_shapefile(admin2_region)
        
        if admin2 in file_list:
            self.parse_shapefile(admin2)
        
        if localadmin in file_list:
            self.parse_shapefile(localadmin)
        
        if localities in file_list:
            self.parse_shapefile(localities)
        
        if neighbourhoods in file_list:
            self.parse_shapefile(neighbourhoods)
        
        if gazetteer in file_list:
            self.parse_shapefile(gazetteer)

    def parse_shapefile(self, fname):
        # with fiona.Env():
        logging.info('parse %s' % fname)
        zipfile = 'zip://%s' % fname
        docs = []
        # counter = 0

        fc = fiona.open(zipfile)
        for feat in fc:
            props = feat['properties']
            logging.info('(%s) %s:%s' % (props['qs_woe_id'], props['qs_iso_cc'], props['qs_adm0']))
            logging.info(feat['properties'])

            if 'qs_woe_id' in props and props['qs_woe_id']:
                doc = self.search.get_by_woeid(props['qs_woe_id'])
                if not doc:
                    logging.warning('Cannot find woeid %s' % props['qs_woe_id'])

            if 'qs_woe_id' in feat['properties']:
                woeid = feat['properties']['qs_woe_id']
                if woeid == None:
                    logging.info('empty woeid')
            
                elif woeid <= 0:
                    logging.info('%s:(invalid)' % woeid)
            
                else:
                    doc = self.get_by_woeid(woeid)
                    if not doc:
                        logging.warning('WTF ... no record for WOEID (%s)' % woeid)
                        logging.info(json.dumps(feat['properties']))
                        continue
            
                    else:
                        docs.append({
                            '_index': self.index,
                            '_id': woeid,
                            '_op_type': 'update',
                            'doc': doc,
                            'doc_as_upsert': True
                        })

    def add_geometry(self, doc, feature):
        pass

    def _add(self, docs):
        helpers.bulk(self.es, docs, request_timeout=60)

    def get_by_woeid(self, woeid):
        try:
            rsp = self.es.get(self.index, woeid)
            if 'found' in rsp:
                return rsp['_source']
            else:
                return None

        except NotFoundError as _:
            return None

if __name__ == '__main__':
    import optparse

    parser = optparse.OptionParser("""index-quattroshapes.py --options <quattroshapes data path>""")
    parser.add_option("-e", "--elasticsearch", dest="es", help="your ES endpoint; default is localhost:9200", default='localhost:9200')
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    i = QuattroshapesIndexer(opts)

    if len(args) == 0:
        logging.error('You forgot to point to the QuattroShapes directory!')
        sys.exit()

    for path in args:
        logging.info("processing %s" %path)
        i.index_qs(path)
