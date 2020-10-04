#!/usr/bin/env python

import datetime
import json
import logging
import os
import pygeohash
import shapely.geometry
import sys
import re

from woeplanet.docs.placetypes import PlaceTypes

from elasticsearch import Elasticsearch, NotFoundError, helpers

class WhereOnEarthImporter(object):
    def __init__(self, opts):
        self.opts = opts
        self.es = Elasticsearch([self.opts.es], timeout=30, max_retries=10, retry_on_timeout=True)
        self.version = 'whereonearth 0.x'
        self.update_count = opts.batch_size
        self.index = opts.index
        self.placetypes = PlaceTypes(self.es)
        self.source = None
        self.docs = []
        self.counter = 0
        self.version = '0.0.0'
        self.provider = None

        ll = logging.getLogger('elasticsearch')
        ll.setLevel(logging.ERROR)

    def index_geojson(self, path):
        datadir = "%s/data" % path
        logging.debug("Indexing %s" % datadir)

        for root, _, files in os.walk(datadir):
            for file in files:
                if '.geojson' in file or '.json' in file:
                    srcfile = os.path.join(root, file)
                    match = re.search(r'(whereonearth-[^/]*)', srcfile)
                    if not match:
                        logging.warning('This file doesn\'t smell like WhereOnEarth GeoJSON: %s' % srcfile)
                    else:
                        self.source = match.group(1)
                        with open(srcfile) as fh:
                            src = json.load(fh)

                            if 'type' in src and src['type'] == 'Feature':
                                self.index_feature(src, srcfile)

                            elif 'type' in src and src['type'] == 'FeatureCollection':
                                for feature in src['features']:
                                    self.index_feature(feature, srcfile)

                            else:
                                logging.warning('This file doesn\t smell like GeoJSON: %s' % srcfile)

        if len(self.docs):
            logging.info("places %s final counter @ %s" % (self.version, self.counter))
            self.counter += len(self.docs)
            self._add(self.docs)
            self.docs = []

        logging.info("Added %s GeoJSON features" % self.counter)
        logging.info("Finished indexing %s" % datadir)

    def index_feature(self, feature, srcfile):
        props = feature['properties']
        if 'woe:id' in props:
            woeid = props['woe:id']
        elif 'woeid' in props:
            woeid = props['woeid']
        else:
            logging.warning('Can\'t work out woeid property: %s' % srcfile)
            return

        logging.debug('Handling %s' % woeid)

        doc = self.woeify_properties(props, srcfile)

        # Yes, we really *do* have to check for this. Sigh
        if 'geometry' in feature:
            geom = shapely.geometry.shape(feature['geometry'])
            if geom.geom_type != 'MultiPolygon':
                logging.debug('woeid %s geometry is %s (converting to MultiPolygon)' % (doc['woe:id'], geom.geom_type))
                mapping = shapely.geometry.mapping(shapely.geometry.multipolygon.MultiPolygon([geom]))
            else:
                mapping = shapely.geometry.mapping(geom)

            if not geom.is_valid:
                logging.warning('woeid %s: invalid/self-intersecting geometry! Trying to clean ...' % woeid)
                clean = geom.buffer(0)
                if not clean.is_valid:
                    logging.warning('woeid %s: post clean - invalid/self-intersecting geometry!' % woeid)
                else:
                    logging.info('woeid %s: cleaned geometry successfully' % woeid)
                    mapping = shapely.geometry.mapping(clean)

            doc['geometry'] = {
                'type': mapping['type'],
                'coordinates': mapping['coordinates']
            }

        doc = self.merge_doc(doc)

        self.docs.append({
            '_index': self.index,
            '_id': woeid,
            '_op_type': 'update',
            'doc': doc,
            'doc_as_upsert': True
        })

        if len(self.docs) == self.update_count:
            logging.info("places %s counter @ %s" % (self.version, self.counter))
            self.counter += len(self.docs)
            self._add(self.docs)
            self.docs = []

    def woeify_properties(self, properties, srcfile):
        doc = {}

        doc['meta:provider'] = '%s:%s' % (self.source, self.version)

        # whereonearth-airport
        # whereonearth-country:
        # whereonearth-state:
        # whereonearth-timezone:
        # whereonearth-town:
        #   woeid -> woe:id

        woeid = properties.get('woe:id', None)
        if woeid == None:
            woeid = properties.get('woeid', None)
            if woeid:
                doc['woe:id'] = int(woeid)
                properties.pop('woeid', None)
            else:
                logging.warning('Can\'t work out woeid property: %s' % srcfile)
                return None
        else:
            doc['woe:id'] = int(woeid)

        # whereonearth-town:
        #    parent_woeid -> woe:parent_id
        parent = properties.get('parent_woeid', None)
        if parent:
            doc['woe:parent_id'] = int(parent)
            properties.pop('parent_woeid', None)

        # whereonearth-airport
        # whereonearth-country:
        # whereonearth-state:
        # whereonearth-timezone:
        # whereonearth-town:
        #   lang -> woe:lang
        lang = properties.get('lang', None)
        if lang:
            doc['woe:lang'] = lang
            properties.pop('lang', None)

        # whereonearth-town:
        #   adjacent_woeid -> woe:adjacent
        # whereonearth-state:
        #   adjacent -> woe:adjacent
        #   adjacent_woeid -> woe:adjacent
        adjacent = []
        adj = properties.get('adjacent_woeid', None)
        if adj:
            adjacent += adj
            # adjacent.append(adj)
            properties.pop('adjacent_woeid', None)

        adj = properties.get('adjacent', None)
        if adj:
            for placestr in adj:
                place = placestr.split('=')
                adjacent.append(int(place[1]))

            properties.pop('adjacent', None)

        if adjacent:
            doc['woe:adjacent'] = adjacent

        # whereonearth-airport
        # whereonearth-country:
        # whereonearth-state:
        # whereonearth-timezone:
        # whereonearth-town:
        #   name -> woe:name

        name = properties.get('name', None)
        if name:
            doc['woe:name'] = name
            properties.pop('name', None)

        # whereonearth-airport:
        # whereonearth-country:
        # whereonearth-state:
        # whereonearth-timezone:
        # whereonearth-town:
        #   iso -> iso:country
        iso = properties.get('iso', None)
        if iso:
            doc['iso:country'] = iso
            properties.pop('iso', None)

        # whereonearth-airport:
        # whereonearth-country:
        # whereonearth-state:
        # whereonearth-timezone:
        # whereonearth-town:
        #    longitude -> woe:latitude
        #    latitude -> woe:longitude
        #    ne_longitude -> woe:max_longitude
        #    ne_latitude -> woe:max_latitude
        #    sw_longitude -> woe:min_longitude
        #    sw_latitude -> woe:max_longitude
        lon = properties.get('longitude', None)
        if lon:
            doc['woe:longitude'] = doc['geom:longitude'] = float(lon)
            properties.pop('longitude', None)

        lat = properties.get('latitude', None)
        if lat:
            doc['woe:latitude'] = doc['geom:latitude'] = float(lat)
            properties.pop('latitude', None)

        nelon = properties.get('ne_longitude', None)
        if nelon:
            doc['woe:max_longitude'] = doc['geom:max_longitude'] = float(nelon)
            properties.pop('ne_longitude', None)

        nelat = properties.get('ne_latitude', None)
        if nelat:
            doc['woe:max_latitude'] = doc['geom:max_latitude'] = float(nelat)
            properties.pop('ne_latitude', None)

        swlon = properties.get('sw_longitude', None)
        if swlon:
            doc['woe:min_longitude'] = doc['geom:min_longitude'] = float(swlon)
            properties.pop('sw_longitude', None)

        swlat = properties.get('sw_latitude', None)
        if swlat:
            doc['woe:max_latitude'] = doc['geom:max_latitude'] = float(swlat)
            properties.pop('sw_latitude', None)

        if all (key in doc for key in ('geom:latitude', 'geom:longitude')):
            doc['geom:centroid'] = [
                doc['geom:longitude'],
                doc['geom:latitude']
            ]
            doc['geom:hash'] = pygeohash.encode(doc['geom:latitude'], doc['geom:longitude'])
            doc['woe:centroid'] = doc['geom:centroid']
            doc['woe:hash'] = doc['geom:hash']

        if all (key in doc for key in ('geom:min_latitude', 'geom:min_longitude', 'geom:max_latitude', 'geom:max_longitude')):
            if doc['geom:min_longitude'] != doc['geom:max_longitude'] and doc['geom:min_latitude'] != doc['geom:max_latitude']:
                doc['geom:bbox'] = [
                    doc['geom:min_longitude'], doc['geom:min_latitude'],
                    doc['geom:max_longitude'], doc['geom:max_latitude']
                ]
                doc['woe:bbox'] = doc['geom:bbox']

        # whereonearth-airport:
        # whereonearth-country:
        # whereonearth-state:
        # whereonearth-timezone:
        # whereonearth-town:
        #   placetype -> woe:placetype, woe:placetype_name
        placetype = properties.get('placetype', None)
        if placetype:
            pt = self.placetypes.by_name(placetype)
            if pt:
                doc['woe:placetype_name'] = pt['shortname']
                doc['woe:placetype'] = pt['id']
                properties.pop('placetype', None)

        # whereonearth-airport:
        # whereonearth-country:
        # whereonearth-state:
        #   hierarchy - > woe:hierarchy

        h = properties.get('hierarchy', None)
        hierarchy = {}
        if h:
            for place in h:
                match = re.search(r'^(woe):(.*)=([0-9]{1,})$', place)
                if match:
                    pt = match.group(2)
                    id = int(match.group(3))
                    if pt == 'planet':
                        hierarchy['planet'] = id
                    elif pt == 'country':
                        hierarchy['country'] = id
                    elif pt == 'continent':
                        hierarchy['continent'] = id
                    elif pt == 'county':
                        hierarchy['county'] = id
                    elif pt == 'state':
                        hierarchy['state'] = id
                    elif pt == 'continent':
                        hierarchy['continent'] = id
                    elif pt == 'localadmin':
                        hierarchy['localadmin'] = id
            if hierarchy:
                doc['woe:hierarchy'] = hierarchy

            properties.pop('hierarchy', None)

        # whereonearth-airport:
        # whereonearth-country:
        # whereonearth-state:
        #   alias_*_* -> woe:alias_*_*

        for key in list(properties):
            if re.match(r'^alias_[A-Z]{3}_[A-Z]{1}$', key):
                alias = 'woe:%s' % key
                doc[alias] = properties[key]
                properties.pop(key, None)

        # whereonearth-airport:
        #   iata:code -> woe:concordances

        iata = properties.get('iata:code', None)
        if iata:
            doc['woe:concordances'] = {
                'iata:code': iata
            }

        # whereonearth-country:
        # wikipedia:id  -> woe:concordances

        wikipedia = properties.get('wikipedia:id', None)
        if wikipedia and iata:
            doc['woe:corcordances']['wikipedia:id'] = int(wikipedia)
        elif wikipedia:
            doc['woe:concordances'] = {
                'wikipedia:id': int(wikipedia)
            }

        return doc

    def merge_doc(self, update):
        woeid = update['woe:id']
        doc = self._get_by_woeid(woeid)
        if not doc:
            raise Exception('Cannot find existing document for woeid:%s' % woeid)

        logging.debug('DOC DUMP FOR %d' % woeid)
        for key, value in update.items():
            logging.debug('%s -> %s' % (key, json.dumps(value)))

            if key == 'meta:provider':
                if not value in doc[key]:
                    doc[key].append(value)

            elif key == 'woe:hierarchy':
                if key in doc:
                    for ckey, cvalue in value.items():
                        doc[key][ckey] = cvalue
                else:
                    doc[key] = value

            elif re.match(r'^woe:alias_[A-Z]{3}_[A-Z]{1}$', key):
                if key in doc:
                    doc[key] = list(set().union(doc[key], value))

                else:
                    doc[key] = value

            elif key == 'woe:adjacent':
                if key in doc:
                    doc[key] = list(set().union(doc[key], value))

                else:
                    doc[key] = value

            elif key == 'woe:concordances':
                if key in doc:
                    for ckey, cvalue in value.items():
                        doc[key][ckey] = cvalue
                else:
                    doc[key] = value

            else:
                doc[key] = value

        doc['meta:updated'] = str(datetime.datetime.utcnow().isoformat())
        return doc

    def _get_by_woeid(self, woeid):
        try:
            rsp = self.es.get(self.index, woeid)
            if 'found' in rsp:
                return rsp['_source']
            else:
                return None

        except NotFoundError as _:
            return None
        
    def _add(self, docs):
        helpers.bulk(self.es, docs, request_timeout=60)
        # pass

def main():
    parser = argparse.ArgumentParser(prog='index-whereonearth', description='Imports WhereOnEarth GeoJSON files into Elasticsearch')
    parser.add_argument('-e', '--elasticsearch', dest='es', metavar='URL', help='your ES endpoint; default is localhost:9200', default='localhost:9200')
    parser.add_argument('-i', '--index', dest='index', action='store_true', help='WoePlanet index name; default is woeplanet', default='woeplanet')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='enable chatty logging; default is false', default=False)
    parser.add_argument('-b', '--batch-size', dest='batch_size', type=int, help='set indexing batchsize; default is 10,000', default=10000)
    parser.add_argument('paths', nargs='*')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    woei = WhereOnEarthImporter(args)

    if len(args.paths) == 0:
        logging.error('You forgot to point to one or more WhereOnEarth data repos!')
        sys.exit()

    for path in args.paths:
        logging.info("Processing %s" %path)
        woei.index_geojson(path)

if __name__ == '__main__':
    import argparse

    main()