#!/usr/bin/env python

import datetime
import geojson_rewind
import json
import logging
import os.path
import pygeohash
import re
import shapely.geometry
import sys
import tarfile

from elasticsearch import Elasticsearch, NotFoundError, helpers
import shapely.geometry
from woeplanet.docs.placetypes import PlaceTypes

class FlickrShapesIndexer(object):
    def __init__(self, opts):
        self.opts = opts
        self.es = Elasticsearch([self.opts.es])
        self.tarball = None
        self.version = None
        self.update_count = 100000
        self.index = 'woeplanet'
        self.source = None
        self.provider = None
        self.placetypes = PlaceTypes(self.es)

        ll = logging.getLogger('elasticsearch')
        ll.setLevel(logging.ERROR)

    def index_tgzfile(self, path):
        pattern = re.compile(r"((flickr_shapes)_public_dataset)_([\d\.]+)\.tar.gz$")
        match = pattern.match(os.path.basename(path))

        if not match:
            logging.error('failed to match source/version number!')
            return False

        groups = match.groups()
        self.source = groups[1]
        self.version = groups[2]
        self.provider = '%s:%s' % (self.source, self.version)

        continents = '%s_continents.geojson' % self.source
        countries = '%s_countries.geojson' % self.source
        regions = '%s_regions.geojson' % self.source
        counties = '%s_counties.geojson' % self.source
        localities = '%s_localities.geojson' % self.source
        neighbourhoods = '%s_neighbourhoods.geojson' % self.source
        superseded = '%s_superseded.geojson' % self.source

        self.tarball = tarfile.open(path)
        file_list = []

        for fname in self.tarball.getnames():
            file_list.append(fname)

        if continents in file_list:
            self.parse_shape(continents)

        if countries in file_list:
            self.parse_shape(countries)

        if regions in file_list:
            self.parse_shape(regions)

        if counties in file_list:
            self.parse_shape(counties)

        if localities in file_list:
            self.parse_shape(localities)

        if neighbourhoods in file_list:
            self.parse_shape(neighbourhoods)

        if superseded in file_list:
            self.parse_superseded(superseded)

        self.tarball.close()
        return True

    def parse_shape(self, fname):
        logging.info('parse %s %s' % (fname, self.version))

        fh = self.tarball.extractfile(fname)
        data = json.load(fh)
        docs = []
        counter = 0

        for feature in data['features']:
            woeid = feature['id']
            doc = self.get_by_woeid(woeid)
            if not doc:
                logging.warning('WTF ... no record for WOEID (%s)' % woeid)
                logging.info(json.dumps(feature))
                continue

            else:
                doc = self.add_geometry(doc, feature)

                if not 'woe:concordances' in doc:
                    doc['woe:concordances'] = {
                        'fs:id': feature['properties']['place_id']
                    }

                elif not 'fs:id' in doc['woe:concordances']:
                    doc['woe:concordances']['fs:id'] = feature['properties']['place_id']

                if not self.provider in doc['meta:provider']:
                    doc['meta:provider'].append(self.provider)

                doc['meta:updated'] = str(datetime.datetime.utcnow().isoformat())

                docs.append({
                    '_index': self.index,
                    '_id': woeid,
                    '_op_type': 'update',
                    'doc': doc,
                    'doc_as_upsert': True
                })

            if len(docs) == self.update_count:
                logging.info('%s %s counter @ %s' % (fname, self.version, counter))
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info('%s %s counter @ %s' % (fname, self.version, counter))
            counter += len(docs)
            self._add(docs)

        logging.info('%s %s updated %s geometries' % (fname, self.version, counter))
        fh.close()

    def parse_superseded(self, fname):
        logging.info('parse superseded %s' % fname)

        fh = self.tarball.extractfile(fname)
        data = json.load(fh)
        docs = []
        counter = 0

        for feature in data['features']:
            old_woeid = int(feature['properties']['woe_id'])
            new_woeid = int(feature['properties']['superseded_by'])
            old = self.get_by_woeid(old_woeid)
            new = self.get_by_woeid(new_woeid)
            logging.debug('woeid:%s is superseded_by woeid:%s' % (old_woeid, new_woeid))

            if new and 'woe:name' in new:
                name = new['woe:name']
            else:
                name = feature['properties']['label'].split(',')[0]

            # This is basically the same code as parse_changes() in
            # index-geoplanet-data.py and should really be refactored into a
            # common function. Today is not that day.

            if old:
                logging.debug('old: %s superseded by %s' % (old_woeid, new_woeid))
                old['woe:superseded_by'] = new_woeid
                if not 'woe:name' in old:
                    old['woe:name'] = name

                if not 'woe:concordances' in old:
                    old['woe:concordances'] = {
                        'fs:id': feature['properties']['place_id']
                    }
                elif not 'fs:id' in old['woe:concordances']:
                    old['woe:concordances']['fs:id'] = feature['properties']['place_id']

                if not 'meta:provider' in old:
                    old['meta:provider'] = [ self.provider]
                else:
                    if not self.provider in old['meta:provider']:
                        old['meta:provider'].append(self.provider)

                old['meta:updated'] = str(datetime.datetime.utcnow().isoformat())
                docs.append({
                    '_index': self.index,
                    '_id': old['woe:id'],
                    '_op_type': 'update',
                    'doc': old,
                    'doc_as_upsert': True
                })
            else:
                # Urghh ... part the first
                doc = {
                    '_index': self.index,
                    '_id': old_woeid,
                    'woe:id': old_woeid,
                    'woe:name': name,
                    'woe:concordances': {
                        'fs:id': feature['properties']['place_id']
                    },
                    'meta:provider': [ self.provider ],
                    'woe:superseded_by': new_woeid,
                    'woe:placetype': 0,
                    'woe:placetype_name': self.placetypes.by_id(0)['shortname'],
                    'meta:indexed': str(datetime.datetime.utcnow().isoformat())
                }
                docs.append(doc)

            if new:
                supersedes = new.get('woe:supersedes', [])
                logging.debug('new: %s supersedes %s' % (new_woeid, old_woeid))

                if not 'woe:name' in new:
                    new['woe:name'] = name

                if not old_woeid in supersedes:
                    supersedes.append(old_woeid)
                    new['woe:supersedes'] = supersedes

                new['meta:updated'] = str(datetime.datetime.utcnow().isoformat())
                docs.append({
                    '_index': self.index,
                    '_id': new['woe:id'],
                    '_op_type': 'update',
                    'doc': new,
                    'doc_as_upsert': True
                })

            else:
                # Urghh ... part the second
                logging.warning('WTF ... no document for new/supersedes WOE ID (%s)' % new_woeid)
                doc = {
                    '_index': self.index,
                    '_id': new_woeid,
                    'woe:id': new_woeid,
                    'woe:name': name,
                    'woe:placetype': 0,
                    'woe:placetype_name': self.placetypes.by_id(0)['shortname'],
                    'woe:supersedes': [ old_woeid ],
                    'woe:concordances': {
                        'fs:id': feature['properties']['place_id']
                    },
                    'meta:provider': [ self.provider],
                    'meta:indexed': str(datetime.datetime.utcnow().isoformat())
                }
                docs.append(doc)

            if len(docs) == self.update_count:
                logging.info('%s %s counter @ %s' % (fname, self.version, counter))
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info('%s %s counter @ %s' % (fname, self.version, counter))
            counter += len(docs)
            self._add(docs)

        logging.info('%s %s updated %s superseded docs' % (fname, self.version, counter))
        fh.close()

    def add_geometry(self, doc, feature):
        geom = feature['geometry']
        geom = shapely.geometry.asShape(geom)
        centroid = geom.centroid
        lat = centroid.y
        lon = centroid.x

        # What really? Something triggers these errors:
        # Invalid latitude: latitudes are range -90 to 90: provided lat: [-267.734515144]
        # Invalid longitude: longitudes are range -180 to 180: provided lon: [-255.248708048]

        if lat >= -90 and lat <= 90 and lon >= -180 and lon <= 180:
            doc['fs:centroid'] = [lon, lat]
            doc['fs:latitude'] = lat
            doc['fs:longitude'] = lon
            doc['fs:hash'] = pygeohash.encode(doc['fs:latitude'], doc['fs:longitude'])

        bbox = feature.get('bbox', None)
        if not bbox:
            bbox = geom.bounds

        doc['fs:min_latitude'] = bbox[1]
        doc['fs:min_longitude'] = bbox[0]
        doc['fs:max_latitude'] = bbox[3]
        doc['fs:max_longitude'] = bbox[2]

        doc['fs:bbox'] = [doc['fs:min_longitude'], doc['fs:min_latitude'], doc['fs:max_longitude'], doc['fs:max_latitude']]

        # Skip even trying to import/fix/index/munge the Flickr boundaries; they're
        # full of self intersections, duplicate rings and other stuff which makes
        # Shapely/Geos go "waaah" ...

        # if feature['geometry']['type'] != 'MultiPolygon':
        #     raise Exception('woeid %s geometry is %s (should be MultiPolygon)' % (doc['woe:id'], feature['geometry']['type']))
        #
        # # Urggh ... there's got to be a better way of doing this
        # if not geom.is_valid:
        #     logging.warning('woeid %s has invalid/self-intersecting geometry' % doc['woe:id'])
        #     clean = geom.buffer(0)
        #     if clean.is_valid:
        #         logging.info('woeid %s fixed/cleaned invalid geometry' % doc['woe:id'])
        #         geom = clean
        #
        # m = shapely.geometry.mapping(geom)
        # doc['geometry'] = {
        #     'type': m['type'],
        #     'coordinates': json.loads(json.dumps(m['coordinates']))
        # }
        # doc['geometry'] = geojson_rewind.rewind(doc['geometry'])

        return doc

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

    parser = optparse.OptionParser("""index-flickr-shapes.py --options N_data_X.Y.Z.zip""")
    parser.add_option("-e", "--elasticsearch", dest="es", help="your ES endpoint; default is localhost:9200", default='localhost:9200')
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    i = FlickrShapesIndexer(opts)

    if len(args) == 0:
        logging.error('You forgot to point to one or more Flickr Shape .tar.gz files')
        sys.exit()

    for path in args:
        logging.info("processing %s" %path)
        i.index_tgzfile(path)
