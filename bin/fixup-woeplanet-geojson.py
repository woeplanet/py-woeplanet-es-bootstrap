#!/usr/bin/env python

import datetime
import geojson
import hashlib
import json
import logging
import os
import pyproj
import shapely.geometry
import sys

class Fixer(object):
    def __init__(self, opts):
        self.opts = opts

    def do_fixup(self, path):
        if not os.path.exists(path):
            raise Exception('%s: no such file or directory' % path)

        elif os.path.isdir(path):
            for root, _, files in os.walk(path):
                for file in files:
                    if '.geojson' in file:
                        srcfile = os.path.join(root, file)
                        self.do_fixups(srcfile)

        else:
            self.do_fixups(path)

    def do_fixups(self, path):
        with open(path, 'r') as reader:
            try:
                doc = json.load(reader)
            except Exception as e:
                logging.error('%s: %s' % (path, e))
                raise e

            srchash = hashlib.md5(json.dumps(doc, sort_keys=True).encode('utf8')).hexdigest()

            doc = self.fixup_scale(doc)
            doc = self.fixup_geometry(doc, path)
            doc = self.fixup_area(doc, path)
            self.validate_geojson(doc, path)

            hash = hashlib.md5(json.dumps(doc, sort_keys=True).encode('utf8')).hexdigest()
            if srchash != hash:
                doc['properties']['meta:updated'] = str(datetime.datetime.utcnow().isoformat())
                logging.debug('Writing update to %s' % path)
                with open(path, 'w') as writer:
                    json.dump(doc, writer)

    def fixup_geometry(self, doc, path):
        woeid = int(doc['properties']['woe:id'])
        if 'geometry' in doc:
            logging.debug('Checking geometry for %s' % path)
            if doc['geometry']['type'] not in ['Point', 'Polygon', 'MultiPolygon']:
                logging.warning('%s: fixing unexpected geometry type %s in %s' % (woeid, doc['geometry']['type'], path))
                if doc['geometry']['type'] == 'point':
                    doc['geometry']['type'] = 'Point'

        else:
            doc['geometry'] = {
                'type': 'Point',
                'coordinates': [0.0, 0.0]
            }

        doc = self.fixup_coords(doc, 'woe:latitude')
        doc = self.fixup_coords(doc, 'woe:longitude')
        doc = self.fixup_coords(doc, 'geom:latitude')
        doc = self.fixup_coords(doc, 'geom:longitude')
        doc = self.fixup_centroid(doc, 'woe:centroid')
        doc = self.fixup_centroid(doc, 'geom:centroid')

        return doc

    def fixup_coords(self, doc, property):
        if not property in doc['properties']:
            doc['properties'][property] = 0.0
        return doc

    def fixup_centroid(self, doc, property):
        if not property in doc['properties']:
            doc['properties'][property] = [0.0, 0.0]
        return doc

    def fixup_area(self, doc, path):
        if 'geometry' in doc:
            logging.debug('Checking geom:area for %s' % path)
            geodoc = geojson.loads(json.dumps(doc))
            geod = pyproj.Geod(ellps='WGS84')
            geom = shapely.geometry.shape(geodoc['geometry'])
            geodoc['properties']['geom:area'] = abs(geod.geometry_area_perimeter(geom)[0])

        return geodoc

    def fixup_scale(self, doc):
        ptid = int(doc['properties']['woe:placetype'])
        scale = 0
        if ptid == 19:  # supername
            scale = 1
        elif ptid == 29:  # continent
            scale = 2
        elif ptid == 38:  # sea
            scale = 3
        elif ptid == 37:    # ocean
            scale = 3
        elif ptid == 21:  # region
            scale = 4
        elif ptid == 12:  # country
            scale = 5
        elif ptid == 8:  # state
            scale = 6
        elif ptid == 18:  # nationality
            scale = 7
        elif ptid == 31:  # timezone
            scale = 8
        elif ptid == 9:  # county
            scale = 9
        elif ptid == 36:    # aggregate
            scale = 10
        elif ptid == 13:  # island
            scale = 11
        elif ptid == 16:  # land feature
            scale = 11
        elif ptid == 24:  # colloquial
            scale = 12
        elif ptid == 10:  # local admin
            scale = 13
        elif ptid == 25:  # zone
            scale = 14
        elif ptid == 15:  # drainage
            scale = 15
        elif ptid == 7:  # town
            scale = 16
        elif ptid == 22:  # suburb
            scale = 17
        elif ptid == 33:  # estate
            scale = 18
        elif ptid == 23:  # sports team
            scale = 19
        elif ptid == 20:  # poi
            scale = 19
        elif ptid == 14:  # airport
            scale = 19
        elif ptid == 17:  # miscellaneous
            scale = 20
        elif ptid == 6:  # street
            scale = 21
        elif ptid == 32:  # nearby intersection
            scale = 21
        elif ptid == 11:  # zip
            scale = 22
        elif ptid == 26:  # historical state
            scale = 23
        elif ptid == 27:  # historical county
            scale = 24
        elif ptid == 35:  # historical town
            scale = 25

        doc['properties']['woe:scale'] = scale
        return doc

    def validate_geojson(self, doc, path):
        geodoc = geojson.loads(json.dumps(doc))
        if not geodoc.is_valid:
            woeid = int(geodoc['properties']['woe:id'])
            raise Exception('%s: invalid GeoJSON found in %s' % (woeid, path))

def main():
    parser = argparse.ArgumentParser(prog='fixup-woeplanet-geojson', description='Load, validate and fixup WOE GeoJSON files or directories')
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='enable chatty logging; default is false', default=False)
    parser.add_argument('paths', nargs='*')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if len(args.paths) == 0:
        logging.error('You forgot to point to one or more source directories or files!')
        sys.exit()

    fixer = Fixer(args)
    for path in args.paths:
        try:
            fixer.do_fixup(path)

        except Exception as e:
            logging.error(e)

if __name__ == '__main__':
    import argparse

    main()