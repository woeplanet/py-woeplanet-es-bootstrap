#!/usr/bin/env python

import geojson
import json
import logging
import os
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
                        self.fixup_geojson(srcfile)
                        self.validate_geojson(srcfile)

        else:
            self.fixup_geojson(path)
            self.validate_geojson(srcfile)

    def fixup_geojson(self, path):
        with open(path, 'r') as reader:
            doc = json.load(reader)

        woeid = int(doc['properties']['woe:id'])
        if 'geometry' in doc:
            if doc['geometry']['type'] not in ['Point', 'Polygon', 'MultiPolygon']:
                logging.warning('%s: fixing unexpected geometry type %s in %s' % (woeid, doc['geometry']['type'], path))
                if doc['geometry']['type'] == 'point':
                    doc['geometry']['type'] = 'Point'

                    with open(path, 'w') as writer:
                        json.dump(doc, writer)

    def validate_geojson(self, path):
        with open(path, 'r') as reader:
            doc = geojson.load(reader)
        
        woeid = int(doc['properties']['woe:id'])
        if not doc.is_valid:
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