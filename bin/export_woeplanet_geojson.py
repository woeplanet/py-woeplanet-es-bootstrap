#!/usr/bin/env python
"""
Exports a WoePlanet ES index as individual GeoJSON files
"""
import logging
import json
import os
import sys

import woeplanet.utils.uri as uri

from elasticsearch import Elasticsearch, NotFoundError, helpers


class WoePlanetExporter:
    """
    Elasticsearch to GeoJSON exporter thing
    """
    def __init__(self, config):
        self.config = config
        self.esclient = Elasticsearch(
            [self.config.es],
            timeout=30,
            max_retries=10,
            retry_on_timeout=True
        )
        self.index = 'woeplanet'
        self.scroll_size = 10000
        self.update_count = 0

        logger = logging.getLogger('elasticsearch')
        logger.setLevel(logging.ERROR)

    def export_as_geojson(self, path):
        """
        Export a WoePlanet index to GeoJSON
        """

        rsp = helpers.scan(self.esclient, index=self.index, scroll='5m', size=self.scroll_size)
        for num, doc in enumerate(rsp):
            logging.debug('doc # %s', num)
            src = doc['_source']

            if src['woe:placetype'] == 0:
                if 'woe:superseded_by' in src:
                    doc = self.get_by_woeid(src['woe:superseded_by'])
                    if doc:
                        src['iso:country'] = doc['iso:country']
                        src['woe:name'] = doc['woe:name']
                        src['woe:placetype'] = doc['woe:placetype']
                        src['woe:placetype_name'] = doc['woe:placetype_name']
                    else:
                        logging.error(
                            '%d: Cannot find superseded_by link to %d!',
                            src['woe:id'],
                            src['woe:superseded_by']
                        )

                else:
                    logging.error(
                        '%d: %s placetype with no superseded_by linkage!',
                        src['woe:id'],
                        src['woe:placetype']
                    )

            if not 'iso:country' in doc:
                doc['iso:country'] = 'ZZ'

            # logging.debug('%d: %s(%d)' % (src['woe:id'], src['woe:placetype_name'], src['woe:placetype']))
            reponame, _, datadir = self.get_repo(path, src)
            logging.debug('%d: %s %s', src['woe:id'], reponame, datadir)
            src['woe:repo'] = reponame

            geometry = src.pop('geometry', None)
            if not geometry:
                lat = None
                lng = None
                if 'geom:latitude' in src:
                    lat = src['geom:latitude']

                if 'geom:longitude' in src:
                    lng = src['geom:longitude']

                if lat and lng:
                    logging.warning('%d: Building Point geometry from coordinates', src['woe:id'])
                    geometry = {
                        'type': 'Point',
                        'coordinates': [lng,
                                        lat]
                    }
                else:
                    logging.debug('%d: Building Point geometry for Null Island', src['woe:id'])
                    geometry = {
                        'type': 'Point',
                        'coordinates': [0,
                                        0]
                    }

            geojson = {
                'type': 'Feature',
                'id': src['woe:id'],
                'properties': src,
                'geometry': geometry
            }

            # os.makedirs(datadir, exist_ok=True)
            outpath = uri.id2abspath(datadir, src['woe:id'])
            os.makedirs(os.path.dirname(outpath), exist_ok=True)
            logging.debug('%d: %s', src['woe:id'], outpath)

            if os.path.isfile(outpath) and not self.config.force:
                with open(outpath, 'r', encoding='UTF-8') as ifh:
                    data = json.load(ifh)

                if data['type'] == 'FeatureCollection':
                    if data['features'][0]['properties']['meta:updated'] != src['meta:updated']:
                        with open(outpath, 'w', encoding='UTF-8') as ofh:
                            json.dump(geojson, ofh)
                else:
                    if data['properties']['meta:updated'] != src['meta:updated']:
                        with open(outpath, 'w', encoding='UTF-8') as ofh:
                            json.dump(geojson, ofh)

            else:
                with open(outpath, 'w', encoding='UTF-8') as ofh:
                    json.dump(geojson, ofh)

            self.update_count += 1
            if (self.update_count % self.scroll_size == 0):
                logging.info('export GeoJSON places @ %d', self.update_count)

        logging.info('export GeoJSON places @ %d', self.update_count)

    def get_repo(self, root, doc):
        """
        Make a repository name
        """

        placetype = doc['woe:placetype']
        placetype_name = doc['woe:placetype_name'].lower()
        if 'iso:country' in doc:
            iso = doc['iso:country'].lower()
        else:
            iso = 'zz'

        if placetype == 11 and (iso == 'gb' or iso == 'ca' or iso == 'pt' or iso == 'jp'):    # zip
            reponame = f"woeplanet-{placetype_name}-{iso}-{doc['woe:name'][0:1].lower()}"
        else:
            reponame = f'woeplanet-{placetype_name}-{iso}'

        repodir = os.path.join(root, reponame)
        datadir = os.path.join(repodir, 'data')
        return reponame, repodir, datadir

    def get_by_woeid(self, woeid):
        """
        Get document by WOEID
        """
        try:
            rsp = self.esclient.get(self.index, woeid)
            if 'found' in rsp:
                return rsp['_source']
            else:
                return None

        except NotFoundError as _:
            return None


def main():
    """
    Script main entry point
    """

    parser = argparse.ArgumentParser(
        prog='export-woeplanet-geojson',
        description='Exports a WoePlanet ES index as individual GeoJSON files'
    )
    parser.add_argument(
        "-e",
        "--elasticsearch",
        dest="es",
        help="your ES endpoint; default is localhost:9200",
        default='localhost:9200'
    )
    parser.add_argument(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        help="force export a WOEID, even if no changes have been made since the last export; default is false",
        default=False
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="verbose",
        action="store_true",
        help="enable chatty logging; default is false",
        default=False
    )
    parser.add_argument('paths', nargs=1)

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    wpe = WoePlanetExporter(args)

    if not args.paths:
        logging.error('You forgot to point the root of your export directory!')
        sys.exit()

    for path in args.paths:
        logging.info("exporting to %s", path)
        wpe.export_as_geojson(path)


if __name__ == '__main__':
    import argparse

    main()