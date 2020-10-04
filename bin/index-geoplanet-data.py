#!/usr/bin/env python

import csv
import datetime
import io
import json
import logging
import os.path
import re
import sqlite3
import sys
import zipfile

from elasticsearch import Elasticsearch, NotFoundError, helpers
from woeplanet.docs.placetypes import PlaceTypes
import pygeohash
import shapely.geometry

class GeoPlanetIndexer(object):
    def __init__(self, opts):
        self.opts = opts
        self.es = Elasticsearch([self.opts.es], timeout=30, max_retries=10, retry_on_timeout=True)
        self.version = None
        self.zf = None
        self.update_count = 100000
        self.index = 'woeplanet'
        self.placetypes = PlaceTypes(self.es)
        self.source = None

        ll = logging.getLogger('elasticsearch')
        ll.setLevel(logging.ERROR)

    def purge(self):
        logging.info("Purging/deleting index: %s" % self.index)
        if self.es.indices.exists(index=self.index):
            self.es.indices.delete(index=self.index)

    def _add(self, docs):
        helpers.bulk(self.es, docs, request_timeout=60)

    def parse_zipfile(self, path):
        pattern = re.compile(r"(((woe|geo)planet)_data)_([\d\.]+)\.zip$")
        match = pattern.match(os.path.basename(path))

        if not match:
            logging.error("failed to match source/version number!")
            return False

        groups = match.groups()
        self.source = groups[1]
        self.version = groups[3]

        places = '%s_places_%s.tsv' % (self.source, self.version)
        aliases = '%s_aliases_%s.tsv' % (self.source, self.version)
        adjacencies = '%s_adjacencies_%s.tsv' % (self.source, self.version)
        changes = '%s_changes_%s.tsv' % (self.source, self.version)
        admins = '%s_admins_%s.tsv' % (self.source, self.version)
        countries = '%s_countries_%s.tsv' % (self.source, self.version)
        timezones = '%s_timezones_%s.tsv' % (self.source, self.version)
        concordance = '%s_concordance_%s.tsv' % (self.source, self.version)
        coords = '%s_coords_%s.tsv' % (self.source, self.version)

        self.zf = zipfile.ZipFile(path)

        file_list = []

        for i in self.zf.infolist():
            file_list.append(i.filename)

        if not places in file_list:
            logging.error("Missing %s" % places)
            return False

        self.parse_places(places)
        self.parse_aliases(aliases)
        self.parse_adjacencies(adjacencies)

        if changes in file_list:
            self.parse_changes(changes)

        if admins in file_list:
            self.parse_admins(admins)

        if countries in file_list:
            self.parse_countries(countries)

        if timezones in file_list:
            self.parse_timezones(timezones)

        if concordance in file_list:
            self.parse_concordance(concordance)

        if coords in file_list:
            self.parse_coords(coords)

        logging.info("finished parsing %s" % path)

    def parse_places(self, fname):
        logging.info("parse places %s" % fname)

        reader = self.zf_reader(fname)
        docs = []
        counter = 0

        for row in reader:
            woeid = int(row['WOE_ID'])
            provider = '%s:%s' % (self.source, self.version)
            doc = self.get_by_woeid(woeid)
            if doc:
                upd = {
                    'woe:id': woeid,
                    'woe:parent_id': int(row['Parent_ID']),
                    'woe:name': row['Name'],
                    'woe:placetype': self.placetypes.by_name(row['PlaceType'])['id'],
                    'woe:placetype_name': row['PlaceType'],
                    'woe:lang' : row['Language'],
                    'iso:country': row['ISO'],
                    'meta:updated': str(datetime.datetime.utcnow().isoformat())
                }
                if not 'meta:provider' in doc:
                    upd['meta:provider'] = [provider]

                else:
                    if not provider in doc['meta:provider']:
                        upd['meta:provider'] = doc['meta:provider']
                        upd['meta:provider'].append(provider)

                docs.append({
                    '_index': self.index,
                    '_id': woeid,
                    '_op_type': 'update',
                    'doc': upd,
                    'doc_as_upsert': True
                })

            else:
                doc = {
                    '_index': self.index,
                    '_id': woeid,
                    'woe:id': woeid,
                    'woe:parent_id': int(row['Parent_ID']),
                    'woe:name': row['Name'],
                    'woe:placetype': self.placetypes.by_name(row['PlaceType'])['id'],
                    'woe:placetype_name': row['PlaceType'],
                    'woe:lang' : row['Language'],
                    'iso:country': row['ISO'],
                    'meta:provider': [provider],
                    'meta:indexed': str(datetime.datetime.utcnow().isoformat())
                }
                docs.append(doc)

            if len(docs) == self.update_count:
                logging.info("places %s counter @ %s" % (self.version, counter))
                counter += len(docs)

                self._add(docs)
                docs = []

        if len(docs):
            logging.info("places %s counter @ %s" % (self.version, counter))
            counter += len(docs)

            self._add(docs)

        logging.info("places %s added %s docs" % (self.version, counter))
        return True



    def parse_adjacencies(self, fname):
        logging.info("parse adjacencies %s" % fname)

        dbfile = "adjacencies-%s" % self.version

        setup = [
            "CREATE TABLE geoplanet_adjacencies (woeid INTEGER, neighbour INTEGER)",
            "CREATE INDEX adjacencies_by_woeid ON geoplanet_adjacencies (woeid)"
        ]

        con, cur = self.sqlite_db(dbfile, setup)
        reader = self.zf_reader(fname)

        logging.info("sql-ized adjacencies start")

        for row in reader:
            woeid = int(row['Place_WOE_ID'])
            woeid_adjacent = int(row['Neighbour_WOE_ID'])

            sql = "INSERT INTO geoplanet_adjacencies (woeid, neighbour) VALUES (?,?)"
            cur.execute(sql, (woeid, woeid_adjacent))

            con.commit()

        logging.info("sql-ized adjacencies complete")

        docs = []
        counter = 0

        ids = []

        res = cur.execute("""SELECT DISTINCT(woeid) FROM geoplanet_adjacencies""")

        for row in res:
            woeid = row[0]
            ids.append(woeid)

        for woeid in ids:
            sql = """SELECT * FROM geoplanet_adjacencies WHERE woeid=?"""
            a_res = cur.execute(sql, (woeid,))

            adjacent = []

            for a_row in a_res:
                woeid, neighbour = a_row
                adjacent.append(neighbour)

            logging.debug("got %s neighbours for WOE ID %s" % (len(adjacent), woeid))

            doc = self.get_by_woeid(woeid)
            if doc:
                doc['woe:adjacent'] = adjacent
                doc['meta:updated'] = str(datetime.datetime.utcnow().isoformat())

                docs.append({
                    '_index': self.index,
                    '_id': woeid,
                    '_op_type': 'update',
                    'doc': doc,
                    'doc_as_upsert': True
                })

            # Blurgh...
            else:
                pt = self.placetypes.by_name('unknown')
                doc = {
                    '_index': self.index,
                    '_id': woeid,
                    'woe:id': woeid,
                    'meta:provider': ['%s:%s' % (self.source, self.version)],
                    'woe:adjacent': adjacent,
                    'woe:placetype': pt['id'],
                    'woe:placetype_name': pt['shortname'],
                    'meta:indexed': str(datetime.datetime.utcnow().isoformat())
                }
                docs.append(doc)

            if len(docs) == self.update_count:
                logging.info("adjacencies counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info("adjacencies counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("finished importing adjacencies")
        os.unlink(dbfile)

    def parse_aliases(self, fname):
        logging.info("parse aliases %s" % fname)

        reader = self.zf_reader(fname)

        logging.info("sql-ized aliases start")

        dbfile = "aliases-%s" % self.version

        setup = [
            "CREATE TABLE geoplanet_aliases (woeid INTEGER, name TEXT, type TEXT)",
            "CREATE INDEX aliases_by_woeid ON geoplanet_aliases (woeid)"
            ]

        con, cur = self.sqlite_db(dbfile, setup)

        for row in reader:
            woeid = int(row['WOE_ID'])
            name = row['Name']

            type = "%s_%s" % (row['Language'], row['Name_Type'])

            sql = "INSERT INTO geoplanet_aliases (woeid, name, type) VALUES (?,?,?)"
            cur.execute(sql, (woeid, name, type))

            con.commit()

        logging.info("sql-ized aliases complete")

        docs = []
        counter = 0

        ids = []
        res = cur.execute("""SELECT DISTINCT(woeid) FROM geoplanet_aliases""")

        # ZOMGWTF... why do I need to do this????
        # (20130309/straup)

        for row in res:
            woeid = row[0]
            ids.append(woeid)

        for woeid in ids:
            sql = """SELECT * FROM geoplanet_aliases WHERE woeid=?"""
            a_res = cur.execute(sql, (woeid,))

            aliases = {}

            for a_row in a_res:
                woeid, name, type = a_row
                k = "woe:alias_%s" % type

                names = aliases.get(k, [])
                names.append(name)

                aliases[k] = names

            doc = self.get_by_woeid(woeid)
            if doc:
                for k, v in list(aliases.items()):
                    doc[k] = v

                doc['meta:updated'] = str(datetime.datetime.utcnow().isoformat())
                doc.pop('meta:indexed', None)
                docs.append({
                    '_index': self.index,
                    '_id': woeid,
                    '_op_type': 'update',
                    'doc': doc,
                    'doc_as_upsert': True
                })

            # Wot?!
            else:
                logging.warning("WTF... backfilling missing record for WOE ID (%s)" % woeid)
                pt = self.placetypes.by_name('unknown')
                doc = {
                    '_index': self.index,
                    '_id': woeid,
                    'woe:id': woeid,
                    'meta:provider': ['%s:%s' % (self.source, self.version)],
                    'woe:placetype': pt['id'],
                    'woe:placetype_name': pt['shortname'],
                    'meta:indexed': str(datetime.datetime.utcnow().isoformat())
                }
                for k, v in list(aliases.items()):
                    doc[k] = v

                docs.append(doc)

            if len(docs) == self.update_count:
                logging.info("aliases counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info("aliases counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("updated aliases for %s docs" % counter)
        os.unlink(dbfile)

    def parse_changes(self, fname):
        logging.info("parse changes %s" % fname)

        reader = self.zf_reader(fname)
        docs = []

        for row in reader:
            # docs = []

            # I know right? This is a problem in the
            # geoplanet_changes_7.8.1 file (20130313/straup)

            try:
                old_woeid = int(row['Woe_id'])
                new_woeid = int(row['Rep_id'])
            except Exception as _:
                continue

            old = self.get_by_woeid(old_woeid)
            new = self.get_by_woeid(new_woeid)
            provider = '%s:%s' % (self.source, self.version)

            if old:
                old['woe:superseded_by'] = new_woeid
                if not 'meta:provider' in old:
                    old['meta:provider'] = [provider]

                else:
                    if not provider in old['meta:provider']:
                        old['meta:provider'].append(provider)

                logging.debug("old: %s new: %s" % (old_woeid, new_woeid))

                old['meta:updated'] = str(datetime.datetime.utcnow().isoformat())
                docs.append({
                    '_index': self.index,
                    '_id': old['woe:id'],
                    '_op_type': 'update',
                    'doc': old,
                    'doc_as_upsert': True
                })

            else:
                logging.debug("old: %s new: %s" % (old_woeid, new_woeid))
                pt = self.placetypes.by_name('unknown')

                doc = {
                    '_index': self.index,
                    '_id': old_woeid,
                    'woe:id': old_woeid,
                    'meta:provider': ['%s:%s' % (self.source, self.version)],
                    'woe:superseded_by': new_woeid,
                    'woe:placetype': pt['id'],
                    'woe:placetype_name': pt['shortname'],
                    'meta:indexed': str(datetime.datetime.utcnow().isoformat())
                }

                docs.append(doc)

            if new:
                supersedes = new.get('woe:supersedes', [])
                logging.debug("new: %s supersedes: %s" % (new_woeid, supersedes))

                if not old_woeid in supersedes:
                    supersedes.append(old_woeid)

                    new['woe:supersedes'] = supersedes

                    logging.debug("old: %s new: %s" % (old_woeid, new_woeid))
                    new['meta:updated'] = str(datetime.datetime.utcnow().isoformat())
                    docs.append({
                        '_index': self.index,
                        '_id': new['woe:id'],
                        '_op_type': 'update',
                        'doc': new,
                        'doc_as_upsert': True
                    })

            else:
                logging.warning("WTF... no record for new WOE ID (%s)" % new_woeid)

        if len(docs):
            self._add(docs)

        logging.info("changes %s amended %s docs" % (self.version, len(docs)))


    def parse_admins(self, fname):
        logging.info("parse admin hierarchy %s" % fname)

        reader = self.zf_reader(fname)
        docs = []
        counter = 0

        for row in reader:
            woeid = int(row['WOE_ID'])

            ## FFS WTF ... bale on even trying to do the disambiguation "fun" of
            ## trying to parse GeoPlanet admins with names and not WOEIDs
            ## (20200623/vicchi)

            try:
                state_id = int(row['State'])
                county_id = int(row['County'])
                localadmin_id = int(row['Local_Admin'])
                country_id = int(row['Country'])
                continent_id =  int(row['Continent'])
            except ValueError as _:
                logging.info("sorry, baling on non WOEID admin hierarchies")
                break

            doc = self.get_by_woeid(woeid)
            if doc:
                update = {
                    '_index': self.index,
                    '_id': woeid,
                    '_op_type': 'update',
                    'doc': {
                        'iso:country': row['ISO'],
                        'woe:hierarchy': {
                            'planet': 1,
                            'continent': continent_id,
                            'region': 0,
                            'country': country_id,
                            'state': state_id,
                            'county': county_id,
                            'localadmin': localadmin_id,
                            'town': 0,
                            'suburb': 0
                        },
                        'meta:updated':
                        str(datetime.datetime.utcnow().isoformat())
                    },
                    'doc_as_upsert': True
                }
                docs.append(update)

            else:
                logging.warning("WTF... no record for WOE ID (%s)" % woeid)

            if len(docs) == self.update_count:
                logging.info("admin hierarchy counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info("admin hierarchy counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("finished importing admin hierarchy")


    def parse_countries(self, fname):
        logging.info("parse countries %s" % fname)

        reader = self.zf_reader(fname)
        docs = []
        counter = 0

        for row in reader:
            woeid = int(row['WOE_ID'])
            doc = self.get_by_woeid(woeid)
            if doc:
                update = {
                    '_index': self.index,
                    '_id': woeid,
                    '_op_type': 'update',
                    'doc': {
                        'iso:country': row['ISO2'].replace("'", ''),
                        'iso:country3': row['ISO3'].replace("'", ''),
                        'meta:updated': str(datetime.datetime.utcnow().isoformat())
                    },
                    'doc_as_upsert': True
                }
                docs.append(update)

            else:
                logging.warning("WTF... no record for WOE ID (%s)" % woeid)

            if len(docs) == self.update_count:
                logging.info("countries counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info("countries counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("finished importing countries")

    def parse_timezones(self, fname):
        logging.info("parse timezones %s" % fname)

        reader = self.zf_reader(fname)
        docs = []
        counter = 0

        for row in reader:
            woeid = int(row['WOE_ID'])
            tzid = int(row['TimeZone_ID'])
            if tzid == 0:
                continue

            doc = self.get_by_woeid(woeid)
            if doc:
                update = {
                    '_index': self.index,
                    '_id': woeid,
                    '_op_type': 'update',
                    'doc': {
                        'woe:timezone_id': tzid,
                        'meta:updated': str(datetime.datetime.utcnow().isoformat())
                    },
                    'doc_as_upsert': True
                }
                docs.append(update)

            else:
                logging.warning("WTF... no record for WOE ID (%s)" % woeid)

            if len(docs) == self.update_count:
                logging.info("timezones counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info("timezones counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("finished importing timezones")

    def parse_concordance(self, fname):
        logging.info("parse concordance %s" % fname)

        reader = self.zf_reader(fname)
        docs = []
        counter = 0

        for row in reader:
            woeid = int(row['WOE_ID'])
            doc = self.get_by_woeid(woeid)
            if doc:
                concordance = {}
                if row['GeoNames_ID'] != '0':
                    concordance['gn:id'] = int(row['GeoNames_ID'])
                if row['QuattroShapes_ID'] != '0':
                    concordance['qs:id'] = int(row['QuattroShapes_ID'])

                if concordance:
                    update = {
                        '_index': self.index,
                        '_id': woeid,
                        '_op_type': 'update',
                        'doc': {
                            'woe:concordances': concordance,
                            'meta:updated': str(datetime.datetime.utcnow().isoformat())
                        },
                        'doc_as_upsert': True
                    }
                    docs.append(update)

            else:
                logging.warning("WTF... no record for WOE ID (%s)" % woeid)

            if len(docs) == self.update_count:
                logging.info("concordance counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info("concordance counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("finished importing concordance")

    def parse_coords(self, fname):
        logging.info("parse coords %s" % fname)

        reader = self.zf_reader(fname)
        docs = []
        counter = 0

        for row in reader:
            woeid = int(row['WOE_ID'])
            doc = self.get_by_woeid(woeid)
            if doc:
                coords = {}
                if len(row['Lat']) != 0 and not row['Lat'].isspace() and row['Lat'] != '\\N':
                    coords['geom:latitude'] = coords['woe:latitude'] = float(row['Lat'])
                if len(row['Lon']) != 0 and not row['Lon'].isspace() and row['Lon'] != '\\N':
                    coords['geom:longitude'] = coords['woe:longitude'] = float(row['Lon'])

                if all (key in coords for key in ('geom:latitude', 'geom:longitude')):
                    coords['geom:centroid'] = [coords['geom:longitude'], coords['geom:latitude']]
                    coords['geom:hash'] = pygeohash.encode(coords['geom:latitude'], coords['geom:longitude'])
                    coords['woe:centroid'] = coords['geom:centroid']
                    coords['woe:hash'] = coords['geom:hash']

                if len(row['SW_Lat']) != 0 and not row['SW_Lat'].isspace() and row['SW_Lat'] != '\\N':
                    coords['geom:min_latitude'] = coords['woe:min_latitude'] = float(row['SW_Lat'])
                if len(row['SW_Lon']) != 0 and not row['SW_Lon'].isspace() and row['SW_Lon'] != '\\N':
                    coords['geom:min_longitude'] = coords['woe:min_longitude'] = float(row['SW_Lon'])
                if len(row['NE_Lat']) != 0 and not row['NE_Lat'].isspace() and row['NE_Lat'] != '\\N':
                    coords['geom:max_latitude'] = coords['woe:max_latitude'] = float(row['NE_Lat'])
                if len(row['NE_Lon']) != 0 and not row['NE_Lon'].isspace() and row['NE_Lon'] != '\\N':
                    coords['geom:max_longitude'] = coords['woe:max_longitude'] = float(row['NE_Lon'])

                if all (key in coords for key in ('geom:min_latitude', 'geom:min_longitude', 'geom:max_latitude', 'geom:max_longitude')):
                    if coords['geom:min_longitude'] != coords['geom:max_longitude'] and coords['geom:min_latitude'] != coords['geom:max_latitude']:
                        coords['geom:bbox'] = [coords['geom:min_longitude'], coords['geom:min_latitude'], coords['geom:max_longitude'], coords['geom:max_latitude']]
                        coords['woe:bbox'] = coords['geom:bbox']

                if 'geom:bbox' in coords:
                    p = shapely.geometry.box(coords['geom:min_longitude'], coords['geom:min_latitude'], coords['geom:max_longitude'], coords['geom:max_latitude'], ccw=True)
                    m = shapely.geometry.mapping(shapely.geometry.multipolygon.MultiPolygon([p]))
                    coords['geometry'] = {
                        'type': m['type'],
                        'coordinates': m['coordinates']
                    }

                elif 'geom:centroid' in coords:
                    coords['geometry'] = {
                        'type': 'Point',
                        'coordinates': coords['geom:centroid']
                    }

                if coords:
                    coords['woe:id'] = woeid
                    coords['meta:updated'] = str(datetime.datetime.utcnow().isoformat())
                    update = {
                        '_index': self.index,
                        '_id': woeid,
                        '_op_type': 'update',
                        'doc': coords,
                        'doc_as_upsert': True

                    }
                    docs.append(update)

            else:
                logging.warning("WTF... no record for WOE ID (%s)" % woeid)

            if len(docs) == self.update_count:
                logging.info("coords counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info("coords counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("finished importing coords")

    def sqlite_db(self, dbfile, setup):
        if os.path.exists(dbfile):
            os.unlink(dbfile)

        con = sqlite3.connect(dbfile)
        cur = con.cursor()

        cur.execute("""PRAGMA synchronous=0""")
        cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")
        cur.execute("""PRAGMA journal_mode=DELETE""")

        for cmd in setup:
            cur.execute(cmd)

        return con, cur

    def zf_reader(self, fname, delimiter='\t'):
        fh = self.zf.open(fname)
        fhw = io.TextIOWrapper(fh, encoding='utf-8')

        # gggggrnnhhhnnnhnhn.... yes, really.
        known_bad = ('7.4.0', '7.4.1')

        if fname.startswith('geoplanet_changes') and self.version in known_bad:
            _ = next(fhw)

            out = io.StringIO()
            out.write("\t".join(["Woe_id", "Rep_id", "Data_Version"]) + "\n")

            while fh.readable():
                try:
                    out.write(next(fhw))
                except Exception as _:
                    break

            out.seek(0)
            return csv.DictReader(out, delimiter=delimiter)

        else:
            return csv.DictReader(fhw, delimiter=delimiter)

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

    parser = optparse.OptionParser("""index-geoplanet-data.py --options N_data_X.Y.Z.zip""")
    parser.add_option("-e", "--elasticsearch", dest="es", help="your ES endpoint; default is localhost:9200", default='localhost:9200')
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)
    parser.add_option("--purge", dest="purge", action="store_true", help="...", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    gpi = GeoPlanetIndexer(opts)

    if opts.purge:
        gpi.purge()

    if len(args) == 0:
        logging.error('You forgot to point to one or more Geoplanet/WoePlanet Data zip files')
        sys.exit()

    for path in args:
        logging.info("processing %s" %path)
        gpi.parse_zipfile(path)