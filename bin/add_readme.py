#!/usr/bin/env python
"""
Creates a woeplanet-data repo template README.md file
"""

import json
import logging
import os
import re
import sys

import chevron
from elasticsearch import Elasticsearch

from woeplanet.docs.placetypes import PlaceTypes


class ReadmeMaker:
    """
    README maker thing
    """
    def __init__(self, config):
        self.config = config
        self.esclient = Elasticsearch(
            [self.config.es],
            timeout=30,
            max_retries=10,
            retry_on_timeout=True
        )
        self.index = config.index
        self.placetypes = PlaceTypes(self.esclient)
        self.manifest = {
            'repos': []
        }

        template = os.path.join(config.templates, 'README.mustache')
        if not os.path.isfile(template):
            raise RuntimeError(f'Cannot find template file {template}')

        with open(template, 'r', encoding='UTF-8') as ifh:
            self.main_template = ifh.read()

        template = os.path.join(config.templates, 'README-CATEGORY.mustache')
        if not os.path.isfile(template):
            raise RuntimeError(f'Cannot find template file {template}')

        with open(template, 'r', encoding='UTF-8') as ifh:
            self.category_template = ifh.read()

        logger = logging.getLogger('elasticsearch')
        logger.setLevel(logging.ERROR)

    def scan_repos(self, paths):
        """
        Scan WoePlanet repositories
        """

        for path in paths:
            # logging.info('path: %s' % path)
            for root, _, _ in os.walk(path):
                dirname = os.path.basename(root)
                repodir = os.path.join(path, root)
                match = re.match(r'^woeplanet-(.*)-([a-z]{2})$', dirname)
                if match:
                    ptname = match.group(1)
                    iso = match.group(2)
                    # logging.info('repo: %s, placetype: %s, iso: %s' % (dirname, ptname, iso))
                    self.make_readme(repodir=repodir, placetype=ptname, country=iso)
                else:
                    match = re.match(r'^woeplanet-(.*)-([a-z]{2})-([a-z]{1})$', dirname)
                    if match:
                        ptname = match.group(1)
                        iso = match.group(2)
                        subcat = match.group(3)
                        # logging.info('repo: %s, placetype: %s, iso: %s, sub: %s' % (dirname, ptname, iso, subcat))
                        self.make_readme(
                            repodir=repodir,
                            placetype=ptname,
                            country=iso,
                            category=subcat
                        )

        with open(self.config.manifest, 'w', encoding='UTF-8') as ofh:
            json.dump(self.manifest, ofh, indent=4)

    def make_readme(self, **kwargs):
        """
        Make me a README
        """

        placetype = kwargs.get('placetype')
        isocode = kwargs.get('country')
        repodir = kwargs.get('repodir')
        category = kwargs.get('category', None)

        ptrec = self.placetypes.by_name(placetype)
        country = self.get_country(isocode)
        readme = os.path.join(repodir, 'README.md')
        metadir = os.path.join(repodir, 'meta')
        os.makedirs(metadir, exist_ok=True)
        metafile = os.path.join(metadir, 'meta.json')
        reponame = os.path.basename(repodir)

        meta = {
            'repo_name': reponame,
            'placetype_name': ptrec['shortname'],
            'placetype_id': ptrec['id'],
            'country': country,
            'iso': isocode.upper()
        }

        if not os.path.isfile(metafile):
            with open(metafile, 'w', encoding='UTF-8') as ofh:
                json.dump(meta, ofh, indent=4)

        if not os.path.isfile(readme):
            logging.debug('Rendering to: %s for %s', readme, country)
            args = {
                'template': self.main_template,
                'data': meta
            }
            if category:
                args['template'] = self.category_template
                args['data']['category'] = category

            with open(readme, 'w', encoding='UTF-8') as ofh:
                ofh.write(chevron.render(**args))

        self.manifest['repos'].append(meta)

    def get_country(self, iso):
        """
        Get country by ISO code
        """

        iso = iso.upper()
        if iso == 'ZZ':
            return 'an unknown or invalid territory'

        if iso == 'XS':
            return 'a disputed territory or region'

        query = {
            'query': {
                'bool': {
                    'must': [{
                        'match': {
                            'iso:country': iso
                        }
                    },
                             {
                                 'match': {
                                     'woe:placetype': 12
                                 }
                             }]
                }
            }
        }
        res = self.esclient.search(body=query, index=self.index)
        country = None

        if res['hits']['total']['value'] == 1:
            src = res['hits']['hits'][0]['_source']
            if src['woe:lang'] in [
                'UNK',
                'JPN',
                'CHI',
                'KOR',
                'GER',
                'DUT',
                'NOR',
                'SPA',
                'FIN',
                'POR',
                'SWE',
                'HUN',
                'ITA',
                'CZE',
                'FRE',
                'RUM'
            ]:
                if 'woe:alias_ENG_P' in src:
                    country = src['woe:alias_ENG_P'][0]
                else:
                    country = src['woe:name']
            else:
                country = src['woe:name']

        else:
            logging.error('No match found for iso:country "%s"', iso)

        return country


def main():
    """
    Script main entry point
    """

    parser = argparse.ArgumentParser(
        prog='make-readme',
        description='Creates a woeplanet-data repo template README.md file'
    )
    parser.add_argument(
        '-e',
        '--elasticsearch',
        dest='es',
        metavar='URL',
        help='your ES endpoint; default is localhost:9200',
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
        '-m',
        '--manifest',
        help='Output manifest JSON path',
        default='manifest.json'
    )
    parser.add_argument(
        '-v',
        '--verbose',
        dest='verbose',
        action='store_true',
        help='enable chatty logging; default is false',
        default=False
    )
    parser.add_argument(
        '-t',
        '--templates',
        dest='templates',
        metavar='PATH',
        help='template file directory; default is ./templates',
        default='./templates'
    )
    parser.add_argument('paths', nargs='*')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if len(args.paths) == 0:
        logging.error('You forgot to point to one of more woeplanet-data repos!')
        sys.exit()

    # es = Elasticsearch([args.es], timeout=30, max_retries=10, retry_on_timeout=True)
    # placetypes = PlaceTypes(es)

    # ll = logging.getLogger('elasticsearch')
    # ll.setLevel(logging.ERROR)

    maker = ReadmeMaker(args)
    maker.scan_repos(args.paths)

    # for path in args.paths:
    #     # logging.info('path: %s' % path)
    #     for root, _, _ in os.walk(path):
    #         dirname = os.path.basename(root)
    #         fulldir = os.path.join(path, root)
    #         match = re.match(r'^woeplanet-(.*)-([a-z]{2})$', dirname)
    #         if match:
    #             ptname = match.group(1)
    #             iso = match.group(2)
    #             # logging.info('repo: %s, placetype: %s, iso: %s' % (dirname, ptname, iso))
    #             write_template_readme(es=es, index=args.index, dirname=fulldir, placetypes=placetypes, placetype_name=ptname, iso=iso)
    #         else:
    #             match = re.match(r'^woeplanet-(.*)-([a-z]{2})-([a-z]{1})$', dirname)
    #             if match:
    #                 ptname = match.group(1)
    #                 iso = match.group(2)
    #                 subcat = match.group(3)
    #                 # logging.info('repo: %s, placetype: %s, iso: %s, sub: %s' % (dirname, ptname, iso, subcat))
    #                 write_template_readme(es=es, index=args.index, dirname=fulldir, placetypes=placetypes, placetype_name=ptname, iso=iso, category=subcat)


# def write_template_readme(**kwargs):
#     es = kwargs.get('es')
#     index = kwargs.get('index')
#     placetypes = kwargs.get('placetypes')
#     placetype_name = kwargs.get('placetype_name')
#     iso = kwargs.get('iso')
#     category = kwargs.get('category', None)
#     dirname = kwargs.get('dirname')

#     placetype = placetypes.by_name(placetype_name)
#     # logging.info(placetype['name'])
#     country = get_country(**kwargs)
#     # logging.info(country)
#     readme = os.path.join(dirname, 'README.md')
#     if not os.path.isfile(readme):
#         pass

# def get_country(**kwargs):
#     iso = kwargs.get('iso')
#     es = kwargs.get('es')
#     index = kwargs.get('index')
#     query = {
#         'query': {
#             'bool': {
#                 'must': [
#                     {
#                         'match': {
#                             'iso:country': iso.upper()
#                         }
#                     },
#                     {
#                         'match': {
#                             'woe:placetype': 12
#                         }
#                     }
#                 ]
#             }
#         }
#     }
#     res = es.search(body=query, index=index)
#     name = None
#     if res['hits']['total']['value'] == 1:
#         src = res['hits']['hits'][0]['_source']
#         if src['woe:lang'] in ['UNK', 'JPN', 'CHI', 'KOR', 'GER', 'DUT', 'NOR', 'SPA', 'FIN', 'POR', 'SWE', 'HUN', 'ITA', 'CZE', 'FRE', 'RUM']:
#             if 'woe:alias_ENG_P' in src:
#                 name = src['woe:alias_ENG_P'][0]
#             else:
#                 name = src['woe:name']
#         else:
#             name = src['woe:name']

#         logging.info('%d: %s' % (src['woe:id'], name))

#     return name

if __name__ == '__main__':
    import argparse

    main()
