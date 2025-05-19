""" update_dois.py
    Synchronize DOI information from FLYF2 to FlyBoy and the config system.
"""

import argparse
import json
from operator import attrgetter
import re
import sys
from time import sleep
import requests
from unidecode import unidecode
import MySQLdb
from tqdm import tqdm
import jrc_common.jrc_common as JRC

# pylint: disable=broad-exception-caught,broad-exception-raised,logging-fstring-interpolation

# Database
READ = {'dois': "SELECT doi FROM doi_data",}
WRITE = {'doi': "INSERT INTO doi_data (doi,title,first_author,"
                + "publication_date) VALUES (%s,%s,%s,%s) ON "
                + "DUPLICATE KEY UPDATE title=%s,first_author=%s,"
                + "publication_date=%s",
         'delete_doi': "DELETE FROM doi_data WHERE doi=%s",
        }
DB = {}
# Configuration
CONFIG = {}
ARG = LOGGER = None
MAX_CROSSREF_TRIES = 3
# General
COUNT = {'delete': 0, 'found': 0, 'foundfb': 0, 'flyboy': 0, 'insert': 0, 'update': 0}


def terminate_program(msg=None):
    ''' Terminate the program gracefully
        Keyword arguments:
          msg: error message or object
        Returns:
          None
    '''
    if msg:
        if not isinstance(msg, str):
            msg = f"An exception of type {type(msg).__name__} occurred. Arguments:\n{msg.args}"
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def call_responder(server, endpoint):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code != 200:
        terminate_program(f"Status: {str(req.status_code)} ({url})")
    return req.json()


def initialize_program():
    """ Connect to FlyBoy database
    """
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbs = ['flyboy']
    for source in dbs:
        dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
        LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
        try:
            DB[source] = JRC.connect_database(dbo)
        except Exception as err:
            terminate_program(err)


def call_doi(doi):
    """ Get DOI information
        Keyword arguments:
        doi: DOI
    """
    url = 'https://api.crossref.org/works/' + doi
    headers = {'mailto': 'svirskasr@hhmi.org'}
    try:
        req = requests.get(url, headers=headers, timeout=10)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code != 200:
        terminate_program(f"Status: {str(req.status_code)} ({url})")
    return req.json()


def get_date(mesg):
    """ Determine the publication date
        Keyword arguments:
        mesg: Crossref record
    """
    if 'published-print' in mesg:
        date = mesg['published-print']['date-parts'][0][0]
    elif 'published-online' in mesg:
        date = mesg['published-online']['date-parts'][0][0]
    elif 'posted' in mesg:
        date = mesg['posted']['date-parts'][0][0]
    else:
        date = 'unknown'
    return date


def call_doi_with_retry(doi):
    """ Looping function for call_doi
        Keyword arguments:
          doi: DOI
        Returns:
          msg: response from crossref.org
          title: publication title
          author: publication first author surname
          date: publication year
    """
    attempt = MAX_CROSSREF_TRIES
    msg = ''
    while attempt:
        try:
            msg = call_doi(doi)
        except Exception as err:
            raise Exception(err) from err
        if 'title' in msg['message'] and 'author' in msg['message']:
            break
        attempt -= 1
        LOGGER.warning("Missing data from crossref.org: retrying (%d)", attempt)
        sleep(0.5)
    title = author = None
    if 'title' in msg['message']:
        title = msg['message']['title'][0]
    if 'author' in msg['message']:
        author = msg['message']['author'][0]['family']
    date = get_date(msg['message'])
    return msg, title, author, date


def call_datacite(doi):
    """ Get record from datacite
        Keyword arguments:
          doi: DOI
        Returns:
          msg: response from crossref.org
          title: publication title
          author: publication first author surname
          date: publication year
    """
    rec = call_responder('datacite', doi)
    title = author = None
    msg = rec['data']['attributes']
    if 'titles' in msg:
        title = msg['titles'][0]['title']
    if 'creators' in msg:
        author = msg['creators'][0]['familyName']
    if 'publicationYear' in msg:
        date = str(msg['publicationYear'])
    else:
        date = 'unknown'
    return rec, title, author, date


def perform_backcheck(rdict):
    """ Check to see if we need to delete DOIs from FlyBoy
        Keyword arguments:
        rdict: dictionary of DOIs
    """
    try:
        DB['flyboy']['cursor'].execute(READ['dois'])
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    rows = DB['flyboy']['cursor'].fetchall()
    for row in rows:
        COUNT['foundfb'] += 1
        if row['doi'] not in rdict:
            LOGGER.warning(WRITE['delete_doi'], (row['doi']))
            try:
                DB['flyboy']['cursor'].execute(WRITE['delete_doi'], (row['doi'],))
            except MySQLdb.Error as err:
                terminate_program(JRC.sql_error(err))
            COUNT['delete'] += 1


def needs_update(doi, msg):
    """ Check if the DOI needs to be updated
        Keyword arguments:
          doi: DOI
          msg: message
        Returns:
          True if the DOI needs to be updated, False otherwise
    """
    if 'indexed' not in msg or 'timestamp' not in msg['indexed']:
        return True
    rec = call_responder('config', f"config/dois/{doi}")
    if not rec:
        return True
    if 'config' not in rec or 'indexed' not in rec['config'] \
       or 'timestamp' not in rec['config']['indexed']:
        return True
    return bool(rec['config']['indexed']['timestamp'] != msg['indexed']['timestamp'])


def split_raw_doi(doi_string):
    """ Split a raw DOI string into multiple DOIs
        Keyword arguments:
          doi: DOI
        Returns:
          dois: list of DOIs
    """
    return re.split(r"\s*\|\s*", doi_string)


def process_single_doi(doi, rdict, ddict):
    """ Process a single DOI
        Keyword arguments:
          doi: DOI
          rdict: dictionary of DOIs
          ddict: dictionary of DOIs
        Returns:
          None
    """
    COUNT['found'] += 1
    if 'janelia' in doi:
        msg, title, author, date = call_datacite(doi)
        ddict[doi] = msg['data']['attributes']
    else:
        try:
            msg, title, author, date = call_doi_with_retry(doi)
        except Exception as err:
            print(err)
            return
        if needs_update(doi, msg['message']):
            ddict[doi] = msg['message']
    rdict[doi] = 1
    if not title:
        LOGGER.error("Missing title for %s", doi)
        return
    if not author:
        LOGGER.error("Missing author for %s (%s)", doi, title)
        return
    LOGGER.debug("%s: %s (%s, %s)", doi, title, author, date)
    title = unidecode(title)
    LOGGER.debug(WRITE['doi'], doi, title, author, date, title, author, date)
    try:
        DB['flyboy']['cursor'].execute(WRITE['doi'], (doi, title, author, date,
                                                title, author, date))
        COUNT['flyboy'] += 1
    except MySQLdb.Error as err:
        LOGGER.error("Could not update doi_data")
        terminate_program(JRC.sql_error(err))


def update_dois():
    """ Sync DOIs in doi_data from StockFinder
    """
    if ARG.DOI:
        rows = {"dois": [ARG.DOI]}
    else:
        LOGGER.info('Fetching DOIs from FLYF2')
        rows = call_responder('flycore', '?request=doilist')
    rdict = {}
    ddict = {}
    for doi_string in tqdm(rows['dois'], desc='Process DOIs'):
        dois = split_raw_doi(doi_string)
        for doi in dois:
            if 'in prep' not in doi:
                process_single_doi(doi, rdict, ddict)
    if not ARG.DOI:
        perform_backcheck(rdict)
    if ARG.WRITE:
        DB['flyboy']['conn'].commit()
        for key in tqdm(ddict, desc='Update config'):
            entry = json.dumps(ddict[key])
            LOGGER.debug(f"Updating {key} in config database")
            resp = requests.post(CONFIG['config']['url'] + 'importjson/dois/' + key,
                                 {"config": entry}, timeout=10)
            if resp.status_code != 200:
                LOGGER.error(resp.json()['rest']['message'])
            else:
                rest = resp.json()
                if 'inserted' in rest['rest']:
                    COUNT['insert'] += rest['rest']['inserted']
                elif 'updated' in rest['rest']:
                    COUNT['update'] += rest['rest']['updated']


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Sync DOIs within FlyBoy")
    PARSER.add_argument('--doi', dest='DOI', action='store',
                        help='Single DOI to insert/update')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', help='Database manifold')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually modify database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    try:
        CONFIG = JRC.simplenamespace_to_dict(JRC.get_config("rest_services"))
    except Exception as err:
        terminate_program(err)
    initialize_program()
    update_dois()
    print(f"DOIs found in StockFinder:             {COUNT['found']}")
    print(f"DOIs found in FlyBoy:                  {COUNT['foundfb']}")
    print(f"DOIs inserted/updated in FlyBoy:       {COUNT['flyboy']}")
    print(f"DOIs deleted from FlyBoy:              {COUNT['delete']}")
    print(f"Documents inserted in config database: {COUNT['insert']}")
    print(f"Documents updated in config database:  {COUNT['update']}")
    terminate_program()
