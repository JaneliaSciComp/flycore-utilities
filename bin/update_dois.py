import argparse
import json
import sys
import colorlog
import requests
from time import sleep
from unidecode import unidecode
import MySQLdb

# Database
READ = {'dois': "SELECT doi FROM doi_data",}
WRITE = {'doi': "INSERT INTO doi_data (doi,title,first_author,publication_date) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE title=%s,first_author=%s,publication_date=%s",
         'delete_doi': "DELETE FROM doi_data WHERE doi=%s",
        }
CONN = dict()
CURSOR = dict()
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
MAX_CROSSREF_TRIES = 3
# General
count = {'delete': 0, 'found': 0, 'foundfb': 0, 'flyboy': 0, 'insert': 0, 'update': 0}


def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        logger.critical('MySQL error [%d]: %s', err.args[0], err.args[1])
    except IndexError:
        logger.critical('MySQL error: %s', err)
    sys.exit(-1)


def db_connect(db):
    """ Connect to specified database
        Keyword arguments:
        db: database key
    """
    logger.info("Connecting to %s on %s", db['name'], db['host'])
    try:
        conn = MySQLdb.connect(host=db['host'], user=db['user'],
                               passwd=db['password'], db=db['name'])
    except MySQLdb.Error as err:
        sql_error(err)
    try:
        cursor = conn.cursor()
        return(conn, cursor)
    except MySQLdb.Error as err:
        sql_error(err)


def call_responder(server, endpoint):
    """ Call a responder
        Keyword arguments:
        server: server
        endpoint: REST endpoint
    """
    url = CONFIG[server]['url'] + endpoint
    try:
        req = requests.get(url)
    except requests.exceptions.RequestException as err:
        logger.critical(err)
        sys.exit(-1)
    if req.status_code != 200:
        logger.error('Status: %s (%s)', str(req.status_code), url)
        sys.exit(-1)
    return req.json()


def initialize_program():
    """ Connect to FlyBoy database
    """
    global CONFIG
    dbc = call_responder('config', 'config/db_config')
    data = dbc['config']
    (CONN['flyboy'], CURSOR['flyboy']) = db_connect(data['flyboy'][ARG.MANIFOLD])
    dbc = call_responder('config', 'config/rest_services')
    CONFIG = dbc['config']


def call_doi(doi):
    """ Get DOI information
        Keyword arguments:
        doi: DOI
    """
    url = 'https://api.crossref.org/works/' + doi
    headers = {'mailto': 'svirskasr@hhmi.org'}
    try:
        req = requests.get(url, headers=headers)
    except requests.exceptions.RequestException as err:
        logger.critical(err)
        sys.exit(-1)
    if req.status_code != 200:
        logger.error('Status: %s (%s)', str(req.status_code), url)
        sys.exit(-1)
    return req.json()


def call_doi_with_retry(doi):
    success = 0
    attempt = MAX_CROSSREF_TRIES
    msg = ''
    while attempt:
        msg = call_doi(doi)
        if 'title' in msg['message'] and 'author' in msg['message']:
            return(msg)
        attempt -= 1
        logger.warning("Missing data from crossref.org: retrying (%d)", attempt)
        sleep(0.5)
    logger.error("Incomplete data from crossref.org")
    return(msg)


def perform_backcheck(rdict):
    try:
        CURSOR['flyboy'].execute(READ['dois'])
    except MySQLdb.Error as err:
        sql_error(err)
    rows = CURSOR['flyboy'].fetchall()
    for row in rows:
        count['foundfb'] += 1
        if row[0] not in rdict:
            logger.warning(WRITE['delete_doi'], (row[0]))
            try:
                CURSOR['flyboy'].execute(WRITE['delete_doi'], (row[0],))
            except MySQLdb.Error as err:
                logger.error("Could not delete DOI from doi_data")
                sql_error(err)
            count['delete'] += 1


def get_date(mesg):
    if 'published-print' in mesg:
        date = mesg['published-print']['date-parts'][0][0]
    elif 'published-online' in mesg:
        date = mesg['published-online']['date-parts'][0][0]
    elif 'posted' in mesg:
        date = mesg['posted']['date-parts'][0][0]
    else:
        date = 'unknown'
    return(date)


def update_dois():
    """ Sync DOIs in doi_data from StockFinder
    """
    logger.info('Fetching DOIs from FLYF2')
    rows = call_responder('flycore', '?request=doilist')
    rdict = {}
    ddict = {}
    for doi in rows['dois']:
        count['found'] += 1
        msg = call_doi_with_retry(doi)
        rdict[doi] = 1
        if 'title' in msg['message']:
            title = msg['message']['title'][0]
        else:
            logger.error("Missing title for %s", doi)
            continue
        if 'author' in msg['message']:
            author = msg['message']['author'][0]['family']
        else:
            logger.error("Missing author for %s (%s)", doi, title)
            continue
        date = get_date(msg['message'])
        ddict[doi] = msg['message']
        logger.info("%s: %s (%s, %s)", doi, title, author, date)
        title = unidecode(title)
        logger.debug(WRITE['doi'], doi, title, author, date, title, author, date)
        try:
            CURSOR['flyboy'].execute(WRITE['doi'], (doi, title, author, date, title, author, date))
            count['flyboy'] += 1
        except MySQLdb.Error as err:
            logger.error("Could not update doi_data")
            sql_error(err)
    perform_backcheck(rdict)
    if ARG.WRITE:
        CONN['flyboy'].commit()
        resp = requests.post(CONFIG['config']['url'] + 'importjson/dois',
                             {"config": json.dumps(ddict)})
        if resp.status_code != requests.codes.ok:
            logger.error(resp.json()['rest']['message'])
        else:
            rest = resp.json()
            if 'inserted' in rest['rest']:
                count['insert'] += rest['rest']['inserted']
            elif 'updated' in rest['rest']:
                count['update'] += rest['rest']['updated']


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Sync DOIs within FlyBoy")
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

    logger = colorlog.getLogger()
    if ARG.DEBUG:
        logger.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        logger.setLevel(colorlog.colorlog.logging.INFO)
    else:
        logger.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    logger.addHandler(HANDLER)

    initialize_program()
    update_dois()
    print("DOIs found in StockFinder: %d" % count['found'])
    print("DOIs found in FlyBoy: %d" % count['foundfb'])
    print("DOIs inserted/updated in FlyBoy: %d" % count['flyboy'])
    print("DOIs deleted from FlyBoy: %d" % count['delete'])
    print("Documents inserted in config database: %d" % count['insert'])
    print("Documents updated in config database: %d" % count['update'])

    sys.exit(0)
