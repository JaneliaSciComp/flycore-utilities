"""
Remove duplicate Robot IDs from FlyBoy
"""

import argparse
import sys
import colorlog
import requests
import MySQLdb

# Database
READ = {'DUPLICATES': "SELECT RobotID,COUNT(1) AS c FROM StockFinder WHERE " +
                      "RobotID IS NOT NULL AND RobotID>0 GROUP BY 1 HAVING c>1",
        'ROBOT': "SELECT RobotID,__kp_UniqueID,Stock_Name FROM StockFinder WHERE RobotID=%s",
       }
WRITE = {'DELETE': "DELETE FROM StockFinder WHERE __kp_UniqueID=%s",
        }
CONN = dict()
CURSOR = dict()
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# General
COUNT = {'robot': 0, 'kp': 0, 'delete': 0}


def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        LOGGER.critical('MySQL error [%d]: %s', err.args[0], err.args[1])
    except IndexError:
        LOGGER.critical('MySQL error: %s', err)
    sys.exit(-1)


def db_connect(dbc):
    """ Connect to specified database
        Keyword arguments:
        db: database key
    """
    LOGGER.info("Connecting to %s on %s", dbc['name'], dbc['host'])
    try:
        conn = MySQLdb.connect(host=dbc['host'], user=dbc['user'],
                               passwd=dbc['password'], db=dbc['name'])
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
        LOGGER.critical(err)
        sys.exit(-1)
    if req.status_code != 200:
        LOGGER.error('Status: %s', str(req.status_code))
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


def update_flyboy():
    """ Remove duplicate Robot IDs from StockFinder
    """
    LOGGER.info('Fetching duplicate Robot IDs from FlyBoy')
    try:
        CURSOR['flyboy'].execute(READ['DUPLICATES'])
        rows = CURSOR['flyboy'].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        COUNT['robot'] += 1
        LOGGER.debug(READ['ROBOT'], row[0])
        try:
            CURSOR['flyboy'].execute(READ['ROBOT'], (str(row[0]),))
            fbrows = CURSOR['flyboy'].fetchall()
        except MySQLdb.Error as err:
            sql_error(err)
        for fbrow in fbrows:
            kpid = str(int(fbrow[1]))
            LOGGER.debug('Robot ID %s (KP %s, stock name %s)', fbrow[0], kpid, fbrow[2])
            resp = call_responder('flycore', '?request=linedata&kp=' + kpid)
            COUNT['kp'] += 1
            if 'linedata' in resp and resp['linedata'] == '':
                LOGGER.warning('KP %s (Robot ID %s) is not in FLYF2', kpid, fbrow[0])
                LOGGER.debug(WRITE['DELETE'], row[0])
                try:
                    CURSOR['flyboy'].execute(WRITE['DELETE'], (kpid,))
                    if CURSOR['flyboy'].rowcount:
                        COUNT['delete'] += 1
                    else:
                        LOGGER.error("Could not delete KPID %s from StockFinder", kpid)
                except MySQLdb.Error as err:
                    sql_error(err)
    if ARG.WRITE:
        CONN['flyboy'].commit()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Remove duplicate Robot IDs from FlyBoy")
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

    LOGGER = colorlog.getLogger()
    ATTR = colorlog.colorlog.logging if "colorlog" in dir(colorlog) else colorlog
    if ARG.DEBUG:
        LOGGER.setLevel(ATTR.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(ATTR.INFO)
    else:
        LOGGER.setLevel(ATTR.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)

    initialize_program()
    update_flyboy()
    print("Duplicate Robot IDs in StockFinder: %d" % COUNT['robot'])
    print("Associated KPIDs: %d" % COUNT['kp'])
    print("Deleted KPIDs: %d" % COUNT['delete'])

    sys.exit(0)
