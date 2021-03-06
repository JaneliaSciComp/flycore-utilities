''' sync_flyf_publishing_names.py
    Synchronize publishing names from FLYF2 crosses to SAGE.
'''
import argparse
import re
import sys
import colorlog
import requests
import MySQLdb

# Database
READ = {'EXISTS': "SELECT id FROM publishing_name WHERE line_id=%s AND publishing_name=%s",
        'LINEID': "SELECT id FROM line WHERE name=%s",
        'SOURCE': "SELECT source_id,id,line FROM publishing_name_vw",
       }
WRITE = {'DELETE': "DELETE FROM publishing_name WHERE id=%s",
         'PUBLISHING': "INSERT INTO publishing_name (line_id,source_id,"
                       + "publishing_name,for_publishing,published,label,"
                       + "requester,notes,source_create_date,preferred_name) "
                       + "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON "
                       + "DUPLICATE KEY UPDATE publishing_name=%s,"
                       + "for_publishing=%s,published=%s,label=%s,"
                       + "requester=%s,notes=%s",
        }
CONN = dict()
CURSOR = dict()
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# General
COUNT = {'deleted': 0, 'error': 0, 'inserted': 0, 'read': 0, 'skipped': 0,
         'updated': 0}
LINE_ID = dict()


# pylint: disable=W0703
def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        LOGGER.critical('MySQL error [%d]: %s', err.args[0], err.args[1])
    except IndexError:
        LOGGER.critical('MySQL error: %s', err)
    sys.exit(-1)


def db_connect(rdb):
    """ Connect to specified database
        Keyword arguments:
          rdb: database key
    """
    LOGGER.info("Connecting to %s on %s", rdb['name'], rdb['host'])
    try:
        conn = MySQLdb.connect(host=rdb['host'], user=rdb['user'],
                               passwd=rdb['password'], db=rdb['name'])
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
        LOGGER.error('Status: %s (%s)', str(req.status_code), url)
        sys.exit(-1)
    return req.json()


def initialize_program():
    """ Get configuration data
    """
    # pylint: disable=W0603
    global CONFIG
    dbc = call_responder('config', 'config/db_config')
    data = dbc['config']
    (CONN['sage'], CURSOR['sage']) = db_connect(data['sage'][ARG.MANIFOLD])
    dbc = call_responder('config', 'config/rest_services')
    CONFIG = dbc['config']


def get_line_id(line):
    """ Get a line's ID """
    if line in LINE_ID:
        return LINE_ID[line]
    try:
        CURSOR['sage'].execute(READ['LINEID'], [line])
        lrow = CURSOR['sage'].fetchone()
    except MySQLdb.Error as err:
        sql_error(err)
    if not lrow:
        LOGGER.error("Line %s is not in SAGE", line)
        COUNT['error'] += 1
        return 0
    LINE_ID[line] = lrow[0]
    return lrow[0]


def set_publishing_name(line, row):
    """ Fix column data """
    for idx in range(3, 9):
        if row[idx] is None:
            row[idx] = ''
    for idx in range(3, 6):
        if row[idx]:
            row[idx] = 1 if row[idx][0].lower() == 'y' else 0
        else:
            row[idx] = 0
    LOGGER.debug(row)
    publishing_name = row[2]
    # Get line ID
    line_id = get_line_id(line)
    if not line_id:
        return
    # Insert/update record
    short_line = line.split('_')[1] if '_' in line else line
    if re.search(r"IS\d+", short_line):
        short_line = short_line.replace('IS', 'SS')
    default = 1 if short_line == publishing_name else 0
    # Is this an insertion?
    try:
        CURSOR['sage'].execute(READ['EXISTS'], (line_id, publishing_name))
        lrow = CURSOR['sage'].fetchone()
    except MySQLdb.Error as err:
        sql_error(err)
    COUNT['updated' if lrow else 'inserted'] += 1
    if not lrow:
        LOGGER.info("Publishing name %s for %s", publishing_name, line)
    LOGGER.debug("Publishing name %s for %s", publishing_name, line)
    LOGGER.debug(WRITE['PUBLISHING'], line_id, *row[slice(1, 9)], default, *row[slice(2, 8)])
    try:
        CURSOR['sage'].execute(WRITE['PUBLISHING'], (line_id, *row[slice(1, 9)], default,
                                                     *row[slice(2, 8)],))
    except MySQLdb.Error as err:
        sql_error(err)


def update_publishing_names():
    """ Get mapping of __kp_UniqueID to stock name """
    stockmap = dict()
    LOGGER.info("Fetching stock names from Fly Core")
    stocks = call_responder('flycore', '?request=named_stocks')
    for stock in stocks['stocks']:
        stockmap[stock] = stocks['stocks'][stock]['Stock_Name']
    LOGGER.info("Found %d named stocks in Fly Core", len(stockmap))
    # Get publishing names
    flycore_sn = dict()
    LOGGER.info("Fetching publishing names from Fly Core")
    response = call_responder('flycore', '?request=publishing_names')
    allnames = response['publishing']
    LOGGER.info("Found %d publishing names in Fly Core", len(allnames))
    for row in allnames:
        # _kf_parent_UID, __kp_name_serial_number, all_names, for_publishing,
        # published, label, who, notes, create_date
        COUNT['read'] += 1
        flycore_sn[row[1]] = 1
        if row[0] in stockmap and ("\r" in row[2] or "\n" in row[2]):
            COUNT['error'] += 1
            LOGGER.error("%s has a publishing name with carriage returns", stockmap[row[0]])
            continue
        if row[0] in stockmap and stockmap[row[0]] != row[2]:
            line = stockmap[row[0]]
            set_publishing_name(line, row)
        else:
            COUNT['skipped'] += 1
    # Check for deletions
    sage_source = dict()
    try:
        CURSOR['sage'].execute(READ['SOURCE'])
        rows = CURSOR['sage'].fetchall()
    except MySQLdb.Error as err:
        sql_error(err)
    for row in rows:
        # source_id, id, line
        if not re.search(r"IS\d+", row[2]):
            sage_source[row[0]] = row[1]
    LOGGER.info("Found %d records in FLYF2", len(flycore_sn))
    LOGGER.info("Found %d records in SAGE", len(sage_source))
    for pid in sage_source:
        if pid not in flycore_sn:
            LOGGER.warning("%s is present in SAGE but not in Fly Core", pid)
            LOGGER.debug(WRITE['DELETE'], pid)
            try:
                CURSOR['sage'].execute(WRITE['DELETE'], (pid,))
            except MySQLdb.Error as err:
                sql_error(err)
            COUNT['deleted'] += 1
    if ARG.WRITE:
        CONN['sage'].commit()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Sync publishing names from Fly Core to SAGE")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', help='Database manifold')
    PARSER.add_argument('--all', dest='ALL', action='store_true',
                        default=False, help='Process all lines')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False,
                        help='Flag, Actually modify database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()

    LOGGER = colorlog.getLogger()
    if ARG.DEBUG:
        LOGGER.setLevel(colorlog.colorlog.logging.DEBUG)
    elif ARG.VERBOSE:
        LOGGER.setLevel(colorlog.colorlog.logging.INFO)
    else:
        LOGGER.setLevel(colorlog.colorlog.logging.WARNING)
    HANDLER = colorlog.StreamHandler()
    HANDLER.setFormatter(colorlog.ColoredFormatter())
    LOGGER.addHandler(HANDLER)

    initialize_program()
    update_publishing_names()
    print("Publishing names read:     %d" % COUNT['read'])
    print("Publishing names inserted: %d" % COUNT['inserted'])
    print("Publishing names updated:  %d" % COUNT['updated'])
    print("Publishing names deleted:  %d" % COUNT['deleted'])
    print("Publishing names skipped:  %d" % COUNT['skipped'])
    print("Publishing names in error: %d" % COUNT['error'])
    sys.exit(0)
