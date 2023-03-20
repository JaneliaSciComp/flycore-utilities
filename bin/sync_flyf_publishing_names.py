''' sync_flyf_publishing_names.py
    Synchronize publishing names from FLYF2 crosses to SAGE.
'''
import argparse
import re
import sys
import colorlog
import requests
import MySQLdb
from tqdm import tqdm

# Database
READ = {'EXISTS': "SELECT id FROM publishing_name WHERE line_id=%s AND publishing_name=%s",
        'LINEID': "SELECT id FROM line WHERE name=%s",
        'SOURCE': "SELECT source_id,id,line FROM publishing_name_vw",
       }
WRITE = {'PUBLISHING': "INSERT INTO publishing_name (line_id,source_id,"
                       + "publishing_name,for_publishing,published,label,display_genotype,"
                       + "requester,notes,source_create_date,preferred_name) "
                       + "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON "
                       + "DUPLICATE KEY UPDATE publishing_name=%s,"
                       + "for_publishing=%s,published=%s,label=%s,"
                       + "display_genotype=%s,requester=%s,notes=%s",
        }
CONN = {}
CURSOR = {}
# Configuration
CONFIG = {'config': {'url': 'https://config.int.janelia.org/'}}
# General
COUNT = {'deleted': 0, 'format': 0, 'inserted': 0, 'not_stock': 0,
         'read': 0, 'skipped': 0, 'type': 0, 'flags': 0, 'updated': 0,
         'publishing_name': 0, 'genotype': 0}
LINE_ID = {}
WARNINGS = []

# pylint: disable=W0703,R1710

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
        req = requests.get(url, timeout=90)
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
    for idx in range(3, 10):
        if row[idx] is None:
            row[idx] = ''
    for idx in range(3, 7):
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
    utype = 'genotype' if row[6] else 'publishing_name'
    COUNT[utype] += 1
    # Is this an insertion?
    try:
        CURSOR['sage'].execute(READ['EXISTS'], (line_id, publishing_name))
        lrow = CURSOR['sage'].fetchone()
    except MySQLdb.Error as err:
        sql_error(err)
    COUNT['updated' if lrow else 'inserted'] += 1
    if not lrow:
        LOGGER.info("New %s %s for %s", utype, publishing_name, line)
    LOGGER.debug("%s %s for %s", utype, publishing_name, line)
    LOGGER.debug(WRITE['PUBLISHING'], line_id, *row[slice(1, 10)], default, *row[slice(2, 9)])
    try:
        CURSOR['sage'].execute(WRITE['PUBLISHING'], (line_id, *row[slice(1, 10)], default,
                                                     *row[slice(2, 9)],))
    except MySQLdb.Error as err:
        print(row)
        sql_error(err)


def error_condition(stockmap, row):
    if ARG.LINE and stockmap[row[0]] != ARG.LINE:
        COUNT['skipped'] += 1
        return True
    elif not row[3]:
        # Skip names with for_publishing is set to No
        COUNT['flags'] += 1
        WARNINGS.append(f"{row[0]} {row[2]} should be removed")
        return True
    elif ARG.FILTER and not row[2].startswith(ARG.FILTER):
        COUNT['skipped'] += 1
        return True
    elif (not re.search(r"^w[;,\-\+\[]", row[2])) and row[6]:
        COUNT['type'] += 1
        WARNINGS.append(f"{row[0]} {row[2]} may be a publishing name but is flagged as a genotype")
    elif re.search(r"^w[;,\-\+\[]", row[2]) and not row[6]:
        COUNT['type'] += 1
        WARNINGS.append(f"{row[0]} {row[2]} may be a genotype but is flagged as a publishing name")
    return False


def process_single_name(stockmap, row):
    """ Process a single publishing name """
    if not error_condition(stockmap, row):
        if stockmap[row[0]] != row[2]:
            line = stockmap[row[0]]
            set_publishing_name(line, row)
        else:
            COUNT['skipped'] += 1


def update_publishing_names():
    stockmap = {}
    if not ARG.LINE:
        """ Get mapping of __kp_UniqueID to stock name """
        LOGGER.info("Fetching stock names from Fly Core")
        stocks = call_responder('flycore', '?request=named_stocks')
        for stock in stocks['stocks']:
            stockmap[stock] = stocks['stocks'][stock]['Stock_Name']
        if not stockmap:
            LOGGER.critical("No stocks found in FLYF2")
            sys.exit(-1)
        LOGGER.info("Found %d named stocks in Fly Core", len(stockmap))
    # Get publishing names
    flycore_sn = {}
    LOGGER.info("Fetching publishing names from Fly Core")
    if ARG.LINE:
        response = call_responder('flycore', f"?request=publishing_names_join;line={ARG.LINE}")
    else:
        response = call_responder('flycore', f"?request=publishing_names_sync;days={ARG.DAYS}")
    allnames = response['publishing']
    LOGGER.info("Found %d publishing names in Fly Core", len(allnames))
    if not allnames:
        LOGGER.critical("No new names found in FLYF2")
        sys.exit(-1)
    if ARG.LINE:
        stockmap[allnames[0][0]] = ARG.LINE
    for row in tqdm(allnames):
        # _kf_parent_UID, __kp_name_serial_number, all_names, for_publishing,
        # published, label, display_genotype, who, notes, create_date
        COUNT['read'] += 1
        flycore_sn[row[1]] = 1
        if row[0] in stockmap:
            if ("\r" in row[2] or "\n" in row[2]):
                COUNT['format'] += 1
                LOGGER.error("%s has a publishing name with carriage returns", stockmap[row[0]])
                continue
            process_single_name(stockmap, row)
        else:
            COUNT['not_stock'] += 1
    # Check for deletions
    sage_source = {}
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
    if ARG.WRITE:
        CONN['sage'].commit()
    if WARNINGS:
        with open("publishing_name_sync.txt", "w", encoding="ascii") as outstream:
            for line in WARNINGS:
                outstream.write(f"{line}\n")


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Sync publishing names from Fly Core to SAGE")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', help='Database manifold')
    PARSER.add_argument('--days', dest='DAYS', action='store', type=int,
                        default=3, help='Number of days to go back [3]')
    PARSER.add_argument('--filter', dest='FILTER', action='store',
                        help='Publishing name filter (starts with)')
    PARSER.add_argument('--line', dest='LINE', action='store',
                        help='Single line to process')
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
    update_publishing_names()
    print(f"Names read:               {COUNT['read']}")
    print(f"Publishing names:         {COUNT['publishing_name']}")
    print(f"Genotypes:                {COUNT['genotype']}")
    print(f"Names inserted:           {COUNT['inserted']}")
    print(f"Names updated:            {COUNT['updated']}")
    print(f"No stock:                 {COUNT['not_stock']}")
    print(f"Names with bad flags:     {COUNT['flags']}")
    print(f"Names with type mismatch: {COUNT['type']}")
    print(f"Names skipped:            {COUNT['skipped']}")
    print(f"Names with format errors: {COUNT['format']}")
    sys.exit(0)
