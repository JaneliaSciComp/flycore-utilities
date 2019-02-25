import argparse
import json
import select
import sys
import colorlog
import requests
import MySQLdb

# Database
READ = {'crosses': "SELECT cross_type FROM cross_event_vw WHERE cross_barcode=%s",
        'main': "SELECT id FROM line WHERE name=%s",
        'limages': "SELECT line, id FROM image_data_mv WHERE cross_barcode=%s",
        'lassays': "SELECT line,sessions FROM line_summary_vw WHERE line=%s"
       }
WRITE = {'publishing': "DELETE FROM publishing_name WHERE line_id=%s",
         'relationship': "DELETE FROM line_relationship WHERE subject_id=%s OR object_id=%s",
         'lineprop': "DELETE FROM line_property WHERE line_id=%s",
         'event': "DELETE FROM line_event WHERE line_id=%s",
         'line': "DELETE FROM line WHERE id=%s",
         'rename': "UPDATE line SET name=%s WHERE id=%s",
         'relink': "UPDATE image SET line_id=%s WHERE line_id=%s"
        }
CONN = dict()
CURSOR = dict()

# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
COUNT = {"deleted": 0, "error": 0, "read": 0, "renamed": 0}


def sql_error(err):
    """ Log a critical SQL error and exit """
    try:
        logger.critical('MySQL error [%d]: %s', err.args[0], err.args[1])
    except IndexError:
        logger.critical('MySQL error: %s', err)
    sys.exit(-1)


def db_connect(db):
    """ Connect to a database
        Keyword arguments:
        db: database dictionary
    """
    logger.debug("Connecting to %s on %s", db['name'], db['host'])
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
        logger.error('Status: %s', str(req.status_code))
        sys.exit(-1)
    return req.json()


def initialize_program():
    """ Initialize databases """
    global CONFIG
    dbc = call_responder('config', 'config/db_config')
    data = dbc['config']
    (CONN['sage'], CURSOR['sage']) = db_connect(data['sage']['prod'])
    dbc = call_responder('config', 'config/rest_services')
    CONFIG = dbc['config']


def delete_publishing(line_id):
    logger.debug(WRITE['publishing'] % (line_id,))
    try:
        CURSOR['sage'].execute(WRITE['publishing'], (line_id,))
        rowcount = CURSOR['sage'].rowcount
        if (rowcount):
            logger.debug("Deleted publishing names (%d) for line ID %s" % (rowcount, line_id))
    except MySQLdb.Error as err:
        sql_error(err)


def delete_relationship(line_id):
    logger.debug(WRITE['relationship'] % (line_id, line_id))
    try:
        CURSOR['sage'].execute(WRITE['relationship'], (line_id, line_id))
        rowcount = CURSOR['sage'].rowcount
        if (rowcount):
            logger.debug("Deleted line relationships (%d) for line ID %s" % (rowcount, line_id))
    except MySQLdb.Error as err:
        sql_error(err)


def delete_event(line_id):
    logger.debug(WRITE['event'] % (line_id,))
    try:
        CURSOR['sage'].execute(WRITE['event'], (line_id,))
        rowcount = CURSOR['sage'].rowcount
        if (rowcount):
            logger.debug("Deleted line events (%d) for line ID %s" % (rowcount, line_id))
    except MySQLdb.Error as err:
        sql_error(err)


def delete_lineprop(line_id):
    logger.debug(WRITE['lineprop'] % (line_id,))
    try:
        CURSOR['sage'].execute(WRITE['lineprop'], (line_id,))
        rowcount = CURSOR['sage'].rowcount
        if (rowcount):
            logger.debug("Deleted line properties (%d) for line ID %s" % (rowcount, line_id))
    except MySQLdb.Error as err:
        sql_error(err)


def delete_line(line_id):
    delete_relationship(line_id)
    delete_event(line_id)
    delete_lineprop(line_id)
    logger.debug(WRITE['line'] % (line_id,))
    try:
        CURSOR['sage'].execute(WRITE['line'], (line_id,))
        rowcount = CURSOR['sage'].rowcount
        if (rowcount):
            logger.debug("Deleted %d line for line ID %s" % (rowcount, line_id))
    except MySQLdb.Error as err:
        sql_error(err)


def rename_line(line_id, newline):
    # Does the new line already exist?
    try:
        CURSOR['sage'].execute(READ['main'], (newline,))
    except MySQLdb.Error as err:
        sql_error(err)
    row = CURSOR['sage'].fetchone()
    if row:
        logger.debug("New line %s is already in SAGE (%s)", newline, row[0])
        old_line_id = line_id
        line_id = row[0]
        try:
            CURSOR['sage'].execute(WRITE['relink'], (line_id, old_line_id))
            rowcount = CURSOR['sage'].rowcount
            if (rowcount):
                logger.debug("Changed %d images from line ID %s to %s" % (rowcount, old_line_id, line_id))
        except MySQLdb.Error as err:
            sql_error(err)
        delete_line(old_line_id)
    else:
        logger.debug(WRITE['rename'] % (newline, line_id))
        try:
            CURSOR['sage'].execute(WRITE['rename'], (newline, line_id))
            rowcount = CURSOR['sage'].rowcount
            if (rowcount):
                logger.debug("Changed %d line name to %s for line ID %s" % (rowcount, newline, line_id))
        except MySQLdb.Error as err:
            sql_error(err)

def process_cross(cross_id, line_id, line, newline):
    # Look for assays
    try:
        CURSOR['sage'].execute(READ['lassays'], (line,))
    except MySQLdb.Error as err:
        sql_error(err)
    row = CURSOR['sage'].fetchone()
    if row:
        assays = row[1] if row[1] else 0
    if assays:
        logger.critical("Can't modify %s, is annotated: %s)" % (line, assays))
        COUNT['error'] += 1
        return
    # Find crosses
    try:
        CURSOR['sage'].execute(READ['crosses'], (cross_id, ))
    except MySQLdb.Error as err:
        sql_error(err)
    row = CURSOR['sage'].fetchone()
    if row:
        logger.info("%s cross found for ID %s", row[0], cross_id)
    else:
        logger.warning("There are no crosses associated with cross ID %s (%s)", cross_id, line)
        COUNT['error'] += 1
        return
    # Search for images
    try:
        CURSOR['sage'].execute(READ['limages'], (cross_id, ))
    except MySQLdb.Error as err:
        sql_error(err)
    rows = CURSOR['sage'].fetchall()
    if not rows:
        logger.warning("There are no images associated with cross ID %s (%s)", cross_id, line)
        COUNT['error'] += 1
        return
    dlines = dict()
    for row in rows:
        if row[0] != line:
            logger.error("Cross ID %s was TMOGged for line %s, but should be associated with %s", cross_id, row[0], line)
            COUNT['error'] += 1
            return
        logger.info("Found image %s for cross ID %s, line %s", row[1], cross_id, line)
        print("<<Add code here to change lines associated with the above image ID>>")


def process_file(filename):
    if (not filename) and (not select.select([sys.stdin,],[],[],0.0)[0]):
        logger.critical('You must either specify a file or pass data in through STDIN')
        sys.exit(-1)        
    try:
        filehandle = open(filename, "r") if filename else sys.stdin
    except Exception as e:
        logger.critical('Failed to open input: '+ str(e))
        sys.exit(-1)
    for filerow in filehandle:
        filerow = filerow.rstrip()
        newline = ''
        COUNT['read'] += 1
        if len(filerow.split("\t")) == 2:
            [cross_id, line] == filerow.split("\t")
            logger.critical("Line name and new line name match: %s", line)
            COUNT['error'] += 1
            continue
        elif len(filerow.split("\t")) == 3:
            [cross_id, line, newline] = filerow.split("\t")
            if line == newline:
                logger.critical("Line name and new line name match: %s", line)
                COUNT['error'] += 1
                continue
        else:
            logger.critical("Badly formatted line: %s", filerow)
            COUNT['error'] += 1
            continue
        logger.debug("Read %s" % line)
        try:
            CURSOR['sage'].execute(READ['main'], (line,))
        except MySQLdb.Error as err:
            sql_error(err)
        rows = CURSOR['sage'].fetchall()
        if len(rows) == 0:
            logger.warning("Line %s is not in SAGE" % line)
            COUNT['error'] += 1
        elif len(rows) > 1:
            logger.critical("Line %s is in SAGE more than once" % line)
            COUNT['error'] += 1
        else:
            line_id = rows[0][0]
            process_cross(cross_id, line_id, line, newline)
    if filehandle is not sys.stdin:
        filehandle.close()
    if ARG.WRITE:
        CONN['sage'].commit()


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Check lines/cross barcodes for associated imagery and behavioral data")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        default='', help='File containing lines or cross barcodes')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write changes to database')
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
    process_file(ARG.FILE)
    print("Lines read: %d" % COUNT['read'])
    print("Lines deleted: %d" % COUNT['deleted'])
    print("Lines renamed: %d" % COUNT['renamed'])
    print("Errors: %d" % COUNT['error'])
    sys.exit(0)
