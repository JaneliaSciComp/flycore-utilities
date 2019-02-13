import argparse
import json
import select
import sys
import colorlog
import requests
import MySQLdb

# Database
READ = {'main': "SELECT id FROM line WHERE name=%s",
        'limages': "SELECT NULL,line,COUNT(1) FROM image_data_mv WHERE line=%s",
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
    if req.status_code == 200:
        return req.json()
    else:
        logger.error('Status: %s', str(req.status_code))
        sys.exit(-1)


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


def process_line(line_id, line, newline):
    try:
        CURSOR['sage'].execute(READ['limages'], (line,))
    except MySQLdb.Error as err:
        sql_error(err)
    row = CURSOR['sage'].fetchone()
    images = 0
    if row:
        aline = row[1]
        images = row[2] if row[2] else 0
    try:
        CURSOR['sage'].execute(READ['lassays'], (line,))
    except MySQLdb.Error as err:
        sql_error(err)
    row = CURSOR['sage'].fetchone()
    assays = 0
    if row:
        aline = row[0]
        assays = row[1] if row[1] else 0
    if assays:
        logger.critical("Can't modify %s, is annotated: %s)" % (line, assays))
        return
    delete_publishing(line_id)
    if images:
        if not newline:
            logger.critical("Can't rename %s (%d images), new name is unknown" % (line, images))
        else:
            logger.debug("Will rename %s to %s" % (line, newline))
            rename_line(line_id, newline)
            logger.info("Renamed %s to %s" % (line, newline))
    else:
        if not newline:
            logger.debug("Will delete %s" % line)
            delete_line(line_id)
            logger.info("Deleted %s" % line)
        else:
            logger.debug("Will rename %s to %s" % (line, newline))
            rename_line(line_id, newline)
            logger.info("Renamed %s to %s" % (line, newline))

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
        if len(filerow.split("\t")) == 1:
            line = filerow
        else:
            [line, newline] = filerow.split("\t")
            if line == newline:
                logger.critical("Line name and new line name match: %s", line)
                continue
        logger.debug("Read %s" % line)
        try:
            CURSOR['sage'].execute(READ['main'], (line,))
        except MySQLdb.Error as err:
            sql_error(err)
        rows = CURSOR['sage'].fetchall()
        if len(rows) == 0:
            logger.warning("Line %s is not in SAGE" % line)
        elif len(rows) > 1:
            logger.critical("Line %s is in SAGE more than once" % line)
        else:
            line_id = rows[0][0]
            process_line(line_id, line, newline)
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
    sys.exit(0)
