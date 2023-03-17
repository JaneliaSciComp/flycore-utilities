''' sync_flyf_initial_splits.py
    Synchronize initial split (IS) lines from FLYF2 crosses to SAGE.
    SAGE line, line properties, and line relationships are updated.
'''
import argparse
import sys
import colorlog
import requests
import MySQLdb

# Database
READ = {'LINE': "SELECT id FROM line WHERE name=%s",
        'RELATIONSHIP': "SELECT object_id FROM line_relationship_vw "
                        + "WHERE subject=%s AND relationship='child_of'"
       }
WRITE = {'ILINE': "INSERT INTO line (name,lab_id,organism_id) VALUES (%s,"
                  + "getCvTermId('lab','flylight',''),1)",
         'IPROP': "INSERT INTO line_property (line_id,type_id,value) VALUES "
                  + "(%s,getCvTermId('line',%s,''),%s)",
         'UPROP': "UPDATE line_property SET value=%s WHERE line_id=%s AND "
                  + "type_id=getCvTermId('line',%s,'')",
         'DELREL': "DELETE FROM line_relationship WHERE subject_id=%s OR object_id=%s",
         'CREATEREL': "CALL createLineRelationship(%s,%s)",
        }
CONN = dict()
CURSOR = dict()
# Configuration
CONFIG = {'config': {'url': 'http://config.int.janelia.org/'}}
# General
COUNT = {'error': 0, 'inserted': 0, 'read': 0, 'skipped': 0}
PROPS = {"hide": "Y",
         "flycore_permission": "Class 3 (Written)",
         "flycore_project": "Split_GAL4",
         "flycore_project_subcat": "InitialSplits",
         "flycore_lab": "Fly Light"
        }

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


def change_relationships(line_id, split_half, genotype):
    """ Change a line's relationships
        Keyword arguments:
          line_id: line ID
          split_half: split half dictionary
          genotype: line genotype
        Returns 1 for success, 0 for failure
    """
    LOGGER.debug(WRITE['DELREL'], line_id, line_id)
    try:
        CURSOR['sage'].execute(WRITE['DELREL'], (line_id, line_id))
    except Exception as exc:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(exc).__name__, exc.args)
        LOGGER.error(message)
        return 0
    rowcount = CURSOR['sage'].rowcount
    LOGGER.debug("Deleted %d relationships for line ID %s", rowcount, line_id)
    retcode = create_relationships(line_id, split_half)
    if not retcode:
        return 0
    LOGGER.debug(WRITE['UPROP'], genotype, line_id, 'flycore_alias')
    try:
        CURSOR['sage'].execute(WRITE['UPROP'], (genotype, line_id, 'flycore_alias'))
    except Exception as exc:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(exc).__name__, exc.args)
        LOGGER.error(message)
        return 0
    LOGGER.debug("Updated flycore_alias (%s) for line ID %s", genotype, line_id)
    return 1


def create_relationships(line_id, split_half):
    """ Create line relationships
        Keyword arguments:
          line_id: line ID
          split_half: split half dictionary
        Returns 1 for success, 0 for failure
    """
    for parent_id in split_half:
        LOGGER.debug(WRITE['CREATEREL'], line_id, parent_id)
        try:
            CURSOR['sage'].execute(WRITE['CREATEREL'], (line_id, parent_id))
        except Exception as exc:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(exc).__name__, exc.args)
            LOGGER.error(message)
            return 0
        rowcount = CURSOR['sage'].rowcount
        LOGGER.debug("Created %d parent/child relationship from %s to %s",
                     rowcount, line_id, parent_id)
    return 1


def insert_lineprop(line_id, term, value):
    """ Insert a new line property
        Keyword arguments:
          line_id: line ID
          term: line property name
          value: line property value
        Returns 0 for success, 1 for failure
    """
    LOGGER.debug(WRITE['IPROP'], line_id, term, value)
    try:
        CURSOR['sage'].execute(WRITE['IPROP'], (line_id, term, value))
    except Exception as exc:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(exc).__name__, exc.args)
        LOGGER.error(message)
        return 1
    return 0


def insert_line(split, split_half):
    """ Insert a new line
        Keyword arguments:
          split: split dictionary
          split_half: split half dictionary
    """
    LOGGER.debug(WRITE['ILINE'], split['line'])
    try:
        CURSOR['sage'].execute(WRITE['ILINE'], [split['line']])
    except MySQLdb.Error as err:
        sql_error(err)
    line_id = CURSOR['sage'].lastrowid
    if line_id:
        LOGGER.info("Inserted line %s (%s)", split['line'], line_id)
        retcode = []
        ret = create_relationships(line_id, split_half)
        errsum = 0 if ret else 1
        retcode.append(errsum)
        if not errsum:
            for prop in PROPS:
                retcode.append(insert_lineprop(line_id, prop, PROPS[prop]))
            retcode.append(insert_lineprop(line_id, 'flycore_alias', split['genotype']))
            for tot in retcode:
                errsum += tot
        COUNT['error' if errsum else 'inserted'] += 1
    else:
        COUNT['error'] += 1


def check_split_halves(split):
    ''' Check split halves. Errors are as follows:
        - Duplicate split halves
        - Something other than two split halves
        - One or more split halves not found
        Keyword arguments:
          split: split dictionary
    '''
    halves = split['genotype'].split('-x-')
    split_half = dict()
    if len(halves) == 2 and halves[0] == halves[1]:
        LOGGER.error("Duplicate split halves for %s", split['line'])
        COUNT['error'] += 1
        return dict()
    for half in halves:
        try:
            CURSOR['sage'].execute(READ['LINE'], [half])
        except MySQLdb.Error as err:
            sql_error(err)
        row = CURSOR['sage'].fetchone()
        if row:
            split_half[row[0]] = 1
            LOGGER.debug("Split half %s found (%s)", half, row[0])
        else:
            LOGGER.error("Split half %s was not found for %s (%s)", half,
                         split['genotype'], split['line'])
            COUNT['error'] += 1
    if len(split_half) != 2:
        return dict()
    return split_half


def check_existing_line(line_id, split, split_half):
    """ Update relationships for an existing line if necessary
        Keyword arguments:
          line_id: line ID
          split: split dictionary
          split_half: split half dictionary
    """
    error = 0
    try:
        CURSOR['sage'].execute(READ['RELATIONSHIP'], [split['line']])
    except MySQLdb.Error as err:
        sql_error(err)
    rows = CURSOR['sage'].fetchall()
    if len(rows) != 2:
        error += 1
    for sid in rows:
        if sid[0] not in split_half:
            error += 1
    if error:
        LOGGER.debug("Changing relationships for %s (%s)", split['line'], line_id)
        err = change_relationships(line_id, split_half, split['genotype'])
        if not err:
            LOGGER.error("Could not update relationships for line %s (%s)",
                         split['line'], line_id)
            COUNT['error'] += 1
    else:
        LOGGER.debug("Found line %s - skipping load", split['line'])
        COUNT['skipped'] += 1


def update_initial_splits():
    """ Synchronize ibitial split lines """
    LOGGER.info("Fetching initial splits from Fly Core")
    if ARG.LINE:
        splits = call_responder('flycore', '?request=initial_split;line=' + ARG.LINE)
    else:
        splits = call_responder('flycore', '?request=initial_splits')
    LOGGER.info("Found %d initial splits in Fly Core", len(splits['splits']))
    for split in splits['splits']:
        try:
            CURSOR['sage'].execute(READ['LINE'], [split['line']])
        except MySQLdb.Error as err:
            sql_error(err)
        row = CURSOR['sage'].fetchone()
        line_id = row[0] if row else 0
        if (line_id and not ARG.ALL):
            continue
        COUNT['read'] += 1
        LOGGER.info("Cross barcode %s: %s %s", split['cross_barcode'],
                    split['line'], split['genotype'])
        split_half = check_split_halves(split)
        if len(split_half) != 2:
            continue
        if line_id:
            check_existing_line(line_id, split, split_half)
        else:
            insert_line(split, split_half)

    if ARG.WRITE:
        CONN['sage'].commit()
    print("Split crosses:  %d" % len(splits['splits']))


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(description="Sync initial split crosses from Fly Core to SAGE")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', help='Database manifold')
    PARSER.add_argument('--line', dest='LINE', action='store',
                        default='', help='Line')
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
    update_initial_splits()
    print("Lines read:     %d" % COUNT['read'])
    print("Lines inserted: %d" % COUNT['inserted'])
    print("Lines skipped:  %d" % COUNT['skipped'])
    print("Lines in error: %d" % COUNT['error'])
    sys.exit(0)
