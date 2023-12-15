''' add_initial_split.py
    Add initial splits to publishing_name table for a release
'''
import argparse
from operator import attrgetter
import os
import re
import sys
import MySQLdb
import requests
from simple_term_menu import TerminalMenu
from tqdm import tqdm
from jrc_common import jrc_common as JRC

# Databases
DB = {}
READ = {"PN": "SELECT publishing_name FROM publishing_name_vw WHERE line=%s AND "
              + "publishing_name=%s",
        "LID": "SELECT id FROM line WHERE name=%s",
       }
WRITE = {"NAME": "INSERT INTO publishing_name (publishing_name,line_id,for_publishing,published,"
                 + "display_genotype,requester,notes,preferred_name) VALUES (%s,%s,1,1,0,'',%s,1)",
        }
# General
COUNT = {"lines": 0, "inserted": 0}

def terminate_program(msg=None):
    """ Log an optional error to output, close files, and exit
        Keyword arguments:
          err: error message
        Returns:
           None
    """
    if msg:
        LOGGER.critical(msg)
    sys.exit(-1 if msg else 0)


def initialize_program():
    """ Initialize the program
        Keyword arguments:
          None
        Returns:
          None
    """
    # pylint: disable=broad-exception-caught)
    try:
        dbconfig = JRC.get_config("databases")
    except Exception as err:
        terminate_program(err)
    source = 'sage'
    dbo = attrgetter(f"{source}.{ARG.MANIFOLD}.write")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DB[source] = JRC.connect_database(dbo)
    except Exception as err:
        terminate_program(err)
    if not ARG.RELEASE:
        try:
            DB['sage']["cursor"].execute("SELECT DISTINCT value FROM image_property_vw " \
                                         + "WHERE type='alps_release' AND value != '' ORDER BY 1")
            rows = DB['sage']["cursor"].fetchall()
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
        rlist = [row['value'] for row in rows]
        terminal_menu = TerminalMenu(rlist, title="Select a release:")
        chosen = terminal_menu.show()
        if chosen is None:
            LOGGER.critical("You must specify a release")
            sys.exit(-1)
        ARG.RELEASE = rlist[chosen]


def call_responder(server, endpoint, authenticate=False):
    """ Call a REST API
        Keyword arguments:
          server: server name
          endpoint: endpoint
          authenticate: authenticate to server
        Returns:
          JSON
    """
    url = attrgetter(f"{server}.url")(REST) + endpoint
    try:
        if authenticate:
            headers = {"Content-Type": "application/json",
                       "Authorization": "Bearer " + os.environ["NEUPRINT_JWT"]}
            req = requests.get(url, headers=headers, timeout=10)
        else:
            req = requests.get(url, timeout=10)
    except requests.exceptions.RequestException as err:
        terminate_program(err)
    if req.status_code == 200:
        return req.json()
    terminate_program(f"Status: {str(req.status_code)}")


def add_is_lines():
    """ Add initial splits for a release
        Keyword arguments:
          None
        Returns:
          None
    """
    samples = call_responder('jacs', 'process/release/' + ARG.RELEASE)
    LOGGER.info("Samples: %d", len(samples[0]['children']))
    lines = {}
    for smp in tqdm(samples[0]['children'], desc='Getting lines'):
        response = call_responder('jacs', 'data/sample?sampleId=' + smp.replace("Sample#", ""))
        if re.search(r"_I[SL]\d+", response[0]['line']):
            lines[response[0]['line']] = True
    LOGGER.info("Lines: %d", len(lines))
    COUNT['lines'] = len(lines)
    for line in tqdm(lines, desc='Inserting names'):
        pname = re.sub(r"^[A-Z0-9]+_", "", line)
        try:
            DB['sage']["cursor"].execute(READ['PN'], (line, pname))
            rows = DB['sage']["cursor"].fetchall()
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
        if not rows:
            try:
                DB['sage']["cursor"].execute(READ['LID'], (line,))
                lid = DB['sage']["cursor"].fetchone()
            except MySQLdb.Error as err:
                terminate_program(JRC.sql_error(err))
            if not lid:
                LOGGER.error("Could not find %s in line table", line)
                continue
            try:
                DB['sage']["cursor"].execute(WRITE['NAME'], (pname, lid['id'], ARG.RELEASE))
            except MySQLdb.Error as err:
                terminate_program(JRC.sql_error(err))
            COUNT['inserted'] += 1
            LOGGER.debug("Inserted %s", pname)
    print(f"Lines found:    {COUNT['lines']}")
    print(f"Names inserted: {COUNT['inserted']}")
    if ARG.WRITE:
        DB['sage']['conn'].commit()



if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="Perform a full process check")
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['dev', 'prod'], help='manifold')
    PARSER.add_argument('--release', dest='RELEASE', action='store',
                        help='ALPS release')
    PARSER.add_argument('--write', dest='WRITE', action='store_true',
                        default=False, help='Actually write to database')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    REST = JRC.get_config("rest_services")
    initialize_program()
    add_is_lines()
    terminate_program()
