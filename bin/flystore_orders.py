""" flystore_order.py
    Produce a report of FlyStore orders.
"""

import argparse
import datetime
from operator import attrgetter
import sys
import MySQLdb
import pandas as pd
import jrc_common.jrc_common as JRC

# pylint: disable=logging-fstring-interpolation

# Database
READ = {"RID": "SELECT COUNT(1) AS c FROM FlyStore_line_order_history_vw WHERE RobotID=%s",
        "STOCK": "SELECT loh.stock_name,sf.Project AS project,sf.Project_SubCat AS subcat"
                 + ",loh.RobotID,sf.Genotype_GSI_Name_PlateWell AS genotype,sf.Lab_ID AS labid"
                 + ",YEAR(date_filled) AS year,COUNT(1) AS cnt FROM "
                 + "FlyStore_line_order_history_vw loh LEFT OUTER JOIN StockFinder sf ON "
                 + "(sf.Stock_Name=loh.stock_name) WHERE "
                 + "loh.stock_name IS NOT NULL AND loh.stock_name != 'KEEP EMPTY' AND "
                 + "date_filled IS NOT NULL GROUP BY 1,2,3,4,5,6,7",
        "ROBOT": "SELECT loh.stock_name,sf.Project AS project,sf.Project_SubCat AS subcat"
                 + ",loh.RobotID,sf.Genotype_GSI_Name_PlateWell AS genotype,sf.Lab_ID AS labid"
                 + ",YEAR(date_filled) AS year,COUNT(1) AS cnt FROM "
                 + "FlyStore_line_order_history_vw loh LEFT OUTER JOIN StockFinder sf ON "
                 + "(sf.RobotID=loh.RobotID) WHERE date_filled IS NOT NULL "
                 + "GROUP BY 1,2,3,4,5,6,7",
       }
DB = {}

# -----------------------------------------------------------------------------

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
    """ Initialize program
        Keyword arguments:
          None
        Returns:
          None
    """
    try:
        data = JRC.get_config("databases")
    except Exception as err: # pylint: disable=broad-exception-caught)
        terminate_program(err)
    dbo = attrgetter("flyboy.prod.read")(data)
    LOGGER.info("Connecting to %s prod on %s as %s", dbo.name, dbo.host, dbo.user)
    try:
        DB['flyboy'] = JRC.connect_database(dbo)
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))


def search_by_stock():
    """ Search FlyBoy by stock name
        Keyword arguments:
          None
        Returns:
          Empty dictionary, rows from select
    """
    try:
        DB['flyboy']['cursor'].execute(READ['STOCK'])
        rows = DB['flyboy']['cursor'].fetchall()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    LOGGER.info(f"Found {len(rows):,} rows")
    return {}, rows


def search_by_robotid():
    """ Search FlyBoy by robot ID
        Keyword arguments:
          None
        Returns:
          Dictionary of robot IDs, rows from select
    """
    frobotids = {}
    LOGGER.info(f"Reading Robot IDs from {ARG.FILE}")
    with open(ARG.FILE, 'r', encoding='ascii') as infile:
        robotids = dict((rid.rstrip(), True) for rid in infile)
    LOGGER.info(f"Read {len(robotids):,} robot IDs from {ARG.FILE}")
    for rid in robotids:
        try:
            DB['flyboy']['cursor'].execute(READ['RID'], (rid,))
            cnt = DB['flyboy']['cursor'].fetchone()
            if cnt['c']:
                frobotids[str(rid)] = True
        except MySQLdb.Error as err:
            terminate_program(JRC.sql_error(err))
    LOGGER.info(f"Filtered to {len(frobotids):,} Robot IDs")
    LOGGER.info("Fetching orders")
    try:
        DB['flyboy']['cursor'].execute(READ['ROBOT'])
        rows = DB['flyboy']['cursor'].fetchall()
    except MySQLdb.Error as err:
        terminate_program(JRC.sql_error(err))
    LOGGER.info(f"Found {len(rows):,} rows")
    return frobotids, rows


def produce_report():
    """ Produce order report
        Keyword arguments:
          None
        Returns:
          None
    """
    robotids, rows = search_by_robotid() if ARG.FILE else search_by_stock()
    stock = {}
    minyear = maxyear = datetime.datetime.now().year
    for row in rows:
        key = row['stock_name'] if row['stock_name'] else row['RobotID']
        if ARG.FILE and (str(row['RobotID']) not in robotids):
            continue
        if row['year'] < minyear:
            minyear = row['year']
        if key not in stock:
            stock[key] = {'project': row['project'], 'subcat': row['subcat'],
                          'genotype': row['genotype'], 'labid': row['labid'],
                          'robotid': row['RobotID']}
        stock[key][row['year']] = row['cnt']
    LOGGER.info(f"Found {len(stock):,} stocks")
    prow = []
    for stk, row in stock.items():
        payload = {'Stock': stk, 'RobotID': row['robotid'], 'Project': row['project'],
                   'SubCat': row['subcat'], 'Genotype': row['genotype'], 'Lab ID': row['labid']}
        total = 0
        for year in range(minyear, maxyear+1):
            if year in row:
                payload[year] = row[year]
                total += payload[year]
            else:
                payload[year] = 0
        payload['Total'] = total
        prow.append(payload)
    pdf = pd.DataFrame(prow)
    LOGGER.info(f"Will output {pdf.shape[0]:,} rows for years {minyear}-{maxyear}")
    filename = 'flystore_order_report.xlsx'
    pdf.to_excel(filename, index=False)
    print(f"Wrote report to {filename}")

# -----------------------------------------------------------------------------

if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description="FlyStore order report")
    PARSER.add_argument('--file', dest='FILE', action='store',
                        help='File of robot IDs to include')
    PARSER.add_argument('--verbose', dest='VERBOSE', action='store_true',
                        default=False, help='Flag, Chatty')
    PARSER.add_argument('--debug', dest='DEBUG', action='store_true',
                        default=False, help='Flag, Very chatty')
    ARG = PARSER.parse_args()
    LOGGER = JRC.setup_logging(ARG)
    initialize_program()
    produce_report()
    terminate_program()
