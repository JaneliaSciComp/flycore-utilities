''' fix_robot_rack_id.py
'''

import argparse
from operator import attrgetter
import sys
import MySQLdb
from jrc_common import jrc_common

# Database
DB = {}
READ = {"STOCKS": "SELECT stock_id,genotype,"
                  + "SUBSTRING_INDEX(rack_location, '.', 2) AS copy_tray_a,"
                  + "SUBSTRING_INDEX(rack_location_b, '.', 2) AS copy_tray_b, "
                  + "CASE WHEN cell >0 AND cell <=24 THEN 'A' "
                  + "WHEN cell >24 AND cell <=48 THEN 'B' "
                  + "WHEN cell >48 AND cell <=72 THEN 'C' "
                  + "WHEN cell >72 AND cell <=96 THEN 'D' ELSE NULL END AS relative_rack_a,"
                  + "CASE WHEN cell_b >0 AND cell_b <=24 THEN 'A' "
                  + "WHEN cell_b >24 AND cell_b <=48 THEN 'B' "
                  + "WHEN cell_b >48 AND cell_b <=72 THEN 'C' "
                  + "WHEN cell_b >72 AND cell_b <=96 THEN 'D' ELSE NULL END AS relative_rack_b,"
                  + "last_flipped,last_flipped_b,rack AS rack_id_a,rack_b AS rack_id_b "
                  + "FROM __flipper_flystocks_stock WHERE (IFNULL(rack_location,'')<>'' AND "
                  + "rack_location LIKE 'A.%%') OR (IFNULL(rack_location_b,'')<>'' AND "
                  + "rack_location_b like 'B.%%')",
        "COUNT": "SELECT SUBSTRING_INDEX(rack_location, '.', 2) AS copy_tray,"
                 + "CASE WHEN cell >0 AND cell <=24 THEN 'A' "
                 + "WHEN cell >24 AND cell <=48 THEN 'B' "
                 + "WHEN cell >48 AND cell <=72 THEN 'C' "
                 + "WHEN cell >72 AND cell <=96 THEN 'D' ELSE 'ERROR' END AS relative_rack,"
                 + "MAX(last_flipped) AS most_recent_last_flipped,rack AS rack_id,"
                 + "COUNT(rack) AS count_rack FROM __flipper_flystocks_stock "
                 + "WHERE IFNULL(rack_location,'')<>'' AND rack_location LIKE 'A.%%' "
                 + "AND last_flipped >= DATE_ADD(DATE(NOW()), INTERVAL -10 DAY) "
                 + "GROUP BY copy_tray,relative_rack,rack "
                 + "ORDER BY copy_tray,relative_rack,rack_id,count_rack",
        "COUNTB": "SELECT SUBSTRING_INDEX(rack_location_b, '.', 2) AS copy_tray,"
                  + "CASE WHEN cell_b >0 AND cell_b <=24 THEN 'A' "
                  + "WHEN cell_b >24 AND cell_b <=48 THEN 'B' "
                  + "WHEN cell_b >48 AND cell_b <=72 THEN 'C' "
                  + "WHEN cell_b >72 AND cell_b <=96 THEN 'D' ELSE 'ERROR' END AS relative_rack,"
                  + "MAX(last_flipped_b) AS most_recent_last_flipped,rack_b AS rack_id,"
                  + "COUNT(rack_b) AS count_rack FROM __flipper_flystocks_stock "
                  + "WHERE IFNULL(rack_location_b,'')<>'' AND rack_location_b LIKE 'B.%%' "
                  + "AND last_flipped_b >= DATE_ADD(DATE(NOW()), INTERVAL -10 DAY) "
                  + "GROUP BY copy_tray,relative_rack,rack_b "
                  + "ORDER BY copy_tray,relative_rack,rack_id,count_rack",
        "IGNORE": "SELECT SUBSTRING_INDEX(rack_location, '.', 2) AS copy_tray,"
                  + "MAX(last_flipped) AS most_recent_last_flipped,"
                  + "MIN(last_flipped) AS oldest_last_flipped,"
                  + "DATE_ADD(DATE(NOW()),INTERVAL -6 MONTH),DATE(NOW()),"
                  + "CASE WHEN (MAX(last_flipped) < DATE_ADD(DATE(NOW()),INTERVAL -6 MONTH)) THEN "
                  + "'No flips to tray in last 6 months' "
                  + "WHEN (MAX(last_flipped)=DATE(NOW())) THEN 'Tray was flipped today' "
                  + "ELSE 'CASE ERROR in IGNORE!' END AS ignore_reason "
                  + "FROM __flipper_flystocks_stock "
                  +  "WHERE IFNULL(rack_location,'')<>'' AND rack_location LIKE 'A.%%' AND "
                  + "IFNULL(last_flipped,'')<>'' GROUP BY copy_tray HAVING "
                  + "(MAX(last_flipped) < DATE_ADD(DATE(NOW()),INTERVAL -6 MONTH)) OR "
                  + "(MAX(last_flipped)=DATE(NOW())) "
                  + "UNION SELECT SUBSTRING_INDEX(rack_location_b, '.', 2) as copy_tray,"
                  + "MAX(last_flipped_b) AS most_recent_last_flipped,"
                  + "MIN(last_flipped_b) AS oldest_last_flipped,"
                  + "DATE_ADD(DATE(NOW()),INTERVAL -6 MONTH),DATE(NOW()),"
                  + "CASE WHEN (MAX(last_flipped_b) < DATE_ADD(DATE(NOW()),INTERVAL -6 MONTH)) "
                  + "THEN 'No flips to tray in last 6 months' "
                  + "WHEN (MAX(last_flipped_b)=DATE(NOW())) THEN 'Tray was flipped today' "
                  + "ELSE 'CASE ERROR in IGNORE!' END AS ignore_reason "
                  + "FROM __flipper_flystocks_stock "
                  + "WHERE IFNULL(rack_location_b,'')<>'' AND rack_location_b LIKE 'B.%%' AND "
                  + "IFNULL(last_flipped_b,'')<>'' GROUP BY copy_tray HAVING "
                  + "(MAX(last_flipped_b) < DATE_ADD(DATE(NOW()),INTERVAL -6 MONTH)) OR "
                  + "(MAX(last_flipped_b)=DATE(NOW()))",
       }
# Counter
COUNT = {"updates": 0}


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
    try:
        dbconfig = jrc_common.get_config("databases")
    except Exception as err:
        terminate_program(err)
    dbo = attrgetter(f"flyboy.{ARG.MANIFOLD}.write")(dbconfig)
    LOGGER.info("Connecting to %s %s on %s as %s", dbo.name, ARG.MANIFOLD, dbo.host, dbo.user)
    try:
        DB["flyboy"] = jrc_common.connect_database(dbo)
    except MySQLdb.Error as err:
        terminate_program(jrc_common.sql_error(err))


def group_list_items_by_common_attribute(alist, getterfunc):
    """ Return a dict with key as key, holding a list of all of the elements
        Keyword arguments:
          alist: input dictionary
          getterfunc: lambda function for input dict values
        Returns:
          Grouped dictionary
    Return a dict with key as key, holding a list of all of the elements
    example:
    list1=[{'a':1},{'a':2},{'a':3},{'a':1}]
    def gfunc1(val):
        return str(val.get('a',''))
    print group_list_items_by_common_key(list1,gfunc1)
    >> {'1': [{'a': 1}, {'a': 1}], '3': [{'a': 3}], '2': [{'a': 2}]}
    """
    retdict={}
    for item in alist:
        currentkey=getterfunc(item)
        #if not retdict.has_key(currentkey):
        if currentkey not in retdict:
            retdict[currentkey]=[item]
        else:
            retdict[currentkey].append(item)
    assert len(alist)==sum([len(subitems) for subitems in retdict.values()])
    return retdict


def get_tray_rack_counts():
    """ Get counts for A and B racks
        Keyword arguments:
          None
        Returns:
          Dictionary
    """
    try:
        DB["flyboy"]["cursor"].execute(READ['COUNT'])
        rows = DB["flyboy"]["cursor"].fetchall()
    except MySQLdb.Error as err:
        terminate_program(jrc_common.sql_error(err))
    LOGGER.info("Rack A count: %d", len(rows))
    d_a_copy = group_list_items_by_common_attribute(rows, lambda row: (row['copy_tray'],
                                                                       row['relative_rack']))
    try:
        DB["flyboy"]["cursor"].execute(READ['COUNTB'])
        rows = DB["flyboy"]["cursor"].fetchall()
    except MySQLdb.Error as err:
        terminate_program(jrc_common.sql_error(err))
    LOGGER.info("Rack B count: %d", len(rows))
    d_b_copy = group_list_items_by_common_attribute(rows, lambda row: (row['copy_tray'],
                                                                       row['relative_rack']))
    d_a_copy.update(d_b_copy)
    # Sample key/value:
    # ('B.RB031', 'C'): [{'copy_tray': 'B.RB031', 'relative_rack': 'C',
    #                     'most_recent_last_flipped': datetime.date(2023, 5, 8),
    #                     'rack_id': '1176', 'count_rack': 8}]
    return d_a_copy


def get_stock_records():
    """ Get stocks
        Keyword arguments:
          None
        Returns:
          Dictionary
    """
    try:
        DB["flyboy"]["cursor"].execute(READ['STOCKS'])
        rows = DB["flyboy"]["cursor"].fetchall()
    except MySQLdb.Error as err:
        terminate_program(jrc_common.sql_error(err))
    LOGGER.info("Stocks: %d", len(rows))
    # Sample key/value:
    # {'stock_id': 9999992, 'genotype': 'TEST#3', 'copy_tray_a': 'A.GR59',
    #  'copy_tray_b': None, 'relative_rack_a': 'D', 'relative_rack_b': None,
    #  'last_flipped': datetime.date(2013, 1, 29), 'last_flipped_b': None,
    #  'rack_id_a': '0975', 'rack_id_b': None}
    return rows


def get_trays_to_ignore():
    """ Get trays that can be ignored (based on 6 months of inactivity)
        Keyword arguments:
          None
        Returns:
          Dictionary
    """
    try:
        DB["flyboy"]["cursor"].execute(READ['IGNORE'])
        rows = DB["flyboy"]["cursor"].fetchall()
    except MySQLdb.Error as err:
        terminate_program(jrc_common.sql_error(err))
    LOGGER.info("Trays to ignore: %d", len(rows))
    # Sample key/value:
    # 'B.UH1': 'No flips to tray in last 6 months'
    return dict([(row['copy_tray'], row['ignore_reason']) for row in rows])


def format_rack_info_record(rack_info):
    """ Format a rack record
        Keyword arguments:
          rack_info: dictionary
        Returns:
          Reformatted rack
    """
    return "\n".join([str(r) for r in rack_info])


def update_stock(stock, rack_info, copy):
    """ Update a single stock
        Keyword arguments:
          stock: stock record
          rack_info: rack record
          copy: "A" or "B"
        Returns:
          None
    """
    if copy == 'A':
        rack_id_var = 'rack_id_a'
        rack_id_sql = 'rack'
    elif copy == 'B':
        rack_id_var = 'rack_id_b'
        rack_id_sql = 'rack_b'
    else:
        raise ValueError(f"invalid copy {copy}")
     # Figure out if and how to fix the rack of this stock
    if rack_info:
        if len(rack_info) >= 1:
            # The rack to use is the one with the highest number of cells so sort
            # by the count and take the one at the end.
            rack_info.sort(key=lambda r: r['count_rack'])
            correct_rack_rec = rack_info[-1]
            # Make sure we have a majority if multiple
            if len(rack_info)==1 or \
               (correct_rack_rec['count_rack'] > rack_info[-2]['count_rack']):
                if stock[rack_id_var] != correct_rack_rec['rack_id']:
                    sql = f"UPDATE __flipper_flystocks_stock SET {rack_id_sql}=" \
                          + f"{correct_rack_rec['rack_id']} WHERE stock_id={stock['stock_id']}"
                    LOGGER.debug(sql)
                    DB["flyboy"]["cursor"].execute(sql)
                    COUNT["updates"] += 1
                    LOGGER.info(f"Changed stock id {stock['stock_id']}'s column {rack_id_sql} " \
                                + f"from {stock[rack_id_var]} to {correct_rack_rec['rack_id']}. " \
                                + f"Rack Choices were:{format_rack_info_record(rack_info)}")
            else:
                LOGGER.warning(f"Not updating stock {stock['stock_id']}/" \
                               + f"rack {stock[rack_id_var]} " \
                               + "because there is no clear majority in the " \
                               + f"quadrant:{format_rack_info_record(rack_info)}")
        else:
            raise ValueError(f"Invalid rack info for key {key}")
    else:
        # This normally means the tray wasn't flipped recently
        LOGGER.debug("Could not find rack info for copy %s stock: %s", copy, stock)


def fix_ids():
    """ Fix rack IDs
        Keyword arguments:
          None
        Returns:
          None
    """
    stocks = get_stock_records()
    d_rack_info = get_tray_rack_counts()
    d_ignore = get_trays_to_ignore()
    ignore_msg = "Ignoring stock %s because its tray %s should be ignored because: %s"
    for stock in stocks:
        if stock['copy_tray_a'] and stock['relative_rack_a']:
            if stock['copy_tray_a'] in d_ignore:
                LOGGER.debug(ignore_msg, stock['stock_id'], stock['copy_tray_a'],
                             d_ignore[stock['copy_tray_a']])
            else:
                key = (stock['copy_tray_a'], stock['relative_rack_a'])
                update_stock(stock, d_rack_info.get(key, None), 'A')
        if stock['copy_tray_b'] and stock['relative_rack_b']:
            if stock['copy_tray_b'] in d_ignore:
                LOGGER.debug(ignore_msg, stock['stock_id'], stock['copy_tray_b'],
                             d_ignore[stock['copy_tray_b']])
            else:
                key = (stock['copy_tray_b'], stock['relative_rack_b'])
                update_stock(stock, d_rack_info.get(key, None), 'B')
        if not ((stock['copy_tray_a'] and stock['relative_rack_a']) or \
                (stock['copy_tray_b'] and stock['relative_rack_b'])):
            LOGGER.debug("Ignoring stock %s because it is missing a tray or rack", stock)
    print("All stocks have been processed")
    print(f"Updated {COUNT['updates']} rows")
    if ARG.WRITE:
        DB["flyboy"]["conn"].commit()
        print("Committed DB Changes")

# -----------------------------------------------------------------------------


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Fix flyflipper racks')
    PARSER.add_argument('--manifold', dest='MANIFOLD', action='store',
                        default='prod', choices=['prod', 'dev'], help='Manifold [prod]')
    PARSER.add_argument('--write', action='store_true', dest='WRITE',
                        default=False, help='Send email')
    PARSER.add_argument('--verbose', action='store_true', dest='VERBOSE',
                        default=False, help='Turn on verbose output')
    PARSER.add_argument('--debug', action='store_true', dest='DEBUG',
                        default=False, help='Turn on debug output')
    ARG = PARSER.parse_args()
    LOGGER = jrc_common.setup_logging(ARG)
    initialize_program()
    fix_ids()
    terminate_program()
