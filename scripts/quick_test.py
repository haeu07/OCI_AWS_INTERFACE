import os
import cx_Oracle
import constants as C
from configparser import ConfigParser

def read_ini_file(filename, section):
    # check if ini file exists
    print( C.PP_SCRIPTS + '\n' )
    filename = os.path.join( C.PP_SCRIPTS, filename)
    print( filename + '\n' )
    if not os.path.isfile( filename ):
        raise Exception( f'File: {filename} does not exist'.format(filename) )

    # read ini file
    parser = ConfigParser()
    parser.read( filename )

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section: {0} not found in the {1} file'.format(section, filename))

    return db


def quick_test_oracle():
    params = read_ini_file(filename='database.ini', section='dbotdsr2.rzsamgpp_int')
    or_connect_str = params['host']
    con = cx_Oracle.connect( params['user'], params['password'], or_connect_str )
    cur = con.cursor()

    run_sql = f"""select pt.product_type_key, pt.product_type_name
                  from   dig_product_type pt
                  where  pt.product_type_key like '1%' """
    cur.execute(run_sql)
    all_rows = cur.fetchall()

    for one_row in all_rows:
        print( one_row[0] + " " + one_row[1] )

    return


quick_test_oracle()
