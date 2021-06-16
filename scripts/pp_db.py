import sys, re
import traceback
import psycopg2  # the posgresql access
from psycopg2.extras import RealDictCursor
import tools as t
import constants as c
import cx_Oracle

# some constants
ARRAY_TAYPE_BY_NAME = 'BY_NAME'    # only works with postgres not with oracle
ARRAY_TAYPE_BY_NUMBER = 'BY_NUMBER'
DB_IDENTIFY_REGEX = 'DBO.DSR.*'

def connect( ini_file_name=c.PP_DB_DEFAULT_INI_FILE, ini_file_section=c.PP_DB_DEFAULT_INI_FILE_SECTION, run_id=None ):
    t.logger( log_type=t.LOG_TYPE_MESSAGE, run_id = run_id, log_text=f"""parameters ini_file_name: {ini_file_name}  ini_file_section:{ini_file_section}""", traceback_info=sys.exc_info()[2] )
    try:
        params = t.read_ini_file(filename=ini_file_name, section=ini_file_section)
    except:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = run_id, log_text=f"""Error reading ini file: {ini_file_name}""", traceback_info=sys.exc_info()[2] )
        raise

    try:
        if params['database'] == 'starsdb': # this is the postgress DB!
            con = psycopg2.connect(**params)
        elif re.match( DB_IDENTIFY_REGEX, params['database'] ) is not None:   # this is ab oracke DB
            or_connect_str = params['host']
            #print( or_connect_str )
            con = cx_Oracle.connect( params['user'], params['password'], or_connect_str )
        else:
            t.logger( log_type=t.LOG_TYPE_ERROR, run_id = run_id, log_text=f"wrong db_type: {params['db_type']} in ini file: {ini_file_name} Error connecting to DB", traceback_info=sys.exc_info()[2] )
            raise
    except:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = run_id, log_text='Error connecting to DB', traceback_info=sys.exc_info()[2] )
        raise

    return con


########################################################################################################################
# procedure:    connect_if_none
# parameters:   p_con           optional connection to a DB
#               ini_file_name
#               ini_file_section
# description:  when no connection given as p_con then a new connection is opened and returned
#               when an existing connection is given as p_con then this connection is returned an no new one is opened
########################################################################################################################

def connect_if_none( p_con=None, ini_file_name=c.PP_DB_DEFAULT_INI_FILE, ini_file_section=c.PP_DB_DEFAULT_INI_FILE_SECTION, run_id=None ):
    if p_con is None:
        t.logger( log_type=t.LOG_TYPE_DEBUG, run_id = run_id, log_text='open new connection',   traceback_info=sys.exc_info()[2], p_log_print = False )
        con = connect( ini_file_name, ini_file_section )
    else:
        # t.logger( log_type=t.LOG_TYPE_DEBUG, run_id = run_id, log_text='use existing connection',   traceback_info=sys.exc_info()[2], p_log_print = False )
        con = p_con
    return con



def open_cursor( connection, array_type=ARRAY_TAYPE_BY_NAME, run_id=None ):
    try:
        if array_type == ARRAY_TAYPE_BY_NAME and str(connection).find('starsdb') != -1:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
        else:
            cursor = connection.cursor()
    except:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = run_id, log_text='Error opening cursor',   traceback_info=sys.exc_info()[2] )
        raise
    return cursor


def execute( cursor, statement, run_id=None ):
    try:
        t.logger( log_type=t.LOG_TYPE_DEBUG, run_id = run_id, log_text='Executing: ' + statement,   traceback_info=sys.exc_info()[2], p_log_print = False )
        cursor.execute(statement)
    except:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = run_id, log_text='Error executing: ' + statement,   traceback_info=sys.exc_info()[2] )
        raise


def fetchone( cursor, run_id=None ):
    try:
        a_fetch = cursor.fetchone()
    except:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = run_id, log_text='Error getching', traceback_info=sys.exc_info()[2] )
        raise
    return a_fetch


def fetchall( cursor, run_id=None ):
    try:
        a_fetch = cursor.fetchall()
    except:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = run_id, log_text='Error getching', traceback_info=sys.exc_info()[2] )
        raise
    return a_fetch


# ---- NOT YET CHECKED WITH ORACLE

def rewind( cursor, position=0, mode='absolute', run_id=None ):
    try:
        cursor.scroll( position, mode='absolute')
    except:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = run_id, log_text='Error scrolling cursomr to: prsition={} mode={}'.format(position,mode), traceback_info=sys.exc_info()[2] )
        raise


def return_single_value( cursor, returned_field, run_id=None ):
    try:
        return cursor.fetchone()[returned_field]
    except:
        t.logger( log_type=t.LOG_TYPE_ERROR, run_id = run_id, log_text='Error in return_single_value returned_field={}'.format(returned_field), traceback_info=sys.exc_info()[2] )
        raise

# ---- NOT YET CHECKED WITH ORACLE END


def close_cursor( cursor ):
    cursor.close()

def close( p_con ):
    p_con.close()


########################################################################################################################
# procedure:    close_if_not_none
# parameters:   p_con           optional connection to a DB
# description:  when a connection is given as p_con then the connection is closed
#               else not closed
########################################################################################################################

def close_if_none( p_con_check, p_con ):
    if p_con_check is None:
        t.logger( log_type=t.LOG_TYPE_DEBUG, log_text='close connection',   traceback_info=sys.exc_info()[2], p_log_print = False )
        p_con.close()
    #else:
    #   t.logger( log_type=t.LOG_TYPE_DEBUG, log_text='dont close connection',   traceback_info=sys.exc_info()[2], p_log_print = False )









