import os, sys, traceback, re, builtins, calendar
from datetime import datetime, timedelta
import constants as C
import pp_db as db

from inspect import currentframe, getframeinfo
from configparser import ConfigParser
from datetime import datetime

#for email sending
import smtplib
import email.utils
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


# error and message logging
LOG_TYPE_ERROR = 'ERROR'
LOG_TYPE_MESSAGE = 'MESSAGE'
LOG_TYPE_DEBUG = 'DEBUG'

LOG_DEST_DAILY_FILE = 1
LOG_DEST_BY_RUN_ID = 2

LOG_CURRENT_LOG_LEVEL = LOG_TYPE_DEBUG     # very detailed debug info get logged
#LOG_CURRENT_LOG_LEVEL = LOG_TYPE_MESSAGE    # errors and messages get loged
LOG_PRINT = True                               # print log messages to std out





########################################################################################################################
# procedure name: logger
# parameters:   log_type            must be one of the constants: LOG_TYPE_ERROR, LOG_TYPE_MESSAGE, LOG_TYPE_DEBUG
#               log_text            the message to be loged
#               run_id              run_id (optional default = 0 )
#               traceback_info      traceback info as coming from sys.exc_info()[2] (optional default=None)
#               log_destination     must be one of the constats: LOG_DEST_DAILY_FILE, LOG_DEST_BY_RUN_ID (optional default=LOG_DEST_DAILY_FILE)
# description:  writes out log messages into a text file.
#               LOG_CURRENT_LOG_LEVEL defines globally what log level is used
#               Format is: date \t log_type \t lineno \t filename \t procedure \t log_text \t traceback
########################################################################################################################
def logger(log_type, log_text, run_id = None, traceback_info = None, p_log_destination = LOG_DEST_BY_RUN_ID, p_log_level = LOG_CURRENT_LOG_LEVEL, p_log_print = LOG_PRINT):

    # check if current log level allows this message to be loged
    if p_log_level == LOG_TYPE_ERROR and ( log_type == LOG_TYPE_MESSAGE or log_type == LOG_TYPE_DEBUG):
        return
    elif p_log_level == LOG_TYPE_MESSAGE and log_type == LOG_TYPE_DEBUG:
        return
    elif p_log_level == LOG_TYPE_DEBUG:
        None

    # define logfile type
    if run_id is None or p_log_destination == LOG_DEST_DAILY_FILE:
        log_destination = LOG_DEST_DAILY_FILE
    else:
        log_destination = p_log_destination

    cf_back = currentframe().f_back
    filename = os.path.basename( getframeinfo(cf_back).filename )
    lineno = cf_back.f_lineno
    procedure = cf_back.f_code.co_name

    """ this put the timestamp and so on in front of every line (good is log will be used for SQL like analysis, 
        bad for copy and paste eg SQLs but nice for column based filtering
    """
    static_text_part = str(datetime.now()) \
                       + '\t' + str(run_id) \
                       + '\t' + log_type \
                       + '\t' + str(lineno) \
                       + '\t' + filename \
                       + '\t' + procedure \
                       + '\t'
    text_offset = len( static_text_part )
    out_text = static_text_part + re.sub(r'\n *','\n'+static_text_part, log_text )  # allign multiline text

    """ this does not pu trimestamp and so on in front of every line
    out_text = str(datetime.now()) \
               + '\t' + str(run_id) \
               + '\t' + log_type \
               + '\t' + str(lineno) \
               + '\t' + filename \
               + '\t' + procedure \
               + '\t' +  re.sub(r'\n *','\n\t\t\t\t\t\t', log_text )  # allign multiline text
    """

    #+ '\t' +  re.sub(r'\n *','\n' + ' '*text_offset, log_text )  # allign multiline text


    if traceback_info:
        out_text = out_text + '\n' + traceback.format_exc(limit = 1000)

    if log_destination == LOG_DEST_DAILY_FILE:
        log_filename = datetime.now().strftime("dailylog_%Y%m%d.txt")
    elif log_destination == LOG_DEST_BY_RUN_ID:
        log_filename = 'runid_' + str(run_id) + '.txt'
    else:
        raise('Invalid parameter: log_destination' + str(LOG_DEST_DAILY_FILE) )

    log_path = os.path.normpath( C.PP_LOG )
    with open( os.path.join( log_path, log_filename ), "a" ) as logfile:
        logfile.write( out_text.rstrip() + '\n' )

    if p_log_print:
        print( out_text.rstrip() )


def read_ini_file(filename, section):
    # check if ini file exists
    filename = os.path.join( C.PP_SCRIPTS, filename)
    if not os.path.isfile( filename ):
        raise Exception( 'File: {0} does not exist'.format(filename) )

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


def findnth(haystack, needle, n):
    parts= haystack.split(needle, n+1)
    if len(parts)<=n+1:
        return -1
    return len(haystack)-len(parts[-1])-len(needle)


def days_between_dates(d1, d2):
    d1 = datetime.strptime(d1, "%Y%m%d")
    d2 = datetime.strptime(d2, "%Y%m%d")
    return builtins.abs((d2 - d1).days)




########################################################################################################################
# procedure name: send_email
# parameters:     p_to, p_subject, p_body, p_files = None, p_cc=None, p_bcc=None, p_run_id=None, p_html = False, p_con=None
# return:         -
# description:    sends email to p_to, p_cc, p_bcc
#                 attach files listed in p_files
#                 p_to, p_cc, p_bcc, p_files can either be a string, a comma separated list of strings or a list in form ['some@email.com','someother@email.com']
#                 p_to can not be empty!
########################################################################################################################

def send_email( p_to, p_subject, p_body, p_files = None, p_cc=None, p_bcc=None, p_run_id=None, p_html = False, p_con=None ):
    con = db.connect_if_none( p_con )
    cur = db.open_cursor( con )

    logger( log_type=LOG_TYPE_MESSAGE, run_id = p_run_id, log_text=f"p_to:{p_to} p_cc:{p_cc} p_bcc:{p_bcc} p_subject:{p_subject}  p_files:{p_files} p_html:{p_html} " )

    SENDER      = 'ralf.haeussler@sonymusic.com'
    SENDERNAME  = 'ralf.haeussler@sonymusic.com'
    HOST        = "email-smtp.us-east-1.amazonaws.com"
    PORT        = 587

    # get the user and pass for the SMTP sending
    run_sql = f"""select l.aws_access_key_id, l.aws_secret_access_key
                  from pp_data_locations l
                  where l.loc_key = 'EMAIL_SENDING'"""
    db.execute(cur, run_sql )
    a_row = db.fetchone(cur)
    USERNAME_SMTP = a_row['aws_access_key_id']
    PASSWORD_SMTP = a_row['aws_secret_access_key']

    # convert string to list if needed
    if isinstance(p_to,str): p_to = p_to.split(',')
    elif p_to is None:       p_to = []

    if isinstance(p_cc,str):  p_cc  = p_cc.split(',')
    elif p_cc is None:        p_cc = []

    if isinstance(p_bcc,str):  p_bcc = p_bcc.split(',')
    elif p_bcc is None:        p_bcc = []

    if isinstance(p_files,str):  p_files = p_files.split(',')
    elif p_files is None:        p_files = []

    all_reciepients = p_to + p_cc + p_bcc

    msg = MIMEMultipart('alternative')
    msg['From'] = email.utils.formataddr((SENDERNAME, SENDER))
    msg['To'] = ",".join(p_to)
    msg['Cc'] = ",".join(p_cc)
    #msg['Bcc'] = ",".join(p_bcc)     # dont show bcc in email header !
    msg['Subject'] = p_subject
    #msg.add_header('X-SES-CONFIGURATION-SET',CONFIGURATION_SET)

    if p_html == True:
        msg.attach( MIMEText(p_body, 'html') )
    else:
        msg.attach( MIMEText(p_body, 'plain') )

    # add attachment files
    for f in p_files or []:
        with open(f, "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=os.path.basename(f)
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % os.path.basename(f)
        msg.attach(part)

    try:
        server = smtplib.SMTP(HOST, PORT)
        server.ehlo()
        server.starttls()
        #stmplib docs recommend calling ehlo() before & after starttls()
        server.ehlo()
        server.login(USERNAME_SMTP, PASSWORD_SMTP)
        server.sendmail(SENDER, all_reciepients, msg.as_string())
        server.close()
    except Exception as e:
        #print ("Error: ", e)
        logger( log_type=LOG_TYPE_ERROR, run_id = p_run_id, log_text=f"error sending email", traceback_info=sys.exc_info()[2] )
    #else:
    #    print ("Email sent!")

    logger( log_type=LOG_TYPE_MESSAGE, run_id = p_run_id, log_text=f"email sent" )
    cur.close()
    db.close_if_none(p_con, con)
