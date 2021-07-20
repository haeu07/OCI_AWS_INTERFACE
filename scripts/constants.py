import os
import sys
from sys import platform
import pathlib

PP_SCHEMA = 'preproc'
PP_DB_DEFAULT_INI_FILE = 'database.ini'
PP_DB_DEFAULT_INI_FILE_SECTION = 'dbotdsr2.rzsamgpp_int'

PP_ROOT = str(pathlib.Path(__file__).parent.absolute())
PP_ROOT = PP_ROOT.replace('\OCI_AWS_INTERFACE\scripts','')
PP_DEFAULT_TIMESTAMP_FMT = 'yyyy-MM-dd HH:mm:ss'
PP_DEFAULT_TIMESTAMP_FMT_PY = '%Y%m%d %H:%M:%S'
PP_DEFAULT_DATE_FMT = 'yyyyMMdd'

if platform == 'win32':
    PP_DB_DEFAULT_INI_FILE_SECTION = 'dbotdsr2.rzsamgpp_int'
    PP_ROOT = os.path.normpath( PP_ROOT )
    PP_SCRIPTS_SUB_PATH = 'OCI_AWS_INTERFACE\\scripts'
    PP_LOG_SUB_PATH = 'OCI_AWS_INTERFACE\\log'
    PP_DATA_SUB_PATH = 'OCI_AWS_INTERFACE\\local_data'

else:
    PP_DB_DEFAULT_INI_FILE_SECTION = 'dbotdsr2.rzsamgpp_int'
    # PP_ROOT = os.path.normpath( "/home/hadoop" )
    PP_ROOT = os.path.normpath( PP_ROOT )
    PP_SCRIPTS_SUB_PATH = 'OCI_AWS_INTERFACE/scripts'
    PP_LOG_SUB_PATH = 'OCI_AWS_INTERFACE/log'
    PP_DATA_SUB_PATH = 'OCI_AWS_INTERFACE/local_data'

PP_SCRIPTS  = os.path.join( PP_ROOT, PP_SCRIPTS_SUB_PATH )
PP_LOG      = os.path.join( PP_ROOT, PP_LOG_SUB_PATH )
PP_DATA     = os.path.join( PP_ROOT, PP_DATA_SUB_PATH )
