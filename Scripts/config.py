'''This file retrieves the environment variables'''
from os import environ
from ast import literal_eval

DBVARS = None
TABLE_NAME = None
TABLE_COLS = None
CREATE_QUERY = None
INSERT_QUERY = None
FILE_LOC = None
FILE_NAME = None
FILE_ENTRIES = None
LOGGER_FILE_LOC = None
BATCH_SIZE = None


def retrieveEnvironmentVariables():
    global DBVARS, TABLE_NAME, TABLE_COLS, CREATE_QUERY, INSERT_QUERY, FILE_LOC, FILE_NAME, FILE_ENTRIES, LOGGER_FILE_LOC, BATCH_SIZE
    try:
        DBVARS = literal_eval(environ.get('DBVARS', r'{}'))
        TABLE_NAME = environ.get('TRADE_TABLE', '')
        TABLE_COLS = literal_eval(environ.get('TRADE_TABLE_COLS', 'None'))
        CREATE_QUERY = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (" + ", ".join([f"{column[0]} {column[1]}" for column in TABLE_COLS]) + ");"
        INSERT_QUERY = f"INSERT INTO {TABLE_NAME} VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        FILE_LOC = environ.get('FILE_LOC', './') + environ.get('FILE_NAME', '')
        FILE_NAME = environ.get('FILE_NAME', '')
        FILE_ENTRIES = int(environ.get('FILE_ENTRIES', '0'))
        LOGGER_FILE_LOC = environ.get('LOGGER_FILE_LOC', './') + environ.get('LOGGER_FILE_NAME', '')
        BATCH_SIZE = int(environ.get('BATCH_SIZE', '10000'))
        print('Env variables retrieved')
        return True
    except Exception as e:
        print(f'Error while retrieving env variables: {e}')
        return False


retrieveEnvironmentVariables()