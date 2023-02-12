'''This file retrieves the environment variables'''
from os import environ
from ast import literal_eval

DBVARS = None
TRADE_TABLE = None
TRADE_TABLE_COLS = None
CREATE_QUERY = None
INSERT_QUERY = None
FILE_LOC = None
FILE_ENTRIES = None
LOGGER_FILE_LOC = None
BATCH_SIZE = None


def retrieveEnvironmentVariables():
    global DBVARS, TRADE_TABLE, TRADE_TABLE_COLS, CREATE_QUERY, INSERT_QUERY, FILE_LOC, FILE_ENTRIES, LOGGER_FILE_LOC, BATCH_SIZE
    try:
        DBVARS = literal_eval(environ.get('DBVARS', r'{}'))
        TRADE_TABLE = environ.get('TRADE_TABLE', '')
        TRADE_TABLE_COLS = literal_eval(environ.get('TRADE_TABLE_COLS', 'None'))
        CREATE_QUERY = f"CREATE TABLE IF NOT EXISTS {TRADE_TABLE} (" + ", ".join([f"{column[0]} {column[1]}" for column in TRADE_TABLE_COLS]) + ");"
        INSERT_QUERY = f"INSERT INTO {TRADE_TABLE} VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
        FILE_LOC = environ.get('FILE_LOC', './') + environ.get('FILE_NAME', '')
        FILE_ENTRIES = int(environ.get('FILE_ENTRIES', '0'))
        LOGGER_FILE_LOC = environ.get('LOGGER_FILE_LOC', './') + environ.get('LOGGER_FILE_NAME', '')
        BATCH_SIZE = int(environ.get('BATCH_SIZE', '10000'))
        print('Env variables retrieved')
        return True
    except Exception as e:
        print(f'Error while retrieving env variables: {e}')
        return False


retrieveEnvironmentVariables()