'''This file contains the DBConnection class'''
import psycopg2
from logger_class import Logger
from config import DBVARS, TRADE_TABLE, CREATE_QUERY, INSERT_QUERY


class DBConnection:
    '''Class containing methods to perform CRUD operations'''
    # class members
    conn = None
    cur = None
    _instance = None
    
    def __new__(cls):
        '''This method overrides new to make it a singleton'''
        if not cls._instance:
            cls._instance = super(DBConnection, cls).__new__(cls)
        return cls._instance
    
    
    @classmethod
    def dbConnect(cls):
        '''This method establishes a connection to DB and create table if they don't exist'''
        try:
            if not cls.conn:
                cls.conn = psycopg2.connect(
                    database=DBVARS['database'],
                    user=DBVARS['user'],
                    password=DBVARS['password'],
                    host=DBVARS['host'],
                    port=DBVARS['port']
                )
                cls.conn.autocommit = False
                cls.cur = cls.conn.cursor()
                Logger.logEvent('Info', f'Established new DB connection - {cls.conn}')
            Logger.logEvent('Info', f'DB connection is already established')
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Could not initialize DB connection - {pe}')
    
    
    @classmethod
    def getConnection(cls):
        '''This method returns DB connection object'''
        try:
            DBConnection.dbConnect()
            Logger.logEvent('Info', f'Getting DB connection - {cls.conn}')
            return cls.conn
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Could not get DB connection - {pe}')
            return None
    
    
    @classmethod
    def createTable(cls):
        '''This method creates the required table if it doesn't exist'''
        try:
            DBConnection.dbConnect()
            cls.cur.execute(CREATE_QUERY)
            if cls.commitChanges():
                Logger.logEvent('Info', f'Created table - {TRADE_TABLE}')
                return
            Logger.logEvent('Error', f'Error while commit of new table')
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Could not create table {TRADE_TABLE}: {pe}')
            cls.conn.rollback()
    
    
    @classmethod
    def insertRows(cls, entries):
        '''This method inserts rows into table'''
        try:
            DBConnection.dbConnect()
            cls.cur.executemany(INSERT_QUERY, entries)
            if cls.commitChanges():
                Logger.logEvent('Info', f'Inserted {len(entries)} rows into table')
                return True
            Logger.logEvent('Error', f'Error while inserting {len(entries)} entriees')
        except psycopg2.DatabaseError as dbe:
            Logger.logEvent('Error', f'Database error while inserting {len(entries)} entries: {dbe}')
            cls.conn.rollback()
            return False
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while inserting {len(entries)} entries: {pe}')
            cls.conn.rollback()
            return False
    
    
    @classmethod
    def truncateTable(cls):
        '''This method drops table if it exists'''
        try:
            DBConnection.dbConnect()
            cls.cur.execute(f"TRUNCATE TABLE {TRADE_TABLE};")
            if cls.commitChanges():
                Logger.logEvent('Info', f'Successfully truncated table {TRADE_TABLE}')
                return True
            Logger.logEvent('Error', f'Error while truncating table {TRADE_TABLE}')
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while truncating table {TRADE_TABLE}: {pe}')
            cls.conn.rollback()
            return False
    
    
    @classmethod
    def dropTable(cls):
        '''This method drops table if it exists'''
        try:
            DBConnection.dbConnect()
            cls.cur.execute(f"DROP TABLE IF EXISTS {TRADE_TABLE};")
            if cls.commitChanges():
                Logger.logEvent('Info', f'Dropped table {TRADE_TABLE}')
                return True
            Logger.logEvent('Error', f'Error while dropping table {TRADE_TABLE}')
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while dropping table {TRADE_TABLE}: {pe}')
            cls.conn.rollback()
            return False
    
    
    @classmethod
    def getCount(cls):
        '''This method drops table if it exists'''
        try:
            DBConnection.dbConnect()
            cls.cur.execute(f"SELECT COUNT(*) FROM {TRADE_TABLE};")
            rows = cls.cur.fetchall()
            if rows:
                Logger.logEvent('Info', f'Retrieved number of rows of table {TRADE_TABLE}')
                return rows[0][0]
            Logger.logEvent('Error', f'Error while counting rows of table {TRADE_TABLE}: {pe}')
            return -1
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while counting rows of table {TRADE_TABLE}: {pe}')
            return -1
    
    
    @classmethod
    def commitChanges(cls):
        '''This method commits the changes to DB'''
        try:
            DBConnection.dbConnect()
            cls.conn.commit()
            Logger.logEvent('Info', 'Commit Successful')
            return True
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while committing changes: {pe}')
            return False
    
    
    @classmethod
    def rollback(cls):
        '''This method rollbacks changes to DB'''
        try:
            DBConnection.dbConnect()
            cls.conn.rollback()
            Logger.logEvent('Info', 'Rollback Successful')
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while rollback: {pe}')
    
    
    @classmethod
    def closeDbConnection(cls):
        '''This method disconnects DB if it is connected'''
        try:
            if cls.conn:
                cls.cur.close()
                cls.conn.close()
                cls.cur = None
                cls.conn = None
                Logger.logEvent('Info', f'Closed connection to DB')
                return
            Logger.logEvent('Info', f'No open connections to close')
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while closing DB Connection: {pe}')