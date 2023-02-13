'''This file contains the DBConnection class'''
import psycopg2
import psycopg2.extras
from logger_class import Logger
from config import DBVARS


class DBConnection:
    '''Class containing methods to perform CRUD operations'''
    # class members
    conn = None
    cur = None
    _instance = None
    retries = None
    insert_query = ''
    
    
    def __new__(cls):
        '''This method overrides new to make it a singleton'''
        if not cls._instance:
            cls._instance = super(DBConnection, cls).__new__(cls)
        return cls._instance
    
    
    @classmethod
    def setInsertQuery(cls, insert_query):
        '''This method sets the insert query'''
        cls.insert_query = insert_query
    
    
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
    def createTable(cls, table_name, create_query):
        '''This method creates the required table if it doesn't exist'''
        try:
            DBConnection.dbConnect()
            cls.cur.execute(create_query)
            cls.commitChanges()
            Logger.logEvent('Info', f'Created table - {table_name}')
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Could not create table {table_name}: {pe}')
            cls.conn.rollback()
    
    
    @classmethod
    def insertRows(cls, entries):
        '''This method inserts rows into table'''
        try:
            DBConnection.dbConnect()
            if not cls.retries:
                cls.retries = 0
            if cls.retries > 3:
                return False
            psycopg2.extras.execute_batch(cls.cur, cls.insert_query, entries)
            cls.commitChanges()
            Logger.logEvent('Info', f'Inserted {len(entries)} rows into table')
            cls.retries = None
            return True
        except psycopg2.DatabaseError as dbe:
            Logger.logEvent('Error', f'Database error while inserting {len(entries)} entries: {dbe}')
            cls.conn.rollback()
            cls.retries += 1
            cls.insertRows(entries)
            return False
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while inserting {len(entries)} entries: {pe}')
            cls.conn.rollback()
            return False
    
    
    @classmethod
    def truncateTable(cls, table_name):
        '''This method drops table if it exists'''
        try:
            DBConnection.dbConnect()
            cls.cur.execute(f"TRUNCATE TABLE {table_name};")
            if cls.commitChanges():
                Logger.logEvent('Info', f'Successfully truncated table {table_name}')
                return True
            Logger.logEvent('Error', f'Error while truncating table {table_name}')
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while truncating table {table_name}: {pe}')
            cls.conn.rollback()
            return False
    
    
    @classmethod
    def dropTable(cls, table_name):
        '''This method drops table if it exists'''
        try:
            DBConnection.dbConnect()
            cls.cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            if cls.commitChanges():
                Logger.logEvent('Info', f'Dropped table {table_name}')
                return True
            Logger.logEvent('Error', f'Error while dropping table {table_name}')
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while dropping table {table_name}: {pe}')
            cls.conn.rollback()
            return False
    
    
    @classmethod
    def getCount(cls, table_name):
        '''This method drops table if it exists'''
        try:
            DBConnection.dbConnect()
            cls.cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            rows = cls.cur.fetchall()
            if rows:
                Logger.logEvent('Info', f'Retrieved number of rows of table {table_name}')
                return rows[0][0]
            Logger.logEvent('Error', f'Error while counting rows of table {table_name}: {pe}')
            return -1
        except psycopg2.Error as pe:
            Logger.logEvent('Error', f'Error while counting rows of table {table_name}: {pe}')
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