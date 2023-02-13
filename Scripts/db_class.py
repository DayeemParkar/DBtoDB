'''This file contains the DBConnection class'''
import psycopg2
from logger_class import Logger
from config import DBVARS


class DBConnection:
    '''Class containing methods to perform operations on database and tables'''
    # class members
    conn = None
    cur = None
    _instance = None
    retries = None
    insert_query = ''
    logger = Logger()
    
    
    def __new__(cls):
        '''This method overrides new to make it a singleton'''
        if not cls._instance:
            cls._instance = super(DBConnection, cls).__new__(cls)
        return cls._instance
    
    
    def setInsertQuery(self, insert_query):
        '''This method sets the insert query'''
        self.insert_query = insert_query
    
    
    def dbConnect(self):
        '''This method establishes a connection to DB and create table if they don't exist'''
        try:
            if not self.conn:
                self.conn = psycopg2.connect(
                    database=DBVARS['database'],
                    user=DBVARS['user'],
                    password=DBVARS['password'],
                    host=DBVARS['host'],
                    port=DBVARS['port']
                )
                self.conn.autocommit = False
                self.cur = self.conn.cursor()
                self.logger.logEvent('Info', f'Established new DB connection - {self.conn}')
            self.logger.logEvent('Info', f'DB connection is already established')
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Could not initialize DB connection - {pe}')
    
    
    def getConnection(self):
        '''This method returns DB connection object'''
        try:
            self.dbConnect()
            self.logger.logEvent('Info', f'Getting DB connection - {self.conn}')
            return self.conn
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Could not get DB connection - {pe}')
            return None
    
    
    def createTable(self, table_name, create_query):
        '''This method creates the required table if it doesn't exist'''
        try:
            self.dbConnect()
            self.cur.execute(create_query)
            self.commitChanges()
            self.logger.logEvent('Info', f'Created table - {table_name}')
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Could not create table {table_name}: {pe}')
            self.conn.rollback()
    
    
    def insertRows(self, entries):
        '''This method inserts rows into table'''
        try:
            self.dbConnect()
            if not self.retries:
                self.retries = 0
            if self.retries > 3:
                return False
            values = ','.join(self.cur.mogrify(self.insert_query[1], i).decode('utf-8')
                for i in entries)
            self.cur.execute(self.insert_query[0] + values + ';')
            self.commitChanges()
            self.logger.logEvent('Info', f'Inserted {len(entries)} rows into table')
            self.retries = None
            return True
        except psycopg2.DatabaseError as dbe:
            self.logger.logEvent('Error', f'Database error while inserting {len(entries)} entries: {dbe}')
            self.conn.rollback()
            self.retries += 1
            self.insertRows(entries)
            return False
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while inserting {len(entries)} entries: {pe}')
            self.conn.rollback()
            return False
    
    
    def truncateTable(self, table_name):
        '''This method drops table if it exists'''
        try:
            self.dbConnect()
            self.cur.execute(f"TRUNCATE TABLE {table_name};")
            if self.commitChanges():
                self.logger.logEvent('Info', f'Successfully truncated table {table_name}')
                return True
            self.logger.logEvent('Error', f'Error while truncating table {table_name}')
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while truncating table {table_name}: {pe}')
            self.conn.rollback()
            return False
    
    
    def dropTable(self, table_name):
        '''This method drops table if it exists'''
        try:
            self.dbConnect()
            self.cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            if self.commitChanges():
                self.logger.logEvent('Info', f'Dropped table {table_name}')
                return True
            self.logger.logEvent('Error', f'Error while dropping table {table_name}')
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while dropping table {table_name}: {pe}')
            self.conn.rollback()
            return False
    
    
    def getCount(self, table_name):
        '''This method drops table if it exists'''
        try:
            self.dbConnect()
            self.cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            rows = self.cur.fetchall()
            if rows:
                self.logger.logEvent('Info', f'Retrieved number of rows of table {table_name}')
                return rows[0][0]
            self.logger.logEvent('Error', f'Error while counting rows of table {table_name}: {pe}')
            return -1
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while counting rows of table {table_name}: {pe}')
            return -1
    
    
    def commitChanges(self):
        '''This method commits the changes to DB'''
        try:
            self.dbConnect()
            self.conn.commit()
            self.logger.logEvent('Info', 'Commit Successful')
            return True
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while committing changes: {pe}')
            return False
    
    
    def rollback(self):
        '''This method rollbacks changes to DB'''
        try:
            self.dbConnect()
            self.conn.rollback()
            self.logger.logEvent('Info', 'Rollback Successful')
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while rollback: {pe}')
    
    
    def closeDbConnection(self):
        '''This method disconnects DB if it is connected'''
        try:
            if self.conn:
                self.cur.close()
                self.conn.close()
                self.cur = None
                self.conn = None
                self.logger.logEvent('Info', f'Closed connection to DB')
                return
            self.logger.logEvent('Info', f'No open connections to close')
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while closing DB Connection: {pe}')