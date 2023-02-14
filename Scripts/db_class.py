'''This file contains the DBConnection class'''
import psycopg2
from logger_class import Logger
from config import DBVARS


class DBConnection:
    '''Class containing methods to connect to DB'''
    # class members
    conn = None
    cur = None
    _instance = None
    logger = Logger()
    
    
    def __new__(cls):
        '''This method overrides new to make it a singleton'''
        if not cls._instance:
            cls._instance = super(DBConnection, cls).__new__(cls)
        return cls._instance
    
    
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
    
    
    def getCursor(self):
        '''This method returns DB cursor object'''
        try:
            self.dbConnect()
            self.logger.logEvent('Info', f'Getting DB cursor - {self.cur}')
            return self.cur
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Could not get DB cursor - {pe}')
            return None
    
    
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