'''This file contains the table operations class'''
import psycopg2
from logger_class import Logger


class TableOperations:
    '''Class containing methods to perform operations on a table'''
    insert_query = None
    retries = None
    database_object = None
    cursor = None
    logger = Logger()
    
    
    def setInsertQuery(self, insert_query):
        '''This method sets the insert query'''
        self.insert_query = insert_query
    
    
    def setDatabaseAndCursor(self, db):
        '''This method sets the database and cursor object'''
        self.database_object = db
        self.cursor = db.getCursor()
    
    
    def createTable(self, table_name, create_query):
        '''This method creates the required table if it doesn't exist'''
        try:
            self.cursor.execute(create_query)
            self.database_object.commitChanges()
            self.logger.logEvent('Info', f'Created table - {table_name}')
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Could not create table {table_name}: {pe}')
            self.database_object.rollback()
    
    
    def insertRows(self, entries):
        '''This method inserts rows into table'''
        try:
            if not self.retries:
                self.retries = 0
            if self.retries > 3:
                return False
            values = ','.join(self.cursor.mogrify(self.insert_query[1], i).decode('utf-8')
                for i in entries)
            self.cursor.execute(self.insert_query[0] + values + ';')
            self.database_object.commitChanges()
            self.logger.logEvent('Info', f'Inserted {len(entries)} rows into table')
            self.retries = None
            return True
        except psycopg2.DatabaseError as dbe:
            self.logger.logEvent('Error', f'Database error while inserting {len(entries)} entries: {dbe}')
            self.database_object.rollback()
            self.retries += 1
            self.insertRows(entries)
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while inserting {len(entries)} entries: {pe}')
            self.database_object.rollback()
            return False
    
    
    def truncateTable(self, table_name):
        '''This method drops table if it exists'''
        try:
            self.cursor.execute(f"TRUNCATE TABLE {table_name};")
            if self.database_object.commitChanges():
                self.logger.logEvent('Info', f'Successfully truncated table {table_name}')
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while truncating table {table_name}: {pe}')
            self.database_object.rollback()
    
    
    def dropTable(self, table_name):
        '''This method drops table if it exists'''
        try:
            self.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            if self.database_object.commitChanges():
                self.logger.logEvent('Info', f'Dropped table {table_name}')
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while dropping table {table_name}: {pe}')
            self.database_object.rollback()
    
    
    def getCount(self, table_name):
        '''This method drops table if it exists'''
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            rows = self.cursor.fetchall()
            if rows:
                self.logger.logEvent('Info', f'Retrieved number of rows of table {table_name}')
                return rows[0][0]
            self.logger.logEvent('Error', f'Error while counting rows of table {table_name}: {pe}')
            return -1
        except psycopg2.Error as pe:
            self.logger.logEvent('Error', f'Error while counting rows of table {table_name}: {pe}')
            return -1