'''This file contains several helper methods'''
from time import perf_counter
from logger_class import Logger
from file_reader_class import FileReader
from config import FILE_LOC, FILE_NAME , LOGGER_FILE_LOC, BATCH_SIZE
from db_class import DBConnection


def startUp():
    '''This method creates logger, file object and a DB Connection'''
    try:
        Logger.initializeLogger(LOGGER_FILE_LOC, 'a+')
        FileReader.initializeFile(file_path=FILE_LOC, mode='r')
        print('Retrieving number of entries in csv')
        no_of_entries = FileReader.getNumberOfEntries()
        columns = FileReader.getLines(1)[0]
        no_of_cols = len(columns)
        DBConnection.dbConnect()
        
        table_name = FILE_NAME.rstrip('.csv')
        insert_query = f'INSERT INTO {table_name} VALUES (' + ', '.join([r'%s'] * no_of_cols) + ');'
        create_query = f'CREATE TABLE {table_name} (' + ', '.join([f"col{i} varchar" for i in range(no_of_cols)]) + ');'
        
        hasHeader = input(f'Does the file have header?[y/n]: ')
        if hasHeader.lower() == 'y':
            create_query = f'CREATE TABLE {table_name} (' + ', '.join([f"{columns[i].replace(' ', '_')} varchar" for i in range(no_of_cols)]) + ');'
            FileReader.setHasHeader(True)
            no_of_entries -= 1
        
        FileReader.moveToTop()
        FileReader.setNumberOfEntries(no_of_entries)
        DBConnection.setInsertQuery(insert_query)
        DBConnection.createTable(table_name, create_query)
        Logger.logEvent('Info', 'Startup successful')
        return DBConnection.getCount(table_name)
    except Exception as e:
        Logger.logEvent('Error', f'Error during startup: {e}')
        return -1


def getInsertionStartingLine(entries):
    '''This method get the starting batch for the insertion of data'''
    try:
        table_name = FILE_NAME.rstrip('.csv')
        if entries == -1 or entries == 0:
            Logger.logEvent('Info', 'No entries, creating table if not exists')
            return 0
        decision = input(f'{entries} entries detected in table, would you like to continue where you left off?[y/n]: ')
        if decision.lower() == 'y':
            FileReader.moveToLine(entries)
            Logger.logEvent('Info', f'Continuing insertion from entry {entries}')
            return entries // BATCH_SIZE
        if decision.lower() == 'n':
            DBConnection.truncateTable(table_name)
            Logger.logEvent('Info', 'Restarting insertion')
            return 0
        Logger.logEvent('Warning', 'Invalid input while choosing how to process with insertion')
        return -1
    except Exception as e:
        Logger.logEvent('Error', f'Error during retrieval of starting batch: {e}')
        return -1


def insertBatch(curr_batch):
    '''This method inserts the batch and moves on to the next if successful'''
    try:
        batch_start_time = perf_counter()
        lines = FileReader.getLines(min(BATCH_SIZE, FileReader.no_of_entries - (BATCH_SIZE * curr_batch)))
        print(lines[0])
        is_insertion_successful = DBConnection.insertRows(lines)
        if is_insertion_successful:
            msg = f'Batch {curr_batch + 1} completed in {perf_counter() - batch_start_time} seconds'
            #print(msg)
            Logger.logEvent('Info', msg)
        else:
            msg = f'Batch {curr_batch + 1} failed after {perf_counter() - batch_start_time} seconds. Retrying'
            #print(msg)
            Logger.logEvent('Error', msg)
            FileReader.moveToLine(curr_batch * BATCH_SIZE)
    except Exception as e:
        Logger.logEvent('Error', f'Error during insertion of batch {curr_batch + 1}: {e}')


def shutdown():
    '''This method closes connections'''
    try:
        DBConnection.closeDbConnection()
        FileReader.closeFile()
        Logger.closeLogger()
        Logger.logEvent('Info', 'Shutdown successful')
    except Exception as e:
        Logger.logEvent('Error', f'Error during shutdown: {e}')