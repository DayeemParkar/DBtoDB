'''This file contains several helper methods'''
from time import perf_counter
from logger_class import Logger
from file_reader_class import FileReader
from config import FILE_LOC, FILE_NAME , LOGGER_FILE_LOC, BATCH_SIZE
from db_class import DBConnection
from table_operations_class import TableOperations

logger = Logger()
db_connection = DBConnection()
table_operations = TableOperations()
file_reader = FileReader()


def startUp():
    '''This method creates logger, file object and a DB Connection'''
    try:
        logger.initializeLogger(LOGGER_FILE_LOC, 'a+')
        file_reader.initializeFile(file_path=FILE_LOC, mode='r')
        print('Retrieving number of entries in csv')
        no_of_entries = file_reader.getNumberOfEntries()
        columns = file_reader.getLines(1)[0]
        no_of_cols = len(columns)
        db_connection.dbConnect()
        table_operations.setDatabaseAndCursor(db_connection)
        
        
        table_name = FILE_NAME.rstrip('.csv')
        insert_query = [f'INSERT INTO {table_name} VALUES ', '(' + ', '.join([r'%s'] * no_of_cols) + ')']
        create_query = f'CREATE TABLE {table_name} (' + ', '.join([f"col{i} varchar" for i in range(no_of_cols)]) + ');'
        
        hasHeader = input(f'Does the file have header?[y/n]: ')
        if hasHeader.lower() == 'y':
            create_query = f'CREATE TABLE {table_name} (' + ', '.join([f"{columns[i].replace(' ', '_')} varchar" for i in range(no_of_cols)]) + ');'
            file_reader.setHasHeader(True)
            no_of_entries -= 1
        
        file_reader.moveToTop()
        file_reader.setNumberOfEntries(no_of_entries)
        table_operations.setInsertQuery(insert_query)
        table_operations.createTable(table_name, create_query)
        logger.logEvent('Info', 'Startup successful')
        return table_operations.getCount(table_name)
    except Exception as e:
        logger.logEvent('Error', f'Error during startup: {e}')
        return -1


def getInsertionStartingBatch(entries):
    '''This method get the starting batch for the insertion of data'''
    try:
        table_name = FILE_NAME.rstrip('.csv')
        if entries == -1 or entries == 0:
            logger.logEvent('Info', 'No entries, creating table if not exists')
            return 0
        decision = input(f'{entries} entries detected in table, would you like to continue where you left off?[y/n]: ')
        if decision.lower() == 'y':
            file_reader.moveToLine(entries)
            logger.logEvent('Info', f'Continuing insertion from entry {entries}')
            return entries // BATCH_SIZE
        if decision.lower() == 'n':
            table_operations.truncateTable(table_name)
            logger.logEvent('Info', 'Restarting insertion')
            return 0
        logger.logEvent('Warning', 'Invalid input while choosing how to process with insertion')
        return -1
    except Exception as e:
        logger.logEvent('Error', f'Error during retrieval of starting batch: {e}')
        return -1


def insertBatch(curr_batch):
    '''This method inserts the batch and moves on to the next if successful'''
    try:
        batch_start_time = perf_counter()
        lines = file_reader.getLines(min(BATCH_SIZE, file_reader.no_of_entries - (BATCH_SIZE * curr_batch)))
        is_insertion_successful = table_operations.insertRows(lines)
        if is_insertion_successful:
            msg = f'Batch {curr_batch + 1} completed in {perf_counter() - batch_start_time} seconds'
            print(msg)
            logger.logEvent('Info', msg)
        else:
            msg = f'Batch {curr_batch + 1} failed after {perf_counter() - batch_start_time} seconds. Retrying'
            print(msg)
            logger.logEvent('Error', msg)
            file_reader.moveToLine(curr_batch * BATCH_SIZE)
    except Exception as e:
        logger.logEvent('Error', f'Error during insertion of batch {curr_batch + 1}: {e}')


def shutdown():
    '''This method closes connections'''
    try:
        db_connection.closeDbConnection()
        file_reader.closeFile()
        logger.closeLogger()
        logger.logEvent('Info', 'Shutdown successful')
    except Exception as e:
        logger.logEvent('Error', f'Error during shutdown: {e}')