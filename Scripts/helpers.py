'''This file contains several helper methods'''
from time import perf_counter
from logger_class import Logger
from file_reader_class import FileReader
from config import FILE_LOC, LOGGER_FILE_LOC, BATCH_SIZE, FILE_ENTRIES
from db_class import DBConnection


def startUp():
    '''This method creates logger, file object and a DB Connection'''
    try:
        Logger.initializeLogger(LOGGER_FILE_LOC, 'a+')
        FileReader.initializeFile(file_path=FILE_LOC, mode='r')
        DBConnection.dbConnect()
        Logger.logEvent('Info', 'Startup successful')
        return DBConnection.getCount()
    except Exception as e:
        Logger.logEvent('Error', f'Error during startup: {e}')
        return -1


def getInsertionStartingLine(entries):
    '''This method get the starting batch for the insertion of data'''
    try:
        if entries == -1:
            DBConnection.createTable()
            Logger.logEvent('Info', 'Could not retrieve entries, creating table')
            return 0
        decision = input(f'{entries} entries detected in table, would you like to continue where you left off?[y/n]: ')
        if decision.lower() == 'y':
            FileReader.moveToLine(entries)
            Logger.logEvent('Info', f'Continuing insertion from entry {entries}')
            return entries // BATCH_SIZE
        if decision.lower() == 'n':
            DBConnection.truncateTable()
            Logger.logEvent('Info', 'Restarting insertion')
            return 0
        Logger.logEvent('Warning', 'Invalid input while choosing how to process with insertion')
        return -1
    except Exception as e:
        Logger.logEvent('Error', f'Error during retrieval of starting batch: {e}')
        return -1


def insertBranch(curr_batch):
    '''This method inserts the batch and moves on to the next if successful'''
    try:
        batch_start_time = perf_counter()
        lines = FileReader.getLines(min(BATCH_SIZE, FILE_ENTRIES - (BATCH_SIZE * curr_batch)))
        is_insertion_successful = DBConnection.insertRows(lines)
        if is_insertion_successful:
            msg = f'Batch {curr_batch + 1} completed in {perf_counter() - batch_start_time} seconds'
            print(msg)
            Logger.logEvent('Info', msg)
            return 1
        msg = f'Batch {curr_batch + 1} failed after {perf_counter() - batch_start_time} seconds. Retrying'
        print(msg)
        Logger.logEvent('Error', msg)
        FileReader.moveToLine(curr_batch * BATCH_SIZE)
        return 0
    except Exception as e:
        Logger.logEvent('Error', f'Error during insertion of batch {curr_batch + 1}: {e}')
        return 0


def shutdown():
    '''This method closes connections'''
    try:
        DBConnection.closeDbConnection()
        FileReader.closeFile()
        Logger.closeLogger()
        Logger.logEvent('Info', 'Shutdown successful')
    except Exception as e:
        Logger.logEvent('Error', f'Error during shutdown: {e}')