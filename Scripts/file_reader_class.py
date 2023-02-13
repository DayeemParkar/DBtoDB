'''This file contains the FileReader class'''
from logger_class import Logger
from time import perf_counter


class FileReader:
    '''This class contains methods for reading lines'''
    file_object = None
    has_header = False
    no_of_entries = 0
    values = None
    
    
    @classmethod
    def initializeFile(cls, file_path, mode):
        '''This method opens the given file'''
        try:
            cls.file_object = open(file_path, mode=mode)
            Logger.logEvent('Info', f'Initialized file {file_path} with mode {mode}')
            return True
        except FileNotFoundError as fnfe:
            Logger.logEvent('Error', f'File {file_path} not found - {fnfe}')
            return False
        except IOError as ioe:
            Logger.logEvent('Error', f'IOError while initializing {file_path} - {ioe}')
            return False
        except Exception as e:
            Logger.logEvent('Error', f'Error while initializing file {file_path} - {e}')
            return False
    
    
    @classmethod
    def setHasHeader(cls, value):
        '''This method sets the has_header boolean'''
        cls.has_header = value
    
    
    @classmethod
    def setNumberOfEntries(cls, value):
        '''This method sets the has_header boolean'''
        cls.no_of_entries = value
    
    
    @classmethod
    def moveToTop(cls):
        '''This method moves pointer to top of file'''
        try:
            cls.file_object.seek(0)
            if cls.has_header:
                cls.file_object.readline()
            Logger.logEvent('Info', 'Moved to top of file')
        except Exception as e:
            Logger.logEvent('Event', 'Failed to move to top of file')
    
    
    @classmethod
    def moveToLine(cls, line_number):
        '''This method moves the pointer to the given line in the file'''
        try:
            if cls.file_object:
                start = perf_counter()
                cls.moveToTop()
                for _ in range(line_number):
                    cls.file_object.readline()
                Logger.logEvent('Info', f'Moved to line {line_number} in file in {perf_counter() - start} seconds')
            else:
                Logger.logEvent('Error', f'File has not been initialized to move to line')
                return False
        except StopIteration as si:
            Logger.logEvent('Error', f'Reached end of file while moving to line {line_number} - {si}')
            return False
        except IOError as ioe:
            Logger.logEvent('Error', f'IOError while moving to line {line_number} in file - {ioe}')
            return False
        except Exception as e:
            Logger.logEvent('Error', f'Error while moving to line {line_number} in file - {e}')
            return False
    
    
    @classmethod
    def getLines(cls, no_of_lines):
        '''This method returns a tuple of lines'''
        try:
            start = perf_counter()
            cls.values = tuple(tuple(cls.file_object.readline().rstrip('\r\n').split(',')) for _ in range(no_of_lines))
            Logger.logEvent('Info', f'Retrieved {no_of_lines} lines in {perf_counter() - start} seconds')
            return cls.values
        except IOError as ioe:
            Logger.logEvent('Error', f'IOError while getting lines from file - {ioe}')
            return tuple()
        except StopIteration as si:
            Logger.logEvent('Error', f'Reached end of file while getting lines from file - {si}')
            return tuple()
        except Exception as e:
            Logger.logEvent('Error', f'Error while getting lines from file - {e}')
            return tuple()
    
    
    @classmethod
    def getNumberOfEntries(cls):
        '''This method returns the number of entries in the file'''
        try:
            if cls.file_object:
                start = perf_counter()
                count = 0
                cls.moveToTop()
                while cls.file_object.readline():
                    count += 1
                cls.moveToTop()
                Logger.logEvent('Info', f'{count} entries detected in file in {perf_counter() - start} seconds')
                return count
        except Exception as e:
            Logger.logEvent('Error', f'Error while getting number of entries of file - {e}')
            return -1
    
    
    @classmethod
    def closeFile(cls):
        '''This method closes the file if it was opened'''
        try:
            if cls.file_object:
                cls.file_object.close()
                cls.file_object = None
                Logger.logEvent('Info', f'File closed successfully')
        except Exception as e:
            Logger.logEvent('Error', f'Error while closing file - {e}')