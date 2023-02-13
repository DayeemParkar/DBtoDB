'''This file contains the FileReader class'''
from logger_class import Logger
from time import perf_counter


class FileReader:
    '''This class contains methods for reading lines'''
    _instance = None
    file_object = None
    has_header = False
    no_of_entries = 0
    values = None
    logger = Logger()
    
    
    def __new__(cls):
        '''This method overrides new to make it a singleton'''
        if not cls._instance:
            cls._instance = super(FileReader, cls).__new__(cls)
        return cls._instance
    
    
    def initializeFile(self, file_path, mode):
        '''This method opens the given file'''
        try:
            self.file_object = open(file_path, mode=mode)
            self.logger.logEvent('Info', f'Initialized file {file_path} with mode {mode}')
            return True
        except FileNotFoundError as fnfe:
            self.logger.logEvent('Error', f'File {file_path} not found - {fnfe}')
            return False
        except IOError as ioe:
            self.logger.logEvent('Error', f'IOError while initializing {file_path} - {ioe}')
            return False
        except Exception as e:
            self.logger.logEvent('Error', f'Error while initializing file {file_path} - {e}')
            return False
    
    
    def setHasHeader(self, value):
        '''This method sets the has_header boolean'''
        self.has_header = value
    
    
    def setNumberOfEntries(self, value):
        '''This method sets the has_header boolean'''
        self.no_of_entries = value
    
    
    def moveToTop(self):
        '''This method moves pointer to top of file'''
        try:
            self.file_object.seek(0)
            if self.has_header:
                self.file_object.readline()
            self.logger.logEvent('Info', 'Moved to top of file')
        except Exception as e:
            self.logger.logEvent('Event', 'Failed to move to top of file')
    
    
    def moveToLine(self, line_number):
        '''This method moves the pointer to the given line in the file'''
        try:
            if self.file_object:
                start = perf_counter()
                self.moveToTop()
                for _ in range(line_number):
                    self.file_object.readline()
                self.logger.logEvent('Info', f'Moved to line {line_number} in file in {perf_counter() - start} seconds')
            else:
                self.logger.logEvent('Error', f'File has not been initialized to move to line')
                return False
        except StopIteration as si:
            self.logger.logEvent('Error', f'Reached end of file while moving to line {line_number} - {si}')
            return False
        except IOError as ioe:
            self.logger.logEvent('Error', f'IOError while moving to line {line_number} in file - {ioe}')
            return False
        except Exception as e:
            self.logger.logEvent('Error', f'Error while moving to line {line_number} in file - {e}')
            return False
    
    
    def getLines(self, no_of_lines):
        '''This method returns a tuple of lines'''
        try:
            start = perf_counter()
            self.values = tuple(tuple(self.file_object.readline().rstrip('\r\n').split(',')) for _ in range(no_of_lines))
            self.logger.logEvent('Info', f'Retrieved {no_of_lines} lines in {perf_counter() - start} seconds')
            return self.values
        except IOError as ioe:
            self.logger.logEvent('Error', f'IOError while getting lines from file - {ioe}')
            return tuple()
        except StopIteration as si:
            self.logger.logEvent('Error', f'Reached end of file while getting lines from file - {si}')
            return tuple()
        except Exception as e:
            self.logger.logEvent('Error', f'Error while getting lines from file - {e}')
            return tuple()
    
    
    def getNumberOfEntries(self):
        '''This method returns the number of entries in the file'''
        try:
            if self.file_object:
                start = perf_counter()
                count = 0
                self.moveToTop()
                while self.file_object.readline():
                    count += 1
                self.moveToTop()
                self.logger.logEvent('Info', f'{count} entries detected in file in {perf_counter() - start} seconds')
                return count
        except Exception as e:
            self.logger.logEvent('Error', f'Error while getting number of entries of file - {e}')
            return -1
    
    
    def closeFile(self):
        '''This method closes the file if it was opened'''
        try:
            if self.file_object:
                self.file_object.close()
                self.file_object = None
                self.logger.logEvent('Info', f'File closed successfully')
        except Exception as e:
            self.logger.logEvent('Error', f'Error while closing file - {e}')