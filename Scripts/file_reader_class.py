'''This file contains the FileReader class'''
from logger_class import Logger


class FileReader:
    '''This class contains methods for reading lines'''
    file_object = None
    
    
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
    def moveToLine(cls, line_number):
        '''This method moves the pointer to the given line in the file'''
        try:
            if cls.file_object:
                cls.file_object.seek(0)
                for _ in range(line_number):
                    cls.file_object.readline()
                Logger.logEvent('Info', f'Moved to line {line_number} in file')
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
            return tuple(tuple(cls.file_object.readline().rstrip('\r\n').split(',')) for _ in range(no_of_lines))
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
    def closeFile(cls):
        '''This method closes the file if it was opened'''
        try:
            if cls.file_object:
                cls.file_object.close()
                cls.file_object = None
                Logger.logEvent('Info', f'File closed successfully')
                return
            Logger.logEvent('Info', f'No file to close')
        except Exception as e:
            Logger.logEvent('Error', f'Error while closing file - {e}')