'''This file contains the Logger class'''
from datetime import datetime


class Logger():
    '''This class contains methods for logging events'''
    logger = None
    
    
    @classmethod
    def initializeLogger(cls, logger_file_path, mode):
        '''This method opens the logger file'''
        try:
            if not cls.logger:
                cls.logger = open(logger_file_path, mode=mode)
        except Exception as e:
            print(f'Error while initializing logger: {e}')
    
    
    @classmethod
    def logEvent(cls, log_type, msg):
        '''This method logs an event into the file'''
        try:
            if cls.logger:
                cls.logger.write(f"{datetime.now()} - {log_type}: {msg}\n")
            else:
                print('Logger file has not been initialized')
        except Exception as e:
            print(f'Error while logging event {msg} of type {log_type}: {e}')
    
    
    @classmethod
    def closeLogger(cls):
        '''This method closes the logger file'''
        try:
            if cls.logger:
                cls.logger.close()
                cls.logger = None
        except Exception as e:
            print(f'Error while closing logger: {e}')