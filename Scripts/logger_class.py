'''This file contains the Logger class'''
from datetime import datetime


class Logger():
    '''This class contains methods for logging events'''
    _instance = None
    logger = None
    
    
    def __new__(cls):
        '''This method overrides new to make it a singleton'''
        if not cls._instance:
            cls._instance = super(Logger, cls).__new__(cls)
        return cls._instance
    
    
    def initializeLogger(self, logger_file_path, mode):
        '''This method opens the logger file'''
        try:
            if not self.logger:
                self.logger = open(logger_file_path, mode=mode)
        except Exception as e:
            print(f'Error while initializing logger: {e}')
    
    
    def logEvent(self, log_type, msg):
        '''This method logs an event into the file'''
        try:
            if self.logger:
                self.logger.write(f"{datetime.now()} - {log_type}: {msg}\n")
            else:
                print('Logger file has not been initialized')
        except Exception as e:
            print(f'Error while logging event {msg} of type {log_type}: {e}')
    
    
    def closeLogger(self):
        '''This method closes the logger file'''
        try:
            if self.logger:
                self.logger.close()
                self.logger = None
        except Exception as e:
            print(f'Error while closing logger: {e}')