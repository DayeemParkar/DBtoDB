from time import perf_counter
from config import BATCH_SIZE
from file_reader_class import FileReader
from helpers import startUp, getInsertionStartingBatch, insertBatch, shutdown

try:
    script_start_time = perf_counter()
    entries = startUp()
    start = getInsertionStartingBatch(entries)
    file_reader = FileReader()

    if start > -1:
        start_time = perf_counter()
        no_of_batches = file_reader.no_of_entries // BATCH_SIZE
        if file_reader.no_of_entries % BATCH_SIZE != 0:
            no_of_batches += 1
        for curr_batch in range(start, no_of_batches):
            insertBatch(curr_batch)
        print('Overall insertion time:', perf_counter() - start_time)
    else:
        print('Invalid input')

    shutdown()
    print('Overall execution time:', perf_counter() - script_start_time)
except KeyboardInterrupt as ke:
    shutdown()
    print('Execution was interrupted by keyboard input')
except Exception as e:
    shutdown()
    print('Error occured while execution of program')