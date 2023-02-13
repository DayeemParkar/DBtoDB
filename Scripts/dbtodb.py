from time import perf_counter
from config import BATCH_SIZE
from file_reader_class import FileReader
from helpers import startUp, getInsertionStartingLine, insertBatch, shutdown


entries = startUp()
start = getInsertionStartingLine(entries)

if start > -1:
    start_time = perf_counter()
    no_of_batches = FileReader.no_of_entries // BATCH_SIZE
    if FileReader.no_of_entries % BATCH_SIZE != 0:
        no_of_batches += 1
    for curr_batch in range(start, no_of_batches):
        insertBatch(curr_batch)
    print('Overall time:', perf_counter() - start_time)
else:
    print('Invalid input')

shutdown()