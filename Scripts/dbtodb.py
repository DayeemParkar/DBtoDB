from time import perf_counter
from config import BATCH_SIZE, FILE_ENTRIES
from helpers import startUp, getInsertionStartingLine, insertBranch, shutdown


entries = startUp()
start = getInsertionStartingLine(entries)

if start > -1:
    start_time = perf_counter()
    no_of_batches = FILE_ENTRIES // BATCH_SIZE
    if FILE_ENTRIES % BATCH_SIZE != 0:
        no_of_batches += 1
    curr_batch = start
    while curr_batch < no_of_batches:
        curr_batch += insertBranch(curr_batch)
    print('Overall time:', perf_counter() - start_time)
else:
    print('Invalid input')

shutdown()