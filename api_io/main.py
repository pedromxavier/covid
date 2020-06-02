""" :: IO ::
    ========

    Este módulo procura relacionar os diferentes tipos de entrada/saída dos dados:
    - .json
    - .csv
    - list
"""
## Standard Library
import csv
import json
import os
import queue
import _thread as thread
import multiprocessing as mp

## Local
import api_lib
from api_constants import CAUSES, BLOCK_SIZE

def get_block(iterator, n: int) -> list:
    block = []
    for _ in range(n):
        try:
            block.append(next(iterator))
        except StopIteration:
            break
    return block

class Writer:

    CSV_HEADER = ('id', 'date', 'state', 'city', 'region', 'gender', 'age', 'place') + CAUSES

    def _write(self, fname: str, results_queue: mp.Queue):
        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'
        
        with open(fname, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.CSV_HEADER)
            writer.writeheader()

            result = results_queue.get(True)
            while result is not None:
                writer.writerow({key : result[key] for key in self.CSV_HEADER})
                result = results_queue.get(True)

    def write(self, fname: str, results_queue: mp.Queue):
        thread.start_new(self._write, (fname, results_queue))