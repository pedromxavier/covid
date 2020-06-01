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

class APIIO:
    CSV_HEADER = ('date', 'state', 'city', 'region', 'gender', 'age', 'place') + CAUSES

    @classmethod
    def to_csv(cls, fname: str, results: list):
        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'

        with open(fname, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=cls.CSV_HEADER)
            writer.writeheader()
            
            for result in get_block(results, BLOCK_SIZE):
                writer.writerow({key : result[key] for key in cls.CSV_HEADER})

    @classmethod
    def join_csv(cls, output_fname:str, fnames: list, delete_input=False):
        lines = []
        for fname in fnames:
            if os.path.exists(fname):
                with open(fname, 'r') as file:
                    header = file.readline()
                    lines.extend(file)

        with open(output_fname, 'w') as file:
            file.write(header)
            file.writelines(lines)

        if delete_input:
            for fname in fnames:
                if os.path.exists(fname): os.remove(fname)

    @classmethod
    def union(cls, results: list, **kwargs) -> list:
        """
        """
        ...