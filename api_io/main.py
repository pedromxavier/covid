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

## Local
from api import API, CAUSES
import api_lib

class APIIO:
    CSV_HEADER = ('date', 'state', 'city', 'region', 'gender', 'age', 'places') + CAUSES

    @classmethod
    def to_csv(cls, fname: str, results: list):
        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'

        with open(fname, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=cls.CSV_HEADER)
            writer.writeheader()
            for result in results:
                row = {key : result[key] for key in cls.CSV_HEADER}
                writer.writerow(row)

    @classmethod
    def save(cls, fname: str, results: list) -> None:
        api_lib.pkdump(fname, results)

    @classmethod
    def load(cls, fname: str) -> list:
        return api_lib.pkload(fname)

    @classmethod
    def union(cls, results: list, **kwargs) -> list:
        """
        """
        ...