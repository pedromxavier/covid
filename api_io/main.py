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
import api_lib
from api_constants import CAUSES


class APIIO:
    CSV_HEADER = ('date', 'state', 'city', 'region', 'gender', 'age', 'place') + CAUSES

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
    def union(cls, results: list, **kwargs) -> list:
        """
        """
        ...