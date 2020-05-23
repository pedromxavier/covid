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
from api import API
import api_lib

class APIIO:

    CSV_HEADER = ['date', 'state', 'city', 'region'] + API.CAUSES

    def to_json(self, fname: str, results: list) -> str:
        if not fname.endswith('.json'):
            fname = f'{fname}.json'

        with open(fname, 'w') as file:
            file.write(json.dumps(results))

    def to_csv(self, fname: str, results: list):
        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'

        with open(fname, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.CSV_HEADER)
            writer.writeheader()
            for result in results:
                row = {key : (str(result[key]) if key in result else "") for key in self.CSV_HEADER}
                writer.writerow(row)

    def union(self, res: list, **kwargs) -> list:
        """
        """
        api_lib.kwget(kwargs, {
            'region': ''
        })
        union_data = {}
        for data in res:
            date = data['date']
            if date not in union_data:
                union_data[date] = {cause_key : 0 for cause_key in API.APIResults.keys}
            for cause_key in API.CAUSE_KEYS:
                union_data[date][cause_key] += data[cause_key]
        results = []
        for date in union_data:
            union_data[date]['date'] = date
            union_data[date]['region'] = kwargs['region']
            results.append(union_data[date])
        return sorted(results, key=lambda x: x['date'])