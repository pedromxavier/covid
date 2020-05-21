## Standard Library
import os
import sqlite3
import datetime

## Local
from database import DataBase

def load(fname: str):
    with open(fname, 'r') as file:
        return file.read()

def dump(fname: str, s: str):
    with open(fname, 'w') as file:
        return file.write(s)

def flatten(x: list):
    return sum(x, [])

class APIDB(DataBase):

    TYPE_INT = 'INTEGER NOT NULL DEFAULT 0'
    TYPE_TEXT = 'TEXT'
    TYPE_DATE = 'DATE'

    DB_FNAME = 'covid'

    SQL_FNAME = 'covid.sql'

    TABLE = {
        'CAUSA': {
            'COVID': TYPE_INT,
            'SRAG': TYPE_INT,
            'PNEUMONIA': TYPE_INT,
            'INSUFICIENCIA_RESPIRATORIA': TYPE_INT,
            'SEPTICEMIA': TYPE_INT,
            'INDETERMINADA': TYPE_INT,
            'OUTRAS': TYPE_INT,
        },
        'LOCAL': {
            'CIDADE': TYPE_TEXT,
            'ESTADO': TYPE_TEXT,
            'LUGAR': TYPE_TEXT,
        },
        'DATA': {
            'DIA': TYPE_DATE
        }
    }

    TABLE_NAME = 'obitos'

    def __init__(self):
        DataBase.__init__(self, self.DB_FNAME)
        self.build()

    def connect(self, **kwargs):
        return DataBase.connect(self, **{
            'detect_types': (sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES),
            **kwargs
            })

    def build(self):
        if not os.path.exists(self.SQL_FNAME):
            dump(self.SQL_FNAME, self.sql_template)

        if not os.path.exists(self.dbname):
            with self as db:
                db(self.sql, None)

    def store(self, res: list):
        """ store([API.APIRequest, ... , API.APIRequest])
        """
        rows = []
        for req in res:
            if not req.success:
                continue
            for row in self.extract(req):
                rows.append(row)

        query, params = self.insert_query(rows)

        return self(query, params)

    def insert_query(self, rows: list) -> (str, tuple):
        query = f"INSERT INTO {self.TABLE_NAME} {self.columns} VALUES {self.wildcards(rows)}"
        params = tuple(flatten(rows))
        return query, params

    def wildcards(self, rows: list) -> str:
        """
        """
        return ", ".join([f"({','.join(['?'] * len(row))})" for row in rows])

    def extract(self, req):
        """
        """
        ## get result data
        data = req.results

        ## today's date
        today = datetime.date.today()

        ## 2019 values
        yield [self.extract_column(data, column, 2019) for column in self.columns]

        ## 2020 values
        if (data['date'] <= today):
            yield [self.extract_column(data, column, 2020) for column in self.columns]
    
    def extract_column(self, data: dict, column: str, year: int):
        if column in self.TABLE['DATA']:
            return datetime.date(year, data['date'].month, data['date'].day)
        elif column in self.TABLE['CAUSA']:
            return data[f"{column}_{year}"]
        elif column in self.TABLE['LOCAL']:
            if column == 'CIDADE':
                return data['city']
            elif column == 'ESTADO':
                return data['state']
            elif column == 'LUGAR':
                return data['place']
        else:
            raise ValueError(f"Campo {column} desconhecido.")

    @property
    def columns(self):
        return f"({','.join(self.columns)})"

    @property
    def COLUMNS(self):
        return sum([list(sec.keys()) for sec in self.TABLE], [])

    @property
    def sql(self):
        return load(self.SQL_FNAME)

    @property
    def sql_template(self):
        sections = []
        for section in self.TABLE:
            fields = []
            for field in self.TABLE[section]:
                fields.append(f"{field} {self.TABLE[section][field]}")
            sections.append(f"\t/* {section} */\n\t" + ",\n\t".join(fields))
        body = ",\n\n".join(sections)
        return f"""CREATE TABLE IF EXISTS {self.TABLE_NAME} (
{body}
);"""

    

