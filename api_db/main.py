## Standard Library
import os
import sqlite3

## Local
from db import DataBase

def load(fname: str):
    with open(fname, 'r') as file:
        return file.read()

def dump(fname: str, s: str):
    with open(fname, 'w') as file:
        return file.write(s)

class DB(DataBase):

    FNAME = 'covid'

    SQL = 'covid.sql'

    def __init__(self):
        DataBase.__init__(self, self.FNAME)
        self.build()

    def connect(self, **params):
        return DataBase.connect(self, {
            'detect_types': (sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES),
            **params
            })

    def build(self):
        if not os.path.exists(self.dbname):
            with self as db:
                db(self.sql)

    @property
    def sql(self):
        return load(self.SQL)

    

