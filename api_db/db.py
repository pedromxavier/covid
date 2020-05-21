import sqlite3

class DataBase:

    def __init__(self, fname: str):
        self.fname = fname
        self._conn = None

    def __repr__(self):
        return f"DataBase({self.fname!r})"

    @classmethod
    def open(cls, fname: str):
        db = cls(fname)
        db.connect()
        return db
        
    def connect(self, **params):
        self._conn = sqlite3.connect(self.dbname, **params)

    def close(self):
        self._conn.close()
        self._conn = None

    def __enter__(self, *args, **kwargs):
        self.connect()
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __call__(self, cmd: str, *params: tuple):
        if self._conn is None: # Not connected
            raise RuntimeError('Not connected to database.')

        self.cursor = self._conn.cursor()
        results = []
        try:
            self.cursor.execute(cmd, params)
            results = self.cursor.fetchall()
            self._conn.commit()
        except sqlite3.Error as error:
            self._conn.rollback()
            raise
        finally:
            self.cursor.close()
            return results

    @property
    def dbname(self):
        return f'{self.fname}.db'



    