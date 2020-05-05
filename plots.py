import matplotlib.pyplot as plt
import csv
import api_lib
import numpy as np
import datetime
from api import API

class Plotter:

    MAX_COLS = 3

    CAUSES = {
        'COVID',
        'SRAG',
        'PNEUMONIA',
        'INSUFICIENCIA_RESPIRATORIA',
        'SEPTICEMIA',
        'INDETERMINADA',
        'OUTRAS'
    }

    RESPIRATORY = {
        'SRAG',
        'PNEUMONIA',
        'INSUFICIENCIA_RESPIRATORIA',
    }

    CAUSE_YEAR = [f'{cause}_{year}' for cause, year in API.CAUSE_YEAR]
    
    x = []
    y = {}

    INIT = False

    @classmethod
    def year_diff(cls, cause):
        assert cause != 'COVID'
        return cls.y[f'{cause}_2020'] - cls.y[f'{cause}_2019']

    @classmethod
    def diff_covid_resp(cls, **kwargs):
        fig, ax = plt.subplots()

        ax.plot(cls.x, cls.y['COVID_2020'], label='COVID 2020')
        ax.plot(cls.x, sum(cls.year_diff(cause) for cause in cls.RESPIRATORY), label='DOENÇAS RESPITÓRIAS 2020 - 2019')

        plt.legend()

        cls.plot(**kwargs)

    @classmethod
    def plot_all(cls, **kwargs):
        fig, ax = plt.subplots()
        for cause_year in cls.CAUSE_YEAR:
            ax.plot(cls.x, cls.y[cause_year], label=cause_year)
        plt.legend()
        cls.plot(**kwargs)

    @classmethod
    def compare_cities(cls, *cities, **kwargs):
        ...

    @classmethod
    def compare_causes(cls, *causes, **kwargs):
        ...

    @classmethod
    def compare_years(cls, *causes):
        n = len(causes)
        ncols = min(cls, cls.MAX_COLS, n)
        nrows = (n//ncols)
        fig, axs = plt.subplots((n//ncols), ncols)
        for i in range(nrows):
            for j in range(ncols):
                cause = causes[i*nrows + j]
                axs[i, j].plot(cls.x, np.cumsum(cls.y[cause]))

    @classmethod
    def plot(cls, fname: str=None, **kwargs):

        ## Adiciona Título ao plot.
        if 'title' in kwargs: plt.title(kwargs['title'])

        ## Rotaciona as datas no eixo x.
        plt.xticks(rotation=70)

        ## Output
        if fname is None:
            plt.show()
        else:
            plt.savefig(fname)

    @classmethod
    def init_csv(cls, fname: str):
        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'

        with open(fname, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)
            table = {header[i] : i for i in range(len(header))}
            x = []
            y = {}
            row = next(reader)
            for cause in cls.CAUSE_YEAR:
                y[cause] = [int(row[table[cause]])]
            
            for row in reader:
                x.append(datetime.date.fromisoformat(row[table['date']]))
                for cause in cls.CAUSE_YEAR:
                    y[cause].append(int(row[table[cause]]))
                        
            for cause in cls.CAUSE_YEAR:
                ## Adjust size
                while len(y[cause]) < len(x):
                    y[cause].append(0)
                while len(y[cause]) > len(x):
                    y[cause].pop(-1)
                y[cause] = np.cumsum(y[cause], dtype=np.float64)
                y[cause][y[cause] == 0] = np.nan
        cls.x = x
        cls.y = y
        cls.INIT = True

if __name__ == '__main__':
    Plotter.init_csv('RJ')
    Plotter.diff_covid_resp(title='Estado do Rio de Janeiro')
