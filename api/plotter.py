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

    def __init__(self, fname:str):
        self.fname = fname
        self.x, self.y = self.parse_csv(self.fname) 

    def year_diff(self, cause):
        assert cause != 'COVID'
        return self.y[f'{cause}_2020'] - self.y[f'{cause}_2019']

    def diff_covid_resp(self, **kwargs):
        fig, ax = plt.subplots()

        ax.plot(self.x, self.y['COVID_2020'], label='COVID 2020')
        ax.plot(self.x, sum(self.year_diff(cause) for cause in self.RESPIRATORY), label='DOENÇAS RESPITÓRIAS 2020 - 2019')

        plt.legend()
        plt.title('Aumento das doenças respitatórias vs. COVID')

        self.plot(**kwargs)

    def plot_all(self, **kwargs):
        fig, ax = plt.subplots()
        for cause_year in self.CAUSE_YEAR:
            ax.plot(self.x, self.y[cause_year], label=cause_year)
        plt.legend()
        self.plot(**kwargs)

    def compare_cities(self, *cities, **kwargs):
        ...

    def compare_causes(self, *causes, **kwargs):
        ...

    def compare_years(self, *causes):
        n = len(causes)
        ncols = min(self, self.MAX_COLS, n)
        nrows = (n//ncols)
        fig, axs = plt.subplots((n//ncols), ncols)
        for i in range(nrows):
            for j in range(ncols):
                cause = causes[i*nrows + j]
                axs[i, j].plot(self.x, np.cumsum(self.y[cause]))

    def plot(self, fname: str=None, **kwargs):

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
    def parse_csv(cls, fname: str) -> (list, dict):
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
        return x, y

if __name__ == '__main__':
    plot = Plotter('RJ')
    plot.diff_covid_resp(title='Estado do Rio de Janeiro')
