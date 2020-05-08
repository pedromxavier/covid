## Standard Library
import csv
import datetime

## Third-Party
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

## Local
from api import API
import api_lib

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

    w = 8.0 #in
    h = 5.0 #in

    def __init__(self, fname:str):
        self.fname = fname
        self.x, self.y = self.parse_csv(self.fname) 

    @staticmethod
    def set_nan(x: np.ndarray):
        z = np.copy(x)
        z[z == 0.0] = np.nan
        return z

    def year_diff(self, cause):
        assert cause != 'COVID'
        return self.y[f'{cause}_2020'] - self.y[f'{cause}_2019']

    def diff_covid_resp(self, **kwargs):
        fig, ax = plt.subplots()

        y = sum(self.year_diff(cause) for cause in self.RESPIRATORY)

        ax.bar(self.x, self.set_nan(self.y['COVID_2020']), width=0.8, label='COVID 2020')
        ax.bar(self.x, self.set_nan(y), width=0.8, label='DOENÇAS RESPITÓRIAS 2020 - 2019')

        plt.legend()
        plt.title('Aumento das doenças respitatórias vs. COVID')

        fig.set_size_inches(self.w, self.h)

        self.plot(**kwargs)

    def plot_all(self, **kwargs):
        fig, ax = plt.subplots()
        for cause_year in self.CAUSE_YEAR:
            ax.plot(self.x, self.set_nan(self.y[cause_year]), label=cause_year)
        plt.legend()
        fig.set_size_inches(self.w, self.h)
        self.plot(**kwargs)
        
    def plot(self, **kwargs):
        ## Get fname kwarg
        fname = api_lib.kwget('save', kwargs)
        title = api_lib.kwget('title', kwargs, '')

        ## Corrige o nome do arquivo
        if fname is not None and not fname.endswith('.png'):
            fname = f'{fname}.png'

        ## Adiciona Título ao plot.
        plt.title(title)

        ## Rotaciona as datas no eixo x.
        plt.xticks(rotation=70)

        ## Output
        if fname is None:
            plt.show()
        else:
            plt.savefig(fname)

    @classmethod
    def parse_csv(cls, fname: str, **kwargs) -> (list, dict):
        accumulate = api_lib.kwget('accumulate', kwargs, False)

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
                if accumulate:
                    y[cause] = np.cumsum(y[cause], dtype=np.float64)
                else:
                    y[cause] = np.array(y[cause], dtype=np.float64)
        return x, y