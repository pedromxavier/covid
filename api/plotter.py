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

    def __init__(self, **kwargs):
        api_lib.kwget(kwargs, {
            'csv': None,
            'res': None,
            'w': 8.0, #inches
            'h': 5.0, #inches
        })

        if kwargs['csv'] is not None and kwargs['res'] is not None:
            raise ValueError('Especifique apenas uma forma de entrada (csv ou res).')
        elif kwargs['csv'] is not None:
            self.x, self.y = self.parse_csv(kwargs['csv'])
        elif kwargs['res'] is not None:
            self.x, self.y = self.parse_res(kwargs['res'])
        else:
            raise ValueError('É preciso especificar um arquivo .csv ou uma lista de resultados da API.')

        self.w, self.h = kwargs['w'], kwargs['h']

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
        ax.bar(self.x, self.set_nan(y), width=0.8, label='DOENÇAS RESPITÓRIAS 2020-2019')

        plt.legend()
        plt.title('Aumento das doenças respitatórias vs. COVID')
        self.plot(fig, ax, **kwargs)

    def plot_all(self, **kwargs):
        """
        """
        fig, ax = plt.subplots()
        for key in API.CAUSE_KEYS:
            ax.plot(self.x, self.set_nan(self.y[key]), label=key)
        plt.legend()
        self.plot(fig, ax, **kwargs)
        
    def plot(self, fig=None, ax=None, **kwargs):
        """
        """
        ## Get fname kwarg
        api_lib.kwget(kwargs, {
            'fname': None,
            'title': '',
        })

        ## Adjust figure size
        if fig is not None: fig.set_size_inches(self.w, self.h)

        ## Corrige o nome do arquivo
        if kwargs['fname'] is not None and not kwargs['fname'].endswith('.png'):
            kwargs['fname'] = f"{kwargs['fname']}.png"

        ## Adiciona Título ao plot.
        if kwargs['title']: plt.title(kwargs['title'])

        ## Rotaciona as datas no eixo x.
        plt.xticks(rotation=70)

        ## Output
        if kwargs['fname'] is None:
            plt.show()
        else:
            plt.savefig(kwargs['fname'])

    @classmethod
    def parse_csv(cls, fname: str, **kwargs) -> (list, dict):
        """
        """
        api_lib.kwget(kwargs, {
            'cumulative': False,
        })

        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'

        with open(fname, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)
            table = {header[i] : i for i in range(len(header))}

            x = []
            y = {cause: [] for cause in API.CAUSE_KEYS}
            
            for row in reader:
                x.append(datetime.date.fromisoformat(row[table['date']]))
                for cause in API.CAUSE_KEYS:
                    y[cause].append(int(row[table[cause]]))
                        
            for cause in API.CAUSE_KEYS:
                ## Adjust size
                while len(y[cause]) < len(x):
                    y[cause].append(0)
                while len(y[cause]) > len(x):
                    y[cause].pop(-1)
                if kwargs['cumulative']:
                    y[cause] = np.cumsum(y[cause], dtype=np.float64)
                else:
                    y[cause] = np.array(y[cause], dtype=np.float64)
        return x, y

    @classmethod
    def parse_res(cls, res: list, **kwargs) -> (list, dict):
        """
        """
        api_lib.kwget(kwargs, {
            'cumulative': False,
        })

        x = []
        y = {cause: [] for cause in API.CAUSE_KEYS}

        for data in res:
            x.append(data['date'])
            for cause in API.CAUSE_KEYS:
                y[cause].append(data[cause])
        
        for cause in API.CAUSE_KEYS:
            ## Adjust size
                while len(y[cause]) < len(x):
                    y[cause].append(0)
                while len(y[cause]) > len(x):
                    y[cause].pop(-1)
                if kwargs['cumulative']:
                    y[cause] = np.cumsum(y[cause], dtype=np.float64)
                else:
                    y[cause] = np.array(y[cause], dtype=np.float64)
        return x, y