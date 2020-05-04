import matplotlib.pyplot as plt
import csv
import xlib
import numpy as np
import datetime
from api import API

class Plotter:

    causes = (
                'COVID',
                'SRAG',
                'PNEUMONIA',
                'INSUFICIENCIA_RESPIRATORIA',
                'SEPTICEMIA',
                'INDETERMINADA',
                'OUTRAS'
            )

    CAUSES = [f'{cause}_{year}' for cause in causes for year in ('2020','2019') if f'{cause}_{year}' != 'COVID_2019']

    @classmethod
    def compare_cities(cls, *cities, **kwargs):
        ...

    @classmethod
    def compare_causes(cls, *causes, **kwargs):
        ...

    @classmethod
    def from_csv(cls, fname: str):
        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'

        _, ax = plt.subplots(1, 1)

        with open(fname, 'r') as file:
            reader = csv.reader(file)
            header = next(reader)
            table = {header[i] : i for i in range(len(header))}
            x = []
            y = {}
            row = next(reader)
            for cause in cls.CAUSES:
                y[cause] = [int(row[table[cause]])]
            
            for row in reader:
                x.append(datetime.date.fromisoformat(row[table['date']]))
                for cause in cls.CAUSES:
                    y[cause].append(int(row[table[cause]]) + y[cause][-1])
                        
            for cause in cls.CAUSES:
                while len(y[cause]) < len(x):
                    y[cause].append(0)
                while len(y[cause]) > len(x):
                    y[cause].pop(-1)
                yy = np.array(y[cause], dtype=np.float64)
                yy[yy == 0] = np.nan
                ax.plot(x, yy, label=cause)

        plt.legend()  
        plt.xticks(rotation=70)
        plt.show()
