import api
import api_io
import datetime

import sys
import os

def main(part: int=1, total: int=1):
    if part == 1 and total == 1:
        date = all
    elif 1 <= part <= total:
        DELTA = datetime.timedelta(days=((api.TODAY - api.BEGIN) / total).days)
        start_date = api.BEGIN + part * DELTA
        if part < total:
            end_date = start_date + DELTA
        else:
            end_date = api.TODAY
        date = (start_date, end_date)
    else:
        raise ValueError()

    ## Define os parâmetros da busca
    client = api.API(
        date=date, ## busca para todas as datas de 2019-01-01 até hoje
        city=all, ## busca para todas as cidades no estados especificados abaixo
        state=all, ## busca para todos os estados
        gender=all, ## busca para cada sexo
        age=True,  ## busca por faixa etária
        places=all, ## busca dados para cada local possível
        )

    ## Resultados
    results = client.get()

    print('resultados rápidos! (brincadeira, é um gerador)')

    print('Escrevendo csv, o monstro desperta')
    api_io.APIIO.to_csv(f'complete-results-{part}-{total}', results)
    print('FIM')

if __name__ == '__main__':
    part = int(sys.argv[1])
    total = int(sys.argv[2])
    with open(os.devnull, 'w') as devnull:
        sys_stdout = sys.stdout
        sys.stdout = devnull
        try:
            main(part, total)
        finally:
            sys.stdout = sys_stdout