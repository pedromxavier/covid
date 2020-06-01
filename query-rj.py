import api
import os
import argparse

def main(date=all, city=all, state=all, gender=all, age=True, places=all, threads=os.cpu_count()):
    ## Define os parâmetros da busca
    api.API(
        date=date, ## busca para todas as datas de 2019-01-01 até hoje
        city=city, ## busca para todas as cidades no estados especificados abaixo
        state='RJ', ## busca para todos os estados
        gender=gender, ## busca para cada sexo
        age=age,  ## busca por faixa etária
        places=places, ## busca dados para cada local possível
        threads=threads,
    ).get() ## coleta os dados e salva em csv

if __name__ == '__main__':
    main()