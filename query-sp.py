import api
import os
import argparse

def main(date=all, city=all, state=all, gender=all, age=True, places=all, block_size=1024, threads=os.cpu_count()):
    ## Define os parâmetros da busca
    api.API(
        date=date, ## busca para todas as datas de 2019-01-01 até hoje
        city=city,## busca para todas as cidades no estados especificados abaixo
        state='SP', ## busca para todos os estados
        gender=gender, ## busca para cada sexo
        age=age,  ## busca por faixa etária
        places=places, ## busca dados para cada local possível
        block_size=block_size,
        threads=threads,
        output='resultados-SP'
    ).get() ## coleta os dados e salva em csv

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='API de busca para dados dos cartórios.')
    parser.add_argument('--gender', type=str, dest='gender', help='gender options', default=all, choices=['M', 'F'])
    parser.add_argument('--threads', type=int, dest='threads', help='number of threads', default=os.cpu_count())
    parser.add_argument('--block-size', type=int, dest='block_size', default=1024)

    args = parser.parse_args()

    main(gender=args.gender, threads=args.threads, block_size=args.block_size)