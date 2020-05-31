import api
import argparse

def main(date=all, city=all, state=all, gender=all, age=True, places=all):
    ## Define os parâmetros da busca
    api.API(
        date=date, ## busca para todas as datas de 2019-01-01 até hoje
        city=city, ## busca para todas as cidades no estados especificados abaixo
        state=state, ## busca para todos os estados
        gender=gender, ## busca para cada sexo
        age=age,  ## busca por faixa etária
        places=places, ## busca dados para cada local possível
    ).get() ## coleta os dados e salva em csv

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='API de busca para dados dos cartórios.')
    parser.add_argument('--gender', type=str, dest='gender', help='gender options', default=all, choices=['M', 'F'])

    args = parser.parse_args()

    main(gender=args.gender)