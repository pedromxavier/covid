import api

def main():
    ## Define os parâmetros da busca
    api.API(
        date=('2020-05-01', api.TODAY), ## busca para todas as datas de 2019-01-01 até hoje
        city='Rio de Janeiro', ## busca para todas as cidades no estados especificados abaixo
        state='RJ', ## busca para todos os estados
        gender=all, ## busca para cada sexo
        age=True,  ## busca por faixa etária
        places=all, ## busca dados para cada local possível
    ).get() ## coleta os dados e salva em csv

if __name__ == '__main__':
    main()