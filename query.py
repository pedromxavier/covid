import api
client = api.API(
    date=all, ## busca para todas as datas de 2019-01-01 até hoje
    city=all, ## busca para todas as cidades no estados especificados abaixo
    state=all, ## busca para todos os estados
    gender=all, ## busca para cada sexo
    age=True,  ## busca por faixa etária
    places=all, ## busca dados para cada local possível
    cache='complete-cache' ## IMPORTANTE. Arquivo para cache dos resultados
    )
results = client.get()

import api_io

api_io.APIIO.to_csv('complete-results', results)