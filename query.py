import api
client = api.API(
    date=all, ## busca todas as datas de 2019-01-01 até hoje
    city=all, ## busca todas as cidades no estado abaixo
    state='RJ', 
    gender=all, ## busca para cada sexo
    age=True,  ## busca por faixa etária
    places='DOMICILIO', ## local
    cache='results-domicilio-cache' ## IMPORTANTE. Arquivo para cache dos resultados
    )
client_results = client.get()

results = []
for res in client_results:
    results.extend(res.results.results)

import api_io

api_io.APIIO.to_csv('results-RJ-DOMICILIO', results)