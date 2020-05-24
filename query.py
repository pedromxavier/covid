import api
import datetime

x = api.API()

x.login()

query = api.APIQuery(
    start_date=datetime.date(2019, 1, 1),
    end_date=datetime.date(2020, 5, 23),
    state='RJ',
    city_id='4646',
    chart='chart5',
    places=list(x.PLACES)
    )

results = api.APIResults(
    date=datetime.date(2020, 5, 23),
    state='RJ',
    city='Rio de Janeiro',
    place="&".join(x.PLACES)
    )

request = api.APIRequest(x.API_URL, query, results, headers=x.REQUEST_HEADERS)
print(request.request.full_url)
ans = x.make_request(request)

with open(f'results-TODOS.json', 'w') as file:
    file.write(ans.read().decode())
