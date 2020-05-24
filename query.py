import api
import datetime

x = api.API()

x.login()

chart=''

requests = []

for place in x.PLACES:

    query = api.APIQuery(
        start_date=datetime.date(2019, 1, 1),
        end_date=datetime.date(2020, 5, 23),
        state='RJ',
        city_id=x.city_id('RJ', 'Rio de Janeiro'),
        chart=chart
        places=[place]
        )

    results = api.APIResults(
        date=datetime.date(2020, 5, 23),
        state='RJ',
        city='Rio de Janeiro',
        place=place
        )

    request = api.APIRequest(x.API_URL, query, results, headers=x.REQUEST_HEADERS)
    print(request.request.full_url)
    ans = x.make_request(request)

    with open(f'results-{place}-teste.json', 'w') as file:
        file.write(ans.read().decode())
