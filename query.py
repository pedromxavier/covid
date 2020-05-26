import api
client = api.API(date=all, city=all, state='RJ', gender=all, age=True, cache='results-cache')
results = client.get()
