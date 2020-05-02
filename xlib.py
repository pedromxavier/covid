import csv

def arange(start, stop, step):
    while start <= stop:
        yield start
        start += step

def load_cities(fname: str) -> list:
    cities = {}
    with open(fname) as file:
        reader = csv.reader(file)
        for row in reader:
            state, city = row
            if state in cities:
                cities[state].append(city)
            else:
                cities[state] = [city]
    return cities
    