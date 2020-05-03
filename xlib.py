from urllib.request import urlopen
from urllib.parse import urlencode, urljoin
from urllib.error import HTTPError
from time import perf_counter as clock
from functools import wraps
import warnings
import hashlib
import datetime
import json
import re
import csv

def arange(start, stop, step):
    while start <= stop:
        yield start
        start += step

def fetch_data(url: str, data: dict=None) -> (object, bytes):
    """ fetch_data(url: str, data: dict=None) -> (dict, bytes)
        Returns an object decoded from a json object string and its hash.
    """
    if data is None:
        url = url
    else:
        url = urljoin(url, f"?{urlencode(data)}")
    try:
        ## Read answer from API
        ans = urlopen(url).read()
        ## Decode JSON into Python dict
        obj = json.loads(ans.decode('utf-8'))
        ## Get hash from `ans` bytes
        hsh = hashlib.sha256(ans).digest()
    except HTTPError as error:
        print(f'GET {url}')
        print(error)
        raise Exception
    return obj, hsh

CITIES_URL = r'https://transparencia.registrocivil.org.br/api/cities'
CITIES_CSV = r'data/cidades.csv'
CITIES_HEADER = ['uf', 'name', 'id']
CITIES_HASH = r'data/cidades.hash'

## Cities
def fetch_cities() -> (list, bytes):
    ans_data, ans_hash = fetch_data(CITIES_URL)
    return ans_data['cities'], ans_hash

def dump_cities(ans_data: list, ans_hash: bytes) -> None:
    with open(CITIES_CSV, 'w') as file:
        writer = csv.DictWriter(file, fieldnames=CITIES_HEADER)
        for row in ans_data:
            writer.writerow(row)
    with open(CITIES_HASH, 'wb') as file:
        file.write(ans_hash)

def load_cities() -> (dict, dict):
    states = {}
    id_table = {}
    with open(CITIES_CSV, encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            state, city, city_id = row
            if state in states:
                states[state].append(city)
            else:
                states[state] = [city]
            id_table[(state, city)] = city_id
    return states, id_table

def update_cities():
    ans_data, ans_hash = fetch_cities()
    try:
        with open(CITIES_HASH, 'rb') as file:
            if ans_hash != file.read():
                raise FileNotFoundError
            else:
                print('Sem atualizações disponíveis.')
    except FileNotFoundError:
        dump_cities(ans_data, ans_hash)
        print('Lista de cidades atualizada.')

def get_date(date: object):
    if type(date) is datetime.date:
        return date
    elif type(date) is str:
        return datetime.date.fromisoformat(date)
    else:
        raise TypeError(f'Especificação de data inválida: `{date}`')

def get_city(city: object):
    if type(city) is str and re.match(r'^[ a-zA-Z]+\-[A-Z]{2}$', city):
        return city.split('-')
    else:
        raise ValueError('Especificação de cidade inválida: {city}.\nO formato correto é `Nome da Cidade-UF`')

def time(callback):
    @wraps(callback)
    def new_callback(*args, **kwargs):
        t = clock()
        x = callback(*args, **kwargs)
        t = clock() - t
        print(f"Tempo: {t:.2f}s")
        return x
    return new_callback

        
