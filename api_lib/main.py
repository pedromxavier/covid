from urllib.request import urlopen
from urllib.parse import urlencode, urljoin
from urllib.error import HTTPError, URLError
from time import perf_counter as clock
from time import sleep
from functools import wraps
import warnings
import hashlib
import datetime
import json
import re
import csv

def kwget(key, kwargs: dict, default=None):
    try:
        return kwargs[key]
    except KeyError:
        return default

def check_kwargs(keys:set, kwargs: dict):
    for key in kwargs:
        if key not in keys:
            raise ValueError(f'Parâmetro inválido: `{key}`. As opções válidas são {" - ".join(keys)}')

def arange(start, stop, step):
    while start <= stop:
        yield start
        start += step

def fetch_data(api_url: str, data: dict=None) -> (object):
    """ fetch_data(api_url: str, data: dict=None) -> (object)
        Returns an object decoded from a json object string.
    """
    url = encode_url(api_url, data)
    try:
        return get_request(url)
    except HTTPError:
        raise RuntimeError(f"500: Internal Server Error\n@ GET {url}")
    except URLError:
        raise RuntimeError("Desconectado da Internet. Operação Cancelada.")

def fetch_data_hash(api_url: str, data: dict=None) -> (object, bytes):
    """ fetch_data(api_url: str, data: dict=None) -> (object, bytes)
        Returns an object decoded from a json object string and its hash.
    """
    url = encode_url(api_url, data)
    try:
        return get_request_hash(url)
    except HTTPError:
        raise RuntimeError(f"500: Internal Server Error\n@ GET {url}")
    except URLError:
        raise RuntimeError("Desconectado da Internet. Operação Cancelada.")

def get_request(url: str) -> (object):
    ## Read answer from API
    ans = urlopen(url).read()
    ## Decode JSON into Python dict
    return json.loads(ans.decode('utf-8'))

def get_request_hash(url: str) -> (object, bytes):
    ## Read answer from API
    ans = urlopen(url).read()
    ## Decode JSON into Python dict
    obj = json.loads(ans.decode('utf-8'))
    ## Get hash from `ans` bytes
    hsh = hashlib.sha256(ans).digest()
    return obj, hsh

def encode_url(url: str, data: dict=None) -> str:
    if data is None:
        return url
    else:
        return urljoin(url, f"?{urlencode(data, True)}")

CITIES_URL = r'https://transparencia.registrocivil.org.br/api/cities'
CITIES_CSV = r'data/cidades.csv'
CITIES_HEADER = ['uf', 'name', 'id']
CITIES_HASH = r'data/cidades.hash'

## Cities
def fetch_cities() -> (list, bytes):
    ans_data, ans_hash = fetch_data_hash(CITIES_URL)
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
            state, city_name, city_id = row
            ascii_city_name = ascii_decode(city_name)
            if state in states:
                states[state].append(ascii_city_name)
            else:
                states[state] = [ascii_city_name]
            id_table[(state, ascii_city_name)] = city_id
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
    if type(city) is str and re.match(r'^[a-zA-zà-ÿÀ-ÿ ]+\-[A-Z]{2}$', city):
        return city.split('-')
    else:
        raise ValueError(f'Especificação de cidade inválida: {city}.\nO formato correto é `Nome da Cidade-UF`')

def time(callback):
    @wraps(callback)
    def new_callback(*args, **kwargs):
        t = clock()
        x = callback(*args, **kwargs)
        t = clock() - t
        print(f"Tempo: {t:.2f}s")
        return x
    return new_callback

ASCII_DECODE = {
    'à': 'a', 'á': 'a', 'À': 'A', 'Á': 'A', 'ã': 'a', 'Ã': 'A', 'â': 'A', 'Â': 'A',
    'é': 'e', 'É': 'E', 'ê': 'e', 'Ê': 'E',
    'í': 'i', 'Í': 'I',
    'ó': 'o', 'Ó': 'O', 'õ': 'o', 'Õ': 'O', 'ô': 'o', 'Ô': 'O',
    'ú': 'u', 'Ú': 'U',
    'ç': 'c', 'Ç': 'C',
}

ASCII_PATTERN = '|'.join(re.escape(key) for key in ASCII_DECODE)

ASCII_REGEX = re.compile(ASCII_PATTERN, flags=re.IGNORECASE)

def ascii_decode(s: str):
    return ASCII_REGEX.sub(lambda match: ASCII_DECODE.get(match.group(0)), s)