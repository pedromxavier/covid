#!/usr/env/python3
## Standard Library
from http.cookiejar import CookieJar
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError
from functools import wraps
import asyncio
import csv
import json
import time
import datetime
import itertools
import threading
import warnings
import pickle

## Third-Party
try:
    import aiohttp
    ASYNC_LIB = True
except ImportError:
    ASYNC_LIB = False
    warnings.warn('Falha ao importar bilioteca `aiohttp`. Requisições assíncronas indisponíveis.', category=ImportWarning, stacklevel=2)
try:
    import nest_asyncio
    nest_asyncio.apply()
    JUPYTER_ASYNC_LIB = True
except ImportError:
    JUPYTER_ASYNC_LIB = False
    warnings.warn('Falha ao importar bilioteca `nest_asyncio`. Requisições assíncronas indisponíveis no Jupyter Notebook.', category=ImportWarning, stacklevel=2)

## Local
import api_lib
import api_db

## Constants
## Possible causes
CAUSES = (
    'COVID',
    'SRAG',
    'PNEUMONIA',
    'INSUFICIENCIA_RESPIRATORIA',
    'SEPTICEMIA',
    'INDETERMINADA',
    'OUTRAS'
)

## Possible places
PLACES = {'HOSPITAL', 'DOMICILIO', 'VIA_PUBLICA', 'AMBULANCIA', 'OUTROS'}

## Time constants
BEGIN = datetime.date(2020, 1, 1)
TODAY = datetime.date.today()
ONE_DAY = datetime.timedelta(days=1)
YEARS = ('2019', '2020')

GENDERS = ["M", "F"]

class APIResults(object):
    """
    """
    __slots__ = ('date', 'state', 'city', 'region', 'gender', 'chart', 'place', 'age') + CAUSES + ('success',)

    __defaults = {
        'date': TODAY,
        'state': None,
        'city': None,
        'region': None,
        'gender': None,
        'chart': None,
        'place': None,
        'age': None,
        **{cause: 0 for cause in CAUSES},
        'success': False
        }

    def __init__(self, **kwargs):
        for name in self.__slots__:
            if name in kwargs:
                setattr(self, name, kwargs[name])
            else:
                setattr(self, name, self.__defaults[name])

    def commit(self, response_data: dict):
        #pylint: disable=no-member
        if self.chart == 3:
            ...
        else:
            raise ValueError(f'Não sei lidar com o chart nº {self.chart}')
        self.success = True
            
    def __getitem__(self, name: str):
        return getattr(self, name)

    def keys(self):
        return [key for key in self.__slots__ if key != 'success']

class APIQuery(object):
    """
        Example Query:
        json {
            "start_date":"2020-01-01",
            "end_date":"2020-05-22",
            "state":"RJ",
            "city_id":"4646",
            "chart":"chart3",
            "gender":"F",
            "places[]":["HOSPITAL","DOMICILIO","VIA_PUBLICA","OUTROS"]
        }
    """

    __slots__ = ('start_date', 'end_date', 'state', 'city_id', 'chart', 'gender', 'places')

    __lists__ = ('places',)

    def __init__(self, *, start_date=None,
                          end_date=None,
                          state=None,
                          city_id=None,
                          chart=None,
                          gender=None,
                          places=None,
                          **extra_kwargs):
        for name in self.__slots__:
            setattr(self, name, eval(name, {}, locals()))

    def __iter__(self):
        return iter(dict(self))

    def __getitem__(self, name: str):
        try:
            if name.endswith('[]'):
                return getattr(self, name[:-2])
            else:
                return getattr(self, name)
        except AttributeError:
            raise KeyError(name)
            
    def keys(self):
        return [(f'{key}[]' if (key in self.__lists__) else key) for key in self.__slots__ if self[key] is not None]

class APIRequest(object):

    __slots__ = ('url', 'query', 'results', 'request')

    def __init__(self, url: str, query: APIQuery, results: APIResults, **options):
        self.url = url
        self.query = dict(query)
        self.results = results
        self.request = Request(f"{self.url}?{urlencode(self.query, True)}", **options)
    
    def __repr__(self):
        return f"APIRequest[{self.success}]"
    
    def get(self):
        return urlopen(self.request)

    def commit(self, response_data: dict):
        self.results.commit(response_data)
    
    @property
    def success(self):
        return self.results.success

class API:

    ## API constants
    API_URL = r'https://transparencia.registrocivil.org.br/api/covid-covid-registral'

    ## Login constants
    LOGIN_URL = r"https://transparencia.registrocivil.org.br/registral-covid"
    XSRF_TOKEN = ""
    LOGIN_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }

    ## City information
    UPDATED_CITIES = False
    STATES, ID_TABLE = api_lib.load_cities()

    ## Years
    YEARS = YEARS

    ## Possible places
    PLACES = PLACES

    ## Log
    LOG_FNAME = 'api.log'

    ## Default block size
    BLOCK_SIZE = 1024

    ## Request
    @property
    def REQUEST_HEADERS(self):
        return {
            "X-XSRF-TOKEN" : self.XSRF_TOKEN,
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }

    def __init__(self, **kwargs):
        """
        """
        ## default keyword arguments
        api_lib.kwget(kwargs, {
            'cumulative' : True,
            'date' : None,
            'state' : None,
            'city' : None,
            'places' : all,
            'cache': False,
            'sync' : not ASYNC_LIB,
            'block': self.BLOCK_SIZE
        })
        self.kwargs = kwargs

        ## Request block size
        self.block_size = self.kwargs['block']

        ## Logging
        self.log_file = open(self.LOG_FNAME, 'a') ## Creates file if it does not exists
        self.log_file.close()
        self.log_lock = threading.Lock()

        ## Cache results
        self.cache = self.__get_cache_kwarg(kwargs['cache'])

        ## Cookies
        self.cookie_jar = CookieJar()

        ## Request Queue
        self.requests = [] #list(self.build_requests())

        ## Results
        self.results = []

        ## Asynchronous Requests
        if not self.kwargs['sync']:
            self.loop = asyncio.get_event_loop()
            self.semaphore = asyncio.Semaphore(self.BLOCK_SIZE)
            self.timeout = aiohttp.ClientTimeout(total=(self.total * 5))

    def log(self, s: str):
        header = f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}]"
        with self.log_lock: print(header, s, file=self.log_file)

    def login(self):
        try:
            ans, req = api_lib.request(self.LOGIN_URL, headers=self.LOGIN_HEADERS)
            self.cookie_jar.extract_cookies(ans, req)
            self.XSRF_TOKEN = next(c for c in self.cookie_jar if c.name == "XSRF-TOKEN").value
            print(f'Autenticado')
        except:
            print(f'Falha no login')
            raise
    
    def extract_chart(self, response_data: dict):
        chart = response_data['chart']
        return {f'{cause}_{year}': chart[year][cause] for year in chart for cause in chart[year]}

    ## -- SYNC --
    def __get_request(self, req: APIRequest):
        try:
            response_data = self.extract_chart(api_lib.get_request_json(req.url, headers=self.REQUEST_HEADERS))
            req.commit(response_data)
        except HTTPError as http_error:
            self.log(f'Code {http_error.code} in GET {req.url}\nError: {http_error}\n')
        except Exception as error:
            self.log(f'Code {200} in GET {req.url}\nError: {error}\n')
        finally:
            next(self.progress)
    
    def __get_requests(self, requests):
        """
        """
        for req in requests: self.__get_request(req)
    ## -- SYNC --

    ## -- ASYNC --
    async def __async_get_request(self, req: APIRequest, session):
        """
        """
        async with session.get(req.url) as response:
            try:
                response_data = self.extract_chart((await response.json()))
                req.commit({**req.data, **response_data})
                next(self.progress)
            except Exception as error:
                self.log(f'Code {response.status} in GET {req.url}\nError: {error}\n')
                raise error
            finally:
                response.close()
        
    async def __async_sem_request(self, req: APIRequest, session):
        """
        """
        async with self.semaphore:
            await self.__async_get_request(req, session)

    async def __async_run(self, requests: list):
        """
        """
        async with aiohttp.ClientSession(timeout=self.timeout, headers=self.REQUEST_HEADERS) as session:
            tasks = [self.__async_sem_request(req, session) for req in requests]
            await asyncio.gather(*tasks)

    def __async_get_requests(self, requests: list):
        """
        """
        try:
            self.loop.run_until_complete(self.__async_run(requests))
        except NameError:
            if not ASYNC_LIB:
                raise ImportError('Falha ao importar bilioteca `aiohttp`. Requisições assíncronas indisponíveis.')
            else:
                raise
        except RuntimeError:
            if not JUPYTER_ASYNC_LIB:
                raise ImportError('Falha ao importar bilioteca `nest_asyncio`. Requisições assíncronas indisponíveis no Jupyter Notebook.')
            else:
                raise
    ## -- ASYNC --
    
    ## -- KWARGS --
    def __get_places_kwarg(self, places_kwarg: object) -> list:
        """
        """
        if type(places_kwarg) is set:
            places = []
            for place in places_kwarg:
                place = place.upper()
                if place not in self.PLACES:
                    raise ValueError(f'Local inválido `{place}`.\nAs opções válidas são: {PLACES}')
                else:
                    places.append(place)
            else:
                return sorted(places)
        elif places_kwarg is all:
            return sorted(self.PLACES)
        else:
            raise TypeError('Especificação de local deve ser um conjunto (`set`) ou `all`.')

    def __get_date_kwarg(self, date_kwarg: object, cumulative: bool=True) -> list:
        """
        """
        if type(date_kwarg) is tuple and len(date_kwarg) == 2:
            start, stop = map(api_lib.get_date, date_kwarg)
        elif date_kwarg is all:
            start = datetime.date(2020, 1, 1)
            stop = datetime.date.today()
        elif date_kwarg is None:
            start = datetime.date.today()
            stop = datetime.date.today()
        elif type(date_kwarg) is datetime.date or type(date_kwarg) is str:
            start = stop = api_lib.get_date(date_kwarg)
        else:
            raise TypeError(f'Especificação de data inválida: {date_kwarg}')

        step = datetime.timedelta(days=1)

        if cumulative:
            return [(start, date) for date in api_lib.arange(start, stop, step)]
        else:
            return [(date, date) for date in api_lib.arange(start, stop, step)]

    def __get_state_kwarg(self, state_kwarg: object) -> list:
        """
        """
        if type(state_kwarg) is str:
            if state_kwarg in self.STATES:
                return [state_kwarg]
            else:
                raise ValueError(f'Estado não cadastrado: {state_kwarg}')
        elif state_kwarg is all:
            return list(self.STATES.keys())
        elif type(state_kwarg) is set:
            return sum([self.__get_state_kwarg(x) for x in state_kwarg], [])
        else:
            raise ValueError(f'Especificação de estado inválida: `{state_kwarg}`.')
    
    def __get_city_kwarg(self, city_kwarg: object, state_sufix: str=None) -> list:
        """
        """
        if type(city_kwarg) is str:
            if state_sufix is not None:
                city_kwarg = f'{city_kwarg}-{state_sufix}'
            city, state = api_lib.get_city(city_kwarg)
            return [(state, city, self.city_id(state, city))]
        elif city_kwarg is all:
            cities = []
            states = self.STATES
            for state in states:
                for city in states[state]:
                    cities.append((state, city, self.city_id(state, city)))
            return cities
        elif type(city_kwarg) is set:
            return sum([self.__get_city_kwarg(x, state_sufix=state_sufix) for x in city_kwarg], [])
        else:
            raise ValueError(f'Especificação de cidade inválida: {city}.\nO formato correto é `Nome da Cidade-UF`')
    
    def __get_cache_kwarg(self, cache_kwarg: str):
        if type(cache_kwarg) is bool and cache_kwarg is False:
            return cache_kwarg
        elif type(cache_kwarg) is str:
            return f"{cache_kwarg}.p"
        else:
            raise TypeError(f"Parâmetro 'cache' deve ser do tipo 'str' ou 'False'.") 
    ## -- KWARGS --

    def city_id(self, state: str, city_name: str):
        """
        """
        ascii_city_name = api_lib.ascii_decode(city_name).upper()
        try:
            return self.ID_TABLE[(state, ascii_city_name)]
        except KeyError:
            if not self.UPDATED_CITIES:
                api_lib.update_cities()
                self.STATES, self.ID_TABLE = api_lib.load_cities()
                self.UPDATED_CITIES = True
                return self.city_id(state, ascii_city_name)
            else:
                raise ValueError(f'Cidade não cadastrada: `{city_name} ({state})`.')

    def get_dates(self, **kwargs):
        return [(date, date) for date in api_lib.arange(BEGIN, TODAY, ONE_DAY)]

    def get_cities(self, **kwargs):
        if kwargs['state'] is None:
            return [(state, city, self.city_id(state, city)) for state in self.STATES for city in self.STATES[state]]
        else:
            return [(kwargs['state'], city, self.city_id(kwargs['state'], city)) for city in self.STATES[kwargs['state']]]

    def get_genders(self, **kwargs):
        return GENDERS

    def get_places(self, **kwargs):
        return PLACES

    def build_requests(self, **kwargs) -> list:
        ## data lists
        dates = self.get_dates(**kwargs)
        cities = self.get_cities(**kwargs)
        places = self.get_places(**kwargs)
        genders = self.get_genders(**kwargs)

        total = len(dates) * len(cities) * len(places) * len(genders)

        print(f'Total: {total}')

        data = {'chart': 'chart3'}
        for start_date, end_date in dates:
            data['start_date'] = start_date
            data['end_date'] = end_date
            data['date'] = end_date
            for state, city, city_id in cities:
                data['state'] = state
                data['city'] = city
                data['city_id'] = city_id
                for place in places:
                    data['places'] = [place]
                    data['place'] = place
                    for gender in genders:
                        data['gender'] = gender
                        yield APIRequest(self.API_URL, APIQuery(**data), APIResults(**data), headers=self.REQUEST_HEADERS)

    @classmethod
    def make_request(cls, request: APIRequest):
        return request.get()

    def get_all(self, **kwargs) -> list:
        attempts = 1
        while True:
            try:
                print(f'Tentativa nº {attempts}')
                results = self.get(**kwargs)
            except KeyboardInterrupt:
                print(':: Cancelado ::')
                break
            except Exception:
                pass
            finally:
                attempts += 1
        return results

    def get(self, **kwargs) -> list:
        """
        """
        api_lib.kwget(kwargs, self.kwargs)
        return self._get(**kwargs)
    
    def _get(self, **kwargs) -> list:
        """
        """
        ## Login
        self.login()
        
        ## Checks for success:
        if self._gather(**kwargs):
            return self.results
        else:
            return None

    def _gather(self, **kwargs) -> bool:
        """
        """
        self.log('START API._gather')
        self.progress = api_lib.progress(self._total, self._done)
        for block in self._blocks():
            try:
                if kwargs['sync']:
                    self.__get_requests(block)
                else:
                    self.__async_get_requests(block)
            except Exception:
                success = False
                break
        else:
            success = True

        ## results <- requests
        self.results = [req for req in self.requests]

        ## cache actual results
        self.cache_results()

        return success

    def cache_results(self):
        if self.cache:
            api_lib.pkdump(self.cache, self.results)
            print(f"Resultados salvos no arquivo de cache '{self.cache}'")

    def _blocks(self):
        """ This function divides the pending requests into
        """
        ## Collect pending requests
        requests = [req for req in self.requests if not req.success]

        ## Returns the whole batch
        if self.block_size is None:
            yield requests
        ## Splits the batch into blocks
        else:
            for i in range(0, len(requests), self.block_size):
                yield requests[i:i+self.block_size]
    
    ## Progress Properties
    @property
    def total(self):
        try:
            return self.progress.total
        except AttributeError:
            return self._total
    
    @property
    def _total(self) -> int:
        return len(self.requests)
    
    @property
    def done(self):
        try:
            return self.progress.done
        except AttributeError:
            return self._done

    @property
    def _done(self) -> int:
        return sum([req.success for req in self.requests])
    
    @property
    def rate(self) -> float:
        try:
            return self.progress.rate
        except AttributeError:
            return self._rate
    
    @property
    def _rate(self) -> float:
        return self._done / self._total



        

