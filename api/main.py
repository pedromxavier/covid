#!/usr/env/python3
## Standard Library
from http.cookiejar import CookieJar
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError
from functools import wraps
import sys
import os
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

## Jupyter Issues
IN_JUPYTER = 'ipykernel' in sys.modules
if IN_JUPYTER:
    try:
        import nest_asyncio
        nest_asyncio.apply()
        JUPYTER_ASYNC_LIB = True
    except ImportError:
        JUPYTER_ASYNC_LIB = False
        warnings.warn('Falha ao importar bilioteca `nest_asyncio`. Requisições assíncronas indisponíveis no Jupyter Notebook.', category=ImportWarning, stacklevel=2)
ASYNC_MODE = ASYNC_LIB and (not IN_JUPYTER or JUPYTER_ASYNC_LIB)

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

## Gender
GENDERS = {"M", "F"}

## City/State table
STATES, ID_TABLE = api_lib.load_cities()

class APIResult(object):
    """
    """
    __slots__ = ('date', 'state', 'city', 'region', 'gender', 'chart', 'place', 'age') + CAUSES

    __defaults = {
        'date': TODAY,
        'state': None,
        'city': None,
        'region': None,
        'gender': None,
        'chart': None,
        'place': None,
        'age': False,
        **{cause: 0 for cause in CAUSES},
        }

    def __init__(self, **kwargs):
        for name in self.__slots__:
            if name in kwargs:
                setattr(self, name, kwargs[name])
            else:
                setattr(self, name, self.__defaults[name])

    def __add__(self, other):
        return APIResult(**{**self, **{k:self[k]+other[k] for k in CAUSES}})

    def __getitem__(self, name: str):
        return getattr(self, name)

    def keys(self):
        return [key for key in self.__slots__ if key != 'success']

class APIResults(object):
    """
    """
    __slots__ = ('date', 'state', 'city', 'region', 'gender', 'chart', 'places', 'age') + CAUSES + ('results', 'success')

    __defaults = {
        'date': TODAY,
        'state': None,
        'city': None,
        'region': None,
        'gender': None,
        'chart': None,
        'places': None,
        'age': False,
        **{cause: 0 for cause in CAUSES},
        }

    AGE_TABLE = {
        '< 9': 0,
        '10 - 19': 10,
        '20 - 29': 20,
        '30 - 39': 30,
        '40 - 49': 40,
        '50 - 59': 50,
        '60 - 69': 60,
        '70 - 79': 70,
        '80 - 89': 80,
        '90 - 99': 90,
        '> 100': 100,
        'N/I': None
        }

    def __init__(self, **kwargs):
        for name in self.__slots__:
            if name in ('results', 'success'): continue
            if name in kwargs:
                setattr(self, name, kwargs[name])
            else:
                setattr(self, name, self.__defaults[name])
        self.results = []
        self.success = False

    def commit(self, response_data: dict):
        chart = response_data['chart']
        #pylint: disable=no-member
        self.chart = API.get_chart(self.gender, self.age)
        if self.chart in {'chart2', 'chart3'}:
            data = {'place': '&'.join(self.places)}
            for age in chart:
                data['age'] = self.AGE_TABLE[age]
                for year in chart[age]:
                    try:
                        data['date'] = datetime.date(int(year), self.date.month, self.date.day)
                    except ValueError:
                        continue
                    for cause in chart[age][year]:
                        data[cause] = chart[age][year][cause]
                    self.results.append(APIResult(**{**self, **data}))
        elif self.chart == 'chart5':
            data = {'place': '&'.join(self.places)}
            for date in chart:
                data['date'] = datetime.date.fromisoformat(date)
                for cause in chart[date]:
                    data[cause] = chart[date][cause][0]['total']
                self.results.append(APIResult(**{**self, **data}))
        else:
            raise ValueError(f'Não sei lidar com o chart nº {self.chart}')
        self.success = True
            
    def __getitem__(self, name: str):
        return getattr(self, name)

    def keys(self):
        return [key for key in self.__slots__ if key not in ('results', 'success')]

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

    __slots__ = ('start_date', 'end_date', 'state', 'city_id', 'gender', 'chart', 'places')

    __lists__ = ('places',)

    def __init__(self, *, start_date=None,
                          end_date=None,
                          state=None,
                          city_id=None,
                          gender=None,
                          age=False,
                          places=None,
                          **extra_kwargs):
        for name in self.__slots__:
            if name == 'chart': continue
            setattr(self, name, eval(name, {}, locals()))

        self.chart = API.get_chart(gender, age)

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

class APIRequestError(Exception):

    def __init__(self, msg: str):
        Exception.__init__(self, msg)
        self.msg = msg
    
    def __str__(self):
        return self.msg

class APIRequest(object):

    __slots__ = ('url', 'query', 'results', 'request')

    def __init__(self, url: str, query: APIQuery, results: APIResults, **options):
        self.url = url
        self.query = query
        self.results = results
        self.request = Request(f"{self.url}?{urlencode(dict(self.query), True)}", **options)
    
    def __repr__(self):
        return f"APIRequest[{self.success}]"
    
    def get(self):
        try:
            response = urlopen(self.request)
            raw_text = response.read()
            self.commit(json.loads(raw_text.decode('utf-8')))
        except HTTPError as error:
            API.log(f'Code {error.code} in GET {self.request.full_url}\nError: {error}')
        except Exception as error:
            API.log(f'Code {200} in GET {self.request.full_url}\nError: {error}')
        finally:
            response.close()

    async def async_get(self, session):
        async with session.get(self.request.full_url) as response:
            try:
                self.commit(await response.json())
            except Exception as error:
                API.log(f'Code {response.status} in GET {self.request.full_url}\nError: {error}')
            finally:
                response.close()

    def commit(self, response_data: dict):
        self.results.commit(response_data)
    
    @property
    def success(self):
        return self.results.success

class APIRequestQueue:

    __slots__ = ('url', 'age', 'dates', 'cities', 'places', 'genders', 'options', 'iterator')

    def __init__(self, url=None, age=None, dates=None, cities=None, places=None, genders=None, **options):
        self.url = url
        
        self.age = age
        self.dates = dates
        self.cities = cities
        self.places = places
        self.genders = genders

        self.options = options

        self.iterator = iter(self)

    def __next__(self):
        return next(self.iterator)

    def __iter__(self):
        data = {'age': self.age}
        for start_date, end_date in self.dates:
            data['start_date'] = start_date
            data['end_date'] = end_date
            data['date'] = end_date
            for state, city, city_id in self.cities:
                data['state'] = state
                data['city'] = city
                data['city_id'] = city_id
                for place_list in self.places:
                    data['places'] = place_list
                    for gender in self.genders:
                        data['gender'] = gender
                        yield APIRequest(self.url, APIQuery(**data), APIResults(**data), **self.options)

    @property
    def total(self):
        return len(self.dates) * len(self.cities) * len(self.places) * len(self.genders) 

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
    STATES, ID_TABLE = STATES, ID_TABLE

    ## Years
    YEARS = YEARS

    ## Possible places
    PLACES = PLACES

    ## Possible genders
    GENDERS = GENDERS

    ## Default block size
    BLOCK_SIZE = 1024

    ## Logging
    LOG_FNAME = 'api.log'
    log_file = open(LOG_FNAME, 'w') ## Creates file if it does not exists, erases previous if exists
    log_file.close()
    log_lock = threading.Lock()

    ## Request
    @property
    def request_headers(self):
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
            'cumulative' : True, ##
            'date' : None,
            'state' : None,
            'city' : None,
            'places' : None,
            'gender' : None,
            'age': False,
            'cache': False,
            'sync' : not ASYNC_MODE,
            'block': self.BLOCK_SIZE
        })
        self.kwargs = kwargs

        ## Request block size
        self.block_size = self.kwargs['block']

        ## Cache results
        self.cache = self.kwargs_cache(**kwargs)

        ## Cookies
        self.cookie_jar = CookieJar()

        ## Request Queue
        self.requests = self.get_request_queue(**self.kwargs)

        ## Results
        self.results = []

        ## Asynchronous Requests
        if not self.kwargs['sync']:
            self.loop = asyncio.get_event_loop()
            self.semaphore = asyncio.Semaphore(self.BLOCK_SIZE)
            self.timeout = aiohttp.ClientTimeout(total=(self.total * 5))

    @classmethod
    def log(cls, s: str):
        header = f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}]"
        with cls.log_lock: 
            with open(cls.LOG_FNAME, 'a') as cls.log_file:
                print(header, s, file=cls.log_file)

    def login(self):
        """ Realiza o login na plataforma dos cartórios.
            Isso é feito extraindo o 'XSRF-Token' dos Cookies e adicionando aos headers.
        """
        try:
            ## Make request to page
            ans, req = api_lib.request(self.LOGIN_URL, headers=self.LOGIN_HEADERS)
            
            ## Extract Token from Cookies
            self.cookie_jar.extract_cookies(ans, req)
            
            ## Gets first occurence of the Token in the cookie jar
            self.XSRF_TOKEN = next(cookie for cookie in self.cookie_jar if cookie.name == "XSRF-TOKEN").value
            print(f'Autenticado')
        except Exception as error:
            print(f'Falha no login')
            raise error
    
    ## Synchronous GET methods
    def sync_request(self, request: APIRequest):
        """ Dispara o request de maneira sequencial
        """
        request.get()
        next(self.progress)

    def sync_run(self, requests: list):
        """ Dispara os requests de maneira sequencial
        """
        for request in requests: self.sync_request(request)

    ## Asynchronous GET methods
    async def async_request(self, request: APIRequest, session):
        """
        """
        await request.async_get(session)
        next(self.progress)


    async def _async_run(self, requests: list):
        """ Dispara os requests de maneira assíncrona.
        """
        async with aiohttp.ClientSession(timeout=self.timeout, headers=self.request_headers) as session:
            tasks = [asyncio.ensure_future(self.async_request(request, session)) for request in requests]
            await asyncio.wait(tasks)

    def async_run(self, requests: list):
        """ Dispara os requests de maneira assíncrona.
        """
        if not ASYNC_MODE:
            raise ImportError("Falha ao obter as bibliotecas necessárias. Requisições assíncronas indisponíveis.")
        else:
            self.loop.run_until_complete(asyncio.ensure_future(self._async_run(requests)))
    
    ## -- KWARGS --
    @staticmethod
    def get_chart(gender:str, age:bool):
        if gender is None and age is False:
            return 'chart5'
        else:
            if gender == 'M':
                return 'chart2'
            elif gender == 'F':
                return 'chart3'
            else: ## age is True and gender is None
                raise RuntimeError('Isso não deve acontecer. JAMAIS!')

    def kwargs_places(self, **kwargs) -> list:
        """
        """
        places_kwarg = kwargs['places']

        if type(places_kwarg) is str:
            if places_kwarg not in self.PLACES:
                raise ValueError(f'Local inválido `{places_kwarg}`.\nAs opções válidas são: {self.PLACES}')
            else:
                return [[places_kwarg]]
        if type(places_kwarg) is set:
            places = []
            for place in places_kwarg:
                place = place.upper()
                if place not in self.PLACES:
                    raise ValueError(f'Local inválido `{place}`.\nAs opções válidas são: {self.PLACES}')
                else:
                    places.append(place)
            else:
                return [places]
        elif places_kwarg is all:
            return [[place] for place in self.PLACES]
        elif places_kwarg is None:
            return [list(self.PLACES)]
        else:
            raise TypeError('Especificação de local deve ser um conjunto (`set`) ou `all`.')

    def kwargs_date(self, **kwargs) -> list:
        """
        """
        date_kwarg = kwargs['date']
        cumulative = kwargs['cumulative']

        if type(date_kwarg) is tuple and len(date_kwarg) == 2:
            start, stop = map(api_lib.get_date, date_kwarg)
        elif date_kwarg is all:
            start = BEGIN
            stop = TODAY
        elif date_kwarg is None:
            start = stop = TODAY
        elif type(date_kwarg) is datetime.date or type(date_kwarg) is str:
            start = stop = api_lib.get_date(date_kwarg)
        else:
            raise TypeError(f'Especificação de data inválida: {date_kwarg}')

        step = ONE_DAY

        if cumulative:
            return [(start, date) for date in api_lib.arange(start, stop, step)]
        else:
            return [(date, date) for date in api_lib.arange(start, stop, step)]
    
    def kwargs_city_state(self, **kwargs) -> list:
        """
        """
        ## necessary kwargs
        city_kwarg = kwargs['city']
        state_kwarg = kwargs['state']

        state_sufix = None

        if state_kwarg is all:
            states = self.STATES
        elif state_kwarg is None and city_kwarg is None:
            return [('Todos', None, None)] # (state, city, city_id)
        elif type(state_kwarg) is str and state_kwarg in self.STATES:
            state_sufix = state_kwarg
            states = [state_kwarg]
        elif type(state_kwarg) is set and all(s in self.STATES for s in state_kwarg):
            states = list(state_kwarg)
        else:
            raise ValueError(f'Especificação de estado inválida: {state_kwarg}')

        if type(city_kwarg) is str:
            if state_sufix is not None:
                city_kwarg = f'{city_kwarg}-{state_sufix}'
            city, state = api_lib.get_city(city_kwarg)
            return [(state, city, self.city_id(state, city))]
        elif city_kwarg is all:
            return [(state, city, self.city_id(state, city)) for state in states for city in self.STATES[state]]
        elif type(city_kwarg) is set:
            return sum([self.kwargs_city_state(city=city, state=state_sufix) for city in city_kwarg], [])
        else:
            raise ValueError(f'Especificação de cidade inválida: {city}.\nO formato correto é `Nome da Cidade-UF`')
    
    def kwargs_cache(self, **kwargs):
        cache_kwarg = kwargs['cache']
        if type(cache_kwarg) is bool and cache_kwarg is False:
            return cache_kwarg
        elif type(cache_kwarg) is str:
            return os.path.join("cache", f"{cache_kwarg}.p")
        else:
            raise TypeError(f"Parâmetro 'cache' deve ser do tipo 'str' ou 'False'.")

    def kwargs_gender(self, **kwargs):
        gender_kwarg = kwargs['gender']
        age_kwarg = kwargs['age']
        if type(gender_kwarg) is str:
            gender = gender_kwarg.upper()
            if gender not in self.GENDERS:
                raise ValueError(f"unknown gender: {gender}")
            return [gender]
        elif gender_kwarg is all:
            return list(self.GENDERS)
        elif gender_kwarg is None:
            if age_kwarg is False:
                return [None]
            else:
                return list(self.GENDERS)
        else:
            raise TypeError(f"invalid gender type {type(gender_kwarg)}")

    def kwargs_age(self, **kwargs):
        age_kwarg = kwargs['age']
        if type(age_kwarg) is bool:
            return age_kwarg
        else:
            raise TypeError('age must be bool')
    
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

    def get_request_queue(self, **kwargs) -> list:
        """
        """
        ## data lists
        age = self.kwargs_age(**kwargs)
        dates = self.kwargs_date(**kwargs)
        cities = self.kwargs_city_state(**kwargs)
        places = self.kwargs_places(**kwargs)
        genders = self.kwargs_gender(**kwargs)

        return APIRequestQueue(
            url=self.API_URL,
            age=age,
            dates=dates,
            cities=cities,
            places=places,
            genders=genders,
            headers=self.request_headers
            )

    def get(self, **kwargs) -> list:
        """
        """
        api_lib.kwget(kwargs, self.kwargs)
        try:
            yield from self._get(**kwargs)
        except KeyboardInterrupt:
            self.log('Keyboard Interrupt')

    def _get(self, **kwargs) -> list:
        """
        """
        ## Login
        self.login()
        
        ## Gather
        return self._gather(**kwargs)
    
    def _gather(self, **kwargs):
        """
        """
        self.log('START API._gather')
        self.progress = api_lib.progress(self.requests.total)
        for block in self._blocks():
            while block:
                try:
                    if kwargs['sync']:
                        self.sync_run(block)
                    else:
                        self.async_run(block)
                except APIRequestError as error:
                    self.log(error)
                finally:
                    results = []
                    pending = []

                    for request in block:
                        if request.success:
                            results.extend(request.results.results)
                        else:
                            pending.append(request)
                    
                    yield from results
                    
                    block = pending

    def _blocks(self):
        """ This generator divides the pending requests into blocks
            of size `self.block_size`
        """
        ## Returns the whole batch
        if self.block_size is None:
            yield self.requests
            return

        ## Splits the batch into blocks
        for _ in range(0, self.total, self.block_size):
            yield [next(self.requests) for _ in range(self.block_size)]
    
    ## Progress Properties
    @property
    def total(self):
        try:
            return self.progress.total
        except AttributeError:
            return self._total
    
    @property
    def _total(self) -> int:
        return self.requests.total
    
    @property
    def done(self):
        try:
            return self.progress.done
        except AttributeError:
            return self._done

    @property
    def _done(self) -> int:
        return 0
    
    @property
    def rate(self) -> float:
        try:
            return self.progress.rate
        except AttributeError:
            return self._rate
    
    @property
    def _rate(self) -> float:
        return self._done / self._total
