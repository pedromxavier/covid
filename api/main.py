#!/usr/env/python3
## Standard Library
from http.cookiejar import CookieJar
from urllib.request import Request, urlopen
from urllib.parse import urlencode
from urllib.error import HTTPError
from functools import wraps, reduce
import ctypes
import sys
import os
import asyncio
import csv
import json
import time
import datetime
import itertools
import threading
import multiprocessing as mp
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
import api_io
from api_constants import CAUSES, STATES, ID_TABLE, YEARS, GENDERS, PLACES
from api_constants import BEGIN, TODAY, ONE_DAY
from api_constants import BLOCK_SIZE, CPU_COUNT

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

    CAUSES = CAUSES

    def __init__(self, **kwargs):
        for name in self.__slots__:
            if name in ('results', 'success'): continue
            if name in kwargs:
                setattr(self, name, kwargs[name])
            else:
                setattr(self, name, self.__defaults[name])
        self.results = []
        self.success = False

    def __iter__(self):
        return iter(self.results)

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
                    for cause in self.CAUSES:
                        if cause in chart[age][year]:
                            data[cause] = chart[age][year][cause]
                        else:
                            data[cause] = 0
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

    def __init__(self, msg: str, code: int):
        Exception.__init__(self, msg)
        self.msg = msg
        self.code = code
    
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
            API.log(error)
        except Exception as error:
            API.log(error)
        finally:
            response.close()

    async def async_get(self, session):
        async with session.get(self.request.full_url) as response:
            try:
                if response.status == 200:
                    self.commit(await response.json())
                elif response.status == 500:
                    pass
                else:
                    API.log(f'Code {response.status} in GET')
            except Exception:
                API.log(f'Code {response.status} in GET with Error')
            finally:
                response.close()

    def commit(self, response_data: dict):
        self.results.commit(response_data)
    
    @property
    def success(self):
        return self.results.success

class APIRequestQueue:

    __slots__ = ('url', 'age', 'dates', 'cities', 'places', 'genders', 'shape', 'total', 'options')

    def __init__(self, url=None, age=None, dates=None, cities=None, places=None, genders=None, **options):
        self.url = url
        
        ## query param sources
        self.age = age
        self.dates = dates
        self.cities = cities
        self.places = places
        self.genders = genders

        self.shape = {
            'dates': len(self.dates),
            'cities': len(self.cities),
            'places': len(self.places),
            'genders': len(self.genders)
        }
        self.total = reduce(lambda x, y: x * y, self.shape.values())

        self.options = options

    def set_options(self, **options):
        self.options = options

    def __getitem__(self, i: int):
        if not (0 <= i < self.total):
            raise IndexError(f'Out of bounds for Request Queue with lenght {self.total}')
        else:
            data = {'age': self.age}
            ## gender
            i, j = divmod(i, self.shape['genders'])
            gender = self.genders[j]
            data['gender'] = gender

            ## place
            i, j = divmod(i, self.shape['places'])
            place_list = self.places[j]
            data['places'] = place_list

            ## city
            i, j = divmod(i, self.shape['cities'])
            state, city, city_id = self.cities[j]
            data['state'] = state
            data['city'] = city
            data['city_id'] = city_id

            ## date
            i, j = divmod(i, self.shape['dates'])
            start_date, end_date = self.dates[j]
            data['start_date'] = start_date
            data['end_date'] = end_date
            data['date'] = end_date

            return APIRequest(self.url, APIQuery(**data), APIResults(**data), **self.options)

class API:

    ## API constants
    API_URL = r'https://transparencia.registrocivil.org.br/api/covid-covid-registral'

    ## City information
    UPDATED_CITIES = False
    STATES, ID_TABLE = STATES, ID_TABLE

    ## Years
    YEARS = YEARS

    ## Possible places
    PLACES = PLACES

    ## Possible genders
    GENDERS = GENDERS

    ## Logging
    LOG_FNAME = 'api.log'
    log_file = open(LOG_FNAME, 'w') ## Creates file if it does not exists, erases previous if exists
    log_file.close()
    log_lock = threading.Lock()

    def __init__(self, 
                date=None,
                state=None,
                city=None,
                places=None,
                gender=None,
                age=False, 
                sync=not ASYNC_MODE,
                block_size=BLOCK_SIZE,
                threads=CPU_COUNT,
                ):
        """ This class is intended to:
            - Prepare and enqueue request given the query specified in 
        """
        ## default keyword arguments
        self.kwargs = {
            'date': date,
            'state': state,
            'city': city,
            'places' : places,
            'gender' : gender,
            'age': age,
            'sync' : sync,
            'block_size': block_size,
            'threads': threads,
        }
        ## Request Queue
        self.requests = self.get_request_queue(**self.kwargs)

        ## Threads
        self.threads = self.kwargs_threads(**self.kwargs)

        ## Sync
        self.sync = self.kwargs_sync(**self.kwargs)

        ## Block size
        self.block_size = self.kwargs_block_size(**self.kwargs)

        ## Progress
        self.progress = api_lib.Progress(self.total, lapse=1.0)

        ## Results
        self.results = []

    @classmethod
    def log(cls, s: str):
        header = f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}]"
        with cls.log_lock: 
            with open(cls.LOG_FNAME, 'a') as cls.log_file:
                print(header, s, file=cls.log_file)
    
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

    def kwargs_threads(self, **kwargs) -> int:
        """
        """
        threads_kwarg = kwargs['threads']
        if type(threads_kwarg) is not int:
            raise TypeError('O número de threads deve ser um inteiro positivo.')
        elif threads_kwarg < 1:
            raise ValueError('O número de threads deve ser um inteiro positivo.')
        elif threads_kwarg > os.cpu_count():
            warnings.warn('Número de threads maior do que o número de processadores.', stacklevel=2)
        return threads_kwarg

    def kwargs_block_size(self, **kwargs) -> int:
        """
        """
        block_size_kwarg = kwargs['block_size']
        if block_size_kwarg is None:
            return block_size_kwarg
        elif type(block_size_kwarg) is not int:
            raise TypeError('block_size is not int')
        elif block_size_kwarg <= 0:
            raise ValueError('block_size must be positive')
        else:
            return block_size_kwarg


    def kwargs_sync(self, **kwargs) -> bool:
        """
        """
        sync_kwarg = kwargs['sync']
        if type(sync_kwarg) is not bool:
            raise TypeError('`sync` deve ser True ou False (bool)')
        else:
            return sync_kwarg        

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

    def get_request_queue(self, **kwargs) -> APIRequestQueue:
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
            genders=genders
            )

    ## Multiprocessing things
    @staticmethod
    def target_func(section_num: int, section: range, request_queue: APIRequestQueue, progress: api_lib.Progress, sync: bool, block_size: int):
        client = APIClient(
            section=section,
            request_queue=request_queue,
            progress=progress,
            sync=sync,
            block_size=block_size
        )
        ## Get results generator
        results = client.get()

        ## Write to csv
        api_io.APIIO.to_csv(f'.results-{section_num}', results)

    def get(self) -> None:
        """
        """
        processes = []
        for section_num, section in self.sections:
            processes.append(
                mp.Process(
                    target=self.target_func, 
                    args=(section_num, section, self.requests, self.progress, self.sync, self.block_size)
                )
            )

        ## Starts displaying progress bar
        try:
            self.progress.start()
            for process in processes:
                process.start()
            for process in processes:
                process.join()
            print('Finished all processes.')
            self.progress.finish()
        except KeyboardInterrupt:
            print('\nAborted.')
            return
        finally:
            for process in processes:
                if process.is_alive():
                    process.kill()

        print('All processes finished. Writing results.')

        fnames = [f'.results-{section_num}.csv'for section_num in range(self.threads)]
        api_io.APIIO.join_csv('results.csv', fnames, delete_input=True)

        print('Finished.')

    def _sections(self):
        """ This generator simply divides range(0, self.total) into sections of size `self.threads`
        """
        if self.threads == 1:
            yield (0, range(self.total))
            return

        size = self.total // self.threads
        more = self.total % self.threads
        for i in range(self.threads - 1):
            yield (i, range(i * size, (i + 1) * size))
        else:
            yield (self.threads - 1, range((self.threads - 1) * size,  self.threads * size + more))
        
    @property
    def sections(self):
        return self._sections()
    
    @property
    def total(self):
        return self.requests.total

    @property
    def done(self):
        return self.progress.done

class APIClient:

    ## Login constants
    LOGIN_URL = r"https://transparencia.registrocivil.org.br/registral-covid"
    LOGIN_HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:76.0) Gecko/20100101 Firefox/76.0",
        "Cache-Control": "max-age=0",
    }

    def __init__(
            self,
            section: range,
            request_queue: APIRequestQueue,
            progress: api_lib.Progress,
            sync: bool=not ASYNC_MODE,
            block_size: int=1024,
            ):
        ## Section
        self.section = iter(section)

        ## Requests
        self.request_queue = request_queue

        ## Progress tracker
        self.progress = progress

        ## Total requests
        self.total = len(section)

        ## Cookies
        self.cookie_jar = CookieJar()

        ## XRSF-Token
        self.xrsf_token = None

        ## Block size
        self.block_size = block_size

        ## Asynchronous Requests
        self.sync = sync
        if not self.sync:
            self.loop = asyncio.get_event_loop()

    @property
    def request_headers(self):
        """ Headers used to avoid Error 403: Forbidden
        """
        return {
            "X-XSRF-TOKEN" : self.xrsf_token,
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }

    def login(self) -> None:
        """ Realiza o login na plataforma dos cartórios.
            Isso é feito extraindo o 'XSRF-Token' dos Cookies e adicionando aos headers.
        """
        ## Make request to page
        ans, req = api_lib.request(self.LOGIN_URL, headers=self.LOGIN_HEADERS)
        
        ## Extract Token from Cookies
        self.cookie_jar.extract_cookies(ans, req)
        
        ## Gets first occurence of the Token in the cookie jar
        self.xrsf_token = next(cookie for cookie in self.cookie_jar if cookie.name == "XSRF-TOKEN").value

    def ensure_login(self) -> None:
        """
        """
        while True:
            try:
                self.login()
                break
            except Exception as error:
                API.log(f'Error in Login: {error}')
                time.sleep(5)
                continue
        self.request_queue.set_options(headers=self.request_headers)

    def get(self) -> object:
        """
        """
        for block in self.blocks:
            while block:
                self.ensure_login()
                try:
                    if self.sync:
                        self.sync_run(block)
                    else:
                        self.async_run(block)
                except Exception as error:
                    API.log(error)
                finally:
                    results = []
                    pending = []
                    for request in block:
                        if request.success:
                            results.extend(request.results)
                            next(self.progress)
                        else:
                            pending.append(request)
                    else:
                        block = pending
                        self.progress.update()
                        yield from results

    ## Synchronous GET methods
    def sync_request(self, request: APIRequest):
        """ Dispara o request de maneira sequencial
        """
        request.get()

    def sync_run(self, requests: list):
        """ Dispara os requests de maneira sequencial
        """
        for request in requests: self.sync_request(request)

    ## Asynchronous GET methods
    async def async_request(self, request: APIRequest, session):
        """
        """
        await request.async_get(session)

    async def _async_run(self, requests: list):
        """ Dispara os requests de maneira assíncrona.
        """
        async with aiohttp.ClientSession(headers=self.request_headers) as session:
            tasks = [asyncio.ensure_future(self.async_request(request, session)) for request in requests]
            await asyncio.wait(tasks)

    def async_run(self, requests: list):
        """ Dispara os requests de maneira assíncrona.
        """
        if not ASYNC_MODE:
            raise ImportError("Falha ao obter as bibliotecas necessárias. Requisições assíncronas indisponíveis.")
        else:
            self.loop.run_until_complete(asyncio.ensure_future(self._async_run(requests)))

    def _blocks(self):
        """ This generator divides the pending requests into blocks of size `self.block_size`
        """
        ## Returns the whole batch
        if self.block_size is None:
            yield [self.request_queue[next(self.section)] for _ in range(self.total)]
        else:
            ## Splits the batch into blocks
            size = self.total // self.block_size
            for _ in range(size):
                yield [self.request_queue[next(self.section)] for _ in range(self.block_size)]
            yield [self.request_queue[next(self.section)] for _ in range(self.total - size * self.block_size)]
    
    @property
    def blocks(self):
        return self._blocks()