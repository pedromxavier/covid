#!/usr/env/python3
## Standard Library
from http.cookiejar import CookieJar
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

class API:

    API_URL = r'https://transparencia.registrocivil.org.br/api/covid-covid-registral'

    ## Login
    LOGIN_URL = r"https://transparencia.registrocivil.org.br/registral-covid"
    XSRF_TOKEN = ""
    LOGIN_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache"
    }

    UPDATED_CITIES = False

    STATES, ID_TABLE = api_lib.load_cities()

    CAUSES = (
                'COVID',
                'SRAG',
                'PNEUMONIA',
                'INSUFICIENCIA_RESPIRATORIA',
                'SEPTICEMIA',
                'INDETERMINADA',
                'OUTRAS'
    )

    YEARS = ('2019', '2020')

    CAUSE_YEAR = [item for item in itertools.product(CAUSES, YEARS) if item != ('COVID', '2019')]

    CAUSE_KEYS = [f'{cause}_{year}' for cause, year in CAUSE_YEAR]

    CSV_HEADER = ['date', 'state', 'city', 'region'] + CAUSE_KEYS

    PLACES = {'HOSPITAL', 'DOMICILIO', 'VIA_PUBLICA', 'AMBULANCIA', 'OUTROS'}

    ## Log
    LOG_FNAME = 'api.log'

    DEFAULT_RESULTS = {cause_key : 0 for cause_key in CAUSE_KEYS}

    RESULT_DATA = {
        **{'date': '', 'state': '', 'city': '', 'region': ''}, 
        **DEFAULT_RESULTS
    }

    BLOCK_SIZE = 1024

    WAIT = 10

    class APIResults(object):
        
        __slots__ = ('date', 'state', 'city', 'region') + ('COVID','SRAG','PNEUMONIA','INSUFICIENCIA_RESPIRATORIA','SEPTICEMIA','INDETERMINADA','OUTRAS')

        def __init__(self, **kwargs):
            for name in self.__slots__: setattr(self, name, kwargs[name])

    class APIRequest(object):

        __slots__ = 'url', 'data', 'success', 'results'

        def __init__(self, url: str, data: dict):
            self.url = url
            self.data = data
            self.success = False
            self.results = None

        def __repr__(self):
            return f"APIRequest[{self.success}]"

        def commit(self, results: object):
            self.results = results
            self.success = True

    class Finish(Exception):
        pass

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
        self.BLOCK_SIZE = self.kwargs['block']

        ## Logging
        self.log_file = open(self.LOG_FNAME, 'a')
        self.log_file.close()
        self.log_lock = threading.Lock()

        ## Cache results
        self.cache = self.__get_cache_kwarg(kwargs['cache'])

        ## Cookies
        self.cookie_jar = CookieJar()

        ## Request Queue
        self.requests = self._requests(**self.kwargs)

        ## Results
        self.results = []

        ## Asynchronous Requests
        if not self.kwargs['sync']:
            self.loop = asyncio.get_event_loop()
            self.semaphore = asyncio.Semaphore(self.BLOCK_SIZE)
            self.timeout = aiohttp.ClientTimeout(total=(self.total * 5))

    @property
    def total(self):
        if hasattr(self, 'progress') and hasattr(self.progress, 'total'):
            return self.progress.total
        else:
            return self._total

    @property
    def done(self):
        if hasattr(self, 'progress') and hasattr(self.progress, 'done'):
            return self.progress.done
        else:
            return self._done
    
    @property
    def _total(self):
        return len(self.requests)

    @property
    def _done(self):
        return sum([req.success for req in self.requests])

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
    
    def extract_chart(self, chart: dict):
        chart = chart['chart']
        chart_data = {f'{cause}_{year}': chart[year][cause] for year in chart for cause in chart[year]}
        return {**self.DEFAULT_RESULTS, **chart_data}
    
    def build_request(self, query_data:dict, date, **include) -> (str, dict):
        query_data['start_date'] = str(date[0])
        query_data['end_date'] = str(date[1])
        url = api_lib.encode_url(self.API_URL, query_data)
        return self.APIRequest(url, {**self.RESULT_DATA, 'date' : query_data['end_date'], **include})

    ## -- SYNC --
    def __get_request(self, req: APIRequest):
        try:
            ans_data = self.extract_chart(api_lib.get_request_json(req.url, headers=self.REQUEST_HEADERS))
            req.commit({**req.data, **ans_data})
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
                    raise ValueError(f'Local inválido `{place}`.\nAs opções válidas são: {" - ".join(self.PLACES)}')
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
    ## -- KWARGS --

    def city_id(self, state: str, city_name: str):
        """
        """
        ascii_city_name = api_lib.ascii_decode(city_name)
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
    
    def __get_cities(self, dates: list, cities, cumulative: bool=True, places: list=PLACES) -> list:
        """ Obtém dados a nível municipal
        """
        query_data = {
            'places[]': places
            }
        include = {
            'cumulative': cumulative,
            'place': '&'.join(places)
            }
        for date in dates:
            for state, city, city_id in cities:
                query_data['city_id'] = city_id
                query_data['state'] = state
                include['city'] = city
                include['state'] = state
                yield self.build_request(query_data, date, **include)
    
    def __get_all_cities_in_states(self, dates, states, cumulative: bool=True, places: list=PLACES) -> list:
        """ Obtém dados a nível municipal
        """
        query_data = {
            'places[]': places
            }
        include = {
            'cumulative': cumulative,
            'place': '&'.join(places)
            }
        for date in dates:
            for state in states:
                query_data['state'] = state
                include['state'] = state
                for city in self.STATES[state]:
                    query_data['city_id'] = self.city_id(state, city)
                    include['city'] = city
                    yield self.build_request(query_data, date, **include)

    
    def __get_states(self, dates: list, states, cumulative: bool=True, places: list=PLACES) -> list:
        """ Obtém dados a nível estadual
        """
        query_data = {
            'places[]': places
            }
        include = {
            'cumulative': cumulative,
            'place': '&'.join(places),
            'city': ''
            }
        for date in dates:
            for state in states:
                query_data['state'] = state
                include['state'] = state
                yield self.build_request(query_data, date, **include)
        
    def __get_country(self, dates: list, cumulative: bool=True, places: list=PLACES) -> list:
        """ Obtém dados a nível federal
        """
        query_data = {
            'state': 'Todos',
            'places[]': places
            }
        include = {
            'cumulative': cumulative,
            'place': '&'.join(places),
            'state': '',
            'city': ''
            }
        for date in dates:
            yield self.build_request(query_data, date, **include)

    def __get_cache_kwarg(self, cache_kwarg: str):
        if type(cache_kwarg) is bool and cache_kwarg is False:
            return cache_kwarg
        elif type(cache_kwarg) is str:
            return f"{cache_kwarg}.p"
        else:
            raise TypeError(f"Parâmetro 'cache' deve ser do tipo 'str' ou 'False'.")

    def _requests(self, **kwargs) -> list:
        """
        """
        if self.cache:
            try:
                requests = api_lib.pkload(self.cache)
            except FileNotFoundError:
                pass
            else:
                ## If any request was retrieved
                if requests: return requests

        dates = self.__get_date_kwarg(kwargs['date'], cumulative=kwargs['cumulative'])
        places = self.__get_places_kwarg(kwargs['places'])
        
        ## Nível Federal
        if kwargs['state'] is None and kwargs['city'] is None:
            requests = self.__get_country(dates, cumulative=kwargs['cumulative'], places=places)
        ## Nível Estadual
        elif kwargs['city'] is None:
            if kwargs['state'] in {None, all} or type(kwargs['state']) in {set, str}:
                states = self.__get_state_kwarg(kwargs['state'])
                requests = self.__get_states(dates, states, cumulative=kwargs['cumulative'], places=places)
        ## Nível Municipal
        elif kwargs['city'] is all and kwargs['state'] is not None:
            states = self.__get_state_kwarg(kwargs['state'])
            requests = self.__get_all_cities_in_states(dates, states, cumulative=kwargs['cumulative'], places=places)
        elif type(kwargs['city']) in {set, str} and kwargs['state'] is None:
            cities = self.__get_city_kwarg(kwargs['city'])
            requests = self.__get_cities(dates, cities, cumulative=kwargs['cumulative'], places=places)
        elif type(kwargs['city']) in {set, str} and type(kwargs['state']) is str:
            cities = self.__get_city_kwarg(kwargs['city'], state_sufix=kwargs['state'])
            requests = self.__get_cities(dates, cities, cumulative=kwargs['cumulative'], places=places)
        else:
            raise ValueError(f"Especificação inválida de localização: (city={kwargs['city']!r}, state={kwargs['state']!r})")
        
        return list(requests)

    @api_lib.time
    @api_lib.no_suspend
    @api_lib.log
    def get_all(self, **kwargs) -> list:
        attempts = 1
        while True:
            try:
                print(f'Tentativa nº {attempts}')
                results = self.get(**kwargs)
            except KeyboardInterrupt:
                print(':: Cancelado ::')
                break
            except self.Finish:
                print(':: Concluído ::')
                break
            except Exception:
                pass
            finally:
                attempts += 1
                time.sleep(self.WAIT)
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
        self._gather(**kwargs)
        return self.results

    def _gather(self, **kwargs):
        self.progress = api_lib.progress(self._total, self._done)
        self.log("START @ gather")
        finish = False
        for block in self._blocks():
            try:
                if kwargs['sync']:
                    self.__get_requests(block)
                else:
                    self.__async_get_requests(block)
            except Exception:
                break
        else:
            finish = True
        
        ## results <- requests
        self.results = [req for req in self.requests]

        ## cache actual results
        self.cache_results()

        ## raise finish signal
        if finish: raise self.Finish

    def cache_results(self):
        if self.cache:
            api_lib.pkdump(self.cache, self.results)
            print(f"Resultados salvos em cache: '{self.cache}'")


    def _blocks(self):
        requests = [req for req in self.requests if not req.success]

        for i in range(0, len(requests), self.BLOCK_SIZE):
            yield requests[i:i+self.BLOCK_SIZE]
    
    def rate(self, res: list) -> float:
        return self.done / self.total

    def to_json(self, fname: str, results: list) -> str:
        if not fname.endswith('.json'):
            fname = f'{fname}.json'

        with open(fname, 'w') as file:
            file.write(json.dumps(results))

    def to_csv(self, fname: str, results: list):
        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'

        with open(fname, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=self.CSV_HEADER)
            writer.writeheader()
            for result in results:
                row = {key : (str(result[key]) if key in result else "") for key in self.CSV_HEADER}
                writer.writerow(row)
    
    def union(self, res: list, **kwargs) -> list:
        """
        """
        api_lib.kwget(kwargs, {
            'region': ''
        })
        union_data = {}
        for data in res:
            date = data['date']
            if date not in union_data:
                union_data[date] = {cause_key : 0 for cause_key in self.CAUSE_KEYS}
            for cause_key in self.CAUSE_KEYS:
                union_data[date][cause_key] += data[cause_key]
        results = []
        for date in union_data:
            union_data[date]['date'] = date
            union_data[date]['region'] = kwargs['region']
            results.append(union_data[date])
        return sorted(results, key=lambda x: x['date'])
    
    @property
    def REQUEST_HEADERS(self):
        return {
            "X-XSRF-TOKEN" : self.XSRF_TOKEN,
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:76.0) Gecko/20100101 Firefox/76.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }



        

