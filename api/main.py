#!/usr/env/python3
## Standard Library
import asyncio
import csv
import json
import datetime
import itertools
import threading
import warnings

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

    QUERY_DATA = {
        'places[]' : ['HOSPITAL', 'DOMICILIO', 'VIA_PUBLICA', 'AMBULANCIA', 'OUTROS']
    }

    requests = []
    results = []

    done = 0
    lock = threading.Lock()

    RESULT_DATA = {
        **{'date': '', 'state': '', 'city': '', 'region': ''}, 
        **{cause_key : 0 for cause_key in CAUSE_KEYS}
    }

    @classmethod
    def progress(cls):
        with cls.lock:
            cls.done += 1
            end = "\r" if cls.done < cls.total else "\n"
            x = cls.done/cls.total
            print(f'Progresso: {cls.pbar(x)} {cls.done}/{cls.total} {100 * x:.2f}%      ', end=end)
            
    @classmethod
    def pbar(cls, x: float):
        if x < 1:
            return f"[{int(x * 16) * '='}>{int((1-x) * 16) * ' '}]"
        else:
            return f"[{'=' * 16}]"

    @classmethod
    def extract_chart(cls, chart: dict):
        return {f'{cause}_{year}' : chart[year][cause] for cause, year in cls.CAUSE_YEAR}

    @classmethod
    def build_request(cls, query_data:dict, date, **include) -> (str, dict):
        query_data['start_date'] = str(date[0])
        query_data['end_date'] = str(date[1])
        url = api_lib.encode_url(cls.API_URL, query_data)
        return (url, {**cls.RESULT_DATA, 'date' : query_data['end_date'], **include})

    ## -- SYNC --
    @classmethod
    def __get_request(cls, url, data):
        ans_data = cls.extract_chart(api_lib.get_request(url)['chart'])
        cls.progress()
        return {**data, **ans_data}

    @classmethod
    def __get_requests(cls):
        return [cls.__get_request(*req) for req in cls.requests]
    ## -- SYNC --

    ## -- ASYNC --    
    @classmethod
    async def __async_get_request(cls, url: str, data: dict, session):
        async with cls.semaphore:
            async with session.get(url) as ans:
                try:
                    ans_data = cls.extract_chart((await ans.json())['chart'])
                    cls.progress()
                    return {**data, **ans_data}
                except:
                    raise Exception(f'Code {ans.status} in GET {url}')

    @classmethod
    async def __async_run(cls):
        cls.semaphore = asyncio.Semaphore(1024)
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.ensure_future(cls.__async_get_request(*req, session)) for req in cls.requests]
            cls.results.extend(await asyncio.gather(*tasks))

    @classmethod
    def __async_get_requests(cls):
        cls.loop = asyncio.get_event_loop()
        cls.loop.run_until_complete(asyncio.ensure_future(cls.__async_run()))

    ## -- ASYNC --

    @classmethod
    def __get_date_kwarg(cls, date_kwarg: object, cumulative: bool=True) -> list:
        if type(date_kwarg) is tuple and len(date_kwarg) == 2:
            start, stop = map(api_lib.get_date, date_kwarg)
        elif date_kwarg is all:
            start = datetime.date(2020, 1, 1)
            stop = datetime.date.today()
        elif date_kwarg is None:
            start = datetime.date.today()
            stop = datetime.date.today()
        elif type(date_kwarg) is datetime.date or type(datetime) is str:
            start = stop = api_lib.get_date(date_kwarg)
        else:
            raise TypeError(f'Especificação de data inválida: `{date_kwarg}`')
        step = datetime.timedelta(days=1)

        if cumulative:
            return [(start, date) for date in api_lib.arange(start, stop, step)]
        else:
            return [(date, date) for date in api_lib.arange(start, stop, step)]

    @classmethod
    def __get_state_kwarg(cls, state_kwarg: object) -> list:
        if type(state_kwarg) is str:
            if state_kwarg in cls.STATES:
                return [state_kwarg]
            else:
                raise ValueError(f'Estado não cadastrado: {state_kwarg}')
        elif state_kwarg is all:
            return list(cls.STATES.keys())
        elif type(state_kwarg) is set:
            return sum([cls.__get_state_kwarg(x) for x in state_kwarg], [])
        else:
            raise ValueError(f'Especificação de estado inválida: `{state_kwarg}`.')

    @classmethod
    def __get_city_kwarg(cls, city_kwarg: object, state_sufix: str=None) -> list:
        if type(city_kwarg) is str:
            if state_sufix is not None:
                city_kwarg = f'{city_kwarg}-{state_sufix}'
            city, state = api_lib.get_city(city_kwarg)
            return [(state, city, cls.city_id(state, city))]
        elif city_kwarg is all:
            cities = []
            states = cls.STATES
            for state in states:
                for city in states[state]:
                    cities.append((state, city, cls.city_id(state, city)))
            return cities
        elif type(city_kwarg) is set:
            return sum([cls.__get_city_kwarg(x, state_sufix=state_sufix) for x in city_kwarg], [])
        else:
            raise ValueError(f'Especificação de cidade inválida: {city}.\nO formato correto é `Nome da Cidade-UF`')

    @classmethod
    def city_id(cls, state: str, city_name: str):
        ascii_city_name = api_lib.ascii_decode(city_name)
        try:
            return cls.ID_TABLE[(state, ascii_city_name)]
        except KeyError:
            if not cls.UPDATED_CITIES:
                api_lib.update_cities()
                cls.STATES, cls.ID_TABLE = api_lib.load_cities()
                cls.UPDATED_CITIES = True
                return cls.city_id(state, ascii_city_name)
            else:
                raise ValueError(f'Cidade não cadastrada: `{city_name} ({state})`.')

    @classmethod
    def __get_cities(cls, dates: list, cities, cumulative: bool=True) -> list:
        """ Obtém dados a nível municipal
        """
        query_data = cls.QUERY_DATA.copy()
        for date in dates:
            for state, city, city_id in cities:
                query_data['city_id'] = city_id
                query_data['state'] = state
                yield cls.build_request(query_data, date, state=state, city=city, cumulative=cumulative)

    @classmethod
    def __get_all_cities_in_states(cls, dates, states, cumulative: bool=True) -> list:
        """
        """
        query_data = cls.QUERY_DATA.copy()
        for date in dates:
            for state in states:
                query_data['state'] = state
                for city in cls.STATES[state]:
                    query_data['city_id'] = cls.city_id(state, city)
                    yield cls.build_request(query_data, date, state=state, city=city, cumulative=cumulative)

    @classmethod
    def __get_states(cls, dates: list, states, cumulative: bool=True) -> list:
        """ Obtém dados a nível estadual
        """
        query_data = cls.QUERY_DATA.copy()
        for date in dates:
            for state in states:
                query_data['state'] = state
                yield cls.build_request(query_data, date, state=state, cumulative=cumulative)
        
    @classmethod
    def __get_country(cls, dates: list, cumulative: bool=True) -> list:
        """ Obtém dados a nível federal
        """
        query_data = {**cls.QUERY_DATA.copy(), 'state' : 'Todos'}
        for date in dates:
            yield cls.build_request(query_data, date, cumulative=cumulative)
        
    @classmethod
    @api_lib.time
    def get(cls, **kwargs) -> list:
        """
        """
        ## Reset Progress counters
        cls.done = 0
        cls.total = 0

        ## Reset results and requests
        del cls.results[:]
        del cls.requests[:]

        ## default keyword arguments
        api_lib.kwget(kwargs, {
            'cumulative' : True,
            'date' : None,
            'state' : None,
            'city' : None,
            'sync' : not ASYNC_LIB,
        })

        sync = kwargs['sync']
        dates = cls.__get_date_kwarg(kwargs['date'], cumulative=kwargs['cumulative'])
        
        ## Nível Federal
        if kwargs['state'] is None and kwargs['city'] is None:
            requests = cls.__get_country(dates, cumulative=kwargs['cumulative'])
        ## Nível Estadual
        elif kwargs['city'] is None:
            if kwargs['state'] in {None, all} or type(kwargs['state']) in {set, str}:
                states = cls.__get_state_kwarg(kwargs['state'])
                requests = cls.__get_states(dates, states, cumulative=kwargs['cumulative'])
        ## Nível Municipal
        elif kwargs['city'] is all and kwargs['state'] is not None:
            states = cls.__get_state_kwarg(kwargs['state'])
            requests = cls.__get_all_cities_in_states(dates, states, cumulative=kwargs['cumulative'])
        elif type(kwargs['city']) in {set, str} and kwargs['state'] is None:
            cities = cls.__get_city_kwarg(kwargs['city'])
            requests = cls.__get_cities(dates, cities, cumulative=kwargs['cumulative'])
        elif type(kwargs['city']) in {set, str} and type(kwargs['state']) is str:
            cities = cls.__get_city_kwarg(kwargs['city'], state_sufix=kwargs['state'])
            requests = cls.__get_cities(dates, cities, cumulative=kwargs['cumulative'])
        else:
            raise ValueError(f"Especificação inválida de localização: (city={kwargs['city']!r}, state={kwargs['state']!r})")

        cls.requests.extend(requests)

        cls.total = len(cls.requests)

        print(f'Total de requisições: {cls.total}')
        
        if sync:
            cls.__get_requests()
        else:
            try:
                cls.__async_get_requests()
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
        return cls.results
        
    @classmethod
    @api_lib.time
    def to_json(cls, fname: str, results: list) -> str:
        if not fname.endswith('.json'):
            fname = f'{fname}.json'

        with open(fname, 'w') as file:
            file.write(json.dumps(results))

    @classmethod
    @api_lib.time
    def to_csv(cls, fname: str, results: list):
        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'

        with open(fname, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=cls.CSV_HEADER)
            writer.writeheader()
            for result in results:
                row = {key : (str(result[key]) if key in result else "") for key in cls.CSV_HEADER}
                writer.writerow(row)

    @classmethod
    def union(cls, res: list, **kwargs) -> list:
        """
        """
        api_lib.kwget(kwargs, {
            'region': ''
        })
        union_data = {}
        for data in res:
            date = data['date']
            if date not in union_data:
                union_data[date] = {cause_key : 0 for cause_key in cls.CAUSE_KEYS}
            for cause_key in cls.CAUSE_KEYS:
                union_data[date][cause_key] += data[cause_key]
        results = []
        for date in union_data:
            union_data[date]['date'] = date
            union_data[date]['region'] = kwargs['region']
            results.append(union_data[date])
        return sorted(results, key=lambda x: x['date'])



        

