#!/usr/env/python3
import asyncio
import aiohttp
import csv
import json
import datetime
import itertools

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

    CSV_HEADER = ['date', 'state', 'city'] + [f'{cause}_{year}' for cause, year in CAUSE_YEAR]

    requests = []
    results = []

    @classmethod
    def extract_chart(cls, chart: dict):
        return {f'{cause}_{year}' : chart[year][cause] for cause, year in cls.CAUSE_YEAR}

    @classmethod
    def build_request(cls, meta_data:dict, date, **include) -> (str, dict):
        meta_data['start_date'] = meta_data['end_date'] = str(date)
        return (api_lib.encode_url(cls.API_URL, meta_data), {'date' : date, **include})

    ## -- SYNC --
    @classmethod
    def get_request(cls, url, data):
        return {**data, **cls.extract_chart(api_lib.get_request(url)['chart'])}

    @classmethod
    def get_requests(cls):
        cls.results.extend([cls.get_request(*req) for req in cls.requests])

    ## -- SYNC --

    ## -- ASYNC --
    @classmethod
    async def async_get_request(cls, url: str, data: dict, session: aiohttp.ClientSession):
        async with session.get(url) as ans:
            return {**data, **await ans.json()}

    @classmethod
    async def async_run(cls):
        async with aiohttp.ClientSession() as session:
            tasks = [asyncio.ensure_future(cls.async_get_request(*req, session)) for req in cls.requests]
            cls.results.extend(await asyncio.gather(*tasks))

    @classmethod
    def async_get_requests(cls):
        cls.loop = asyncio.get_event_loop()
        cls.loop.run_until_complete(asyncio.ensure_future(cls.async_run()))

    ## -- ASYNC --

    @classmethod
    def get_date_kwarg(cls, date_kwarg: object) -> list:
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
        return [date for date in api_lib.arange(start, stop, step)]

    @classmethod
    def get_state_kwarg(cls, state_kwarg: object) -> list:
        if type(state_kwarg) is str:
            if state_kwarg in cls.STATES:
                return [state_kwarg]
            else:
                raise ValueError(f'Estado não cadastrado: {state_kwarg}')
        elif state_kwarg is all:
            return list(cls.STATES.keys())
        elif type(state_kwarg) is set:
            return sum([cls.get_state_kwarg(x) for x in state_kwarg], [])
        else:
            raise ValueError(f'Especificação de estado inválida: `{state_kwarg}`.')

    @classmethod
    def get_city_kwarg(cls, city_kwarg: object) -> list:
        if type(city_kwarg) is str:
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
            return sum([cls.get_city_kwarg(x) for x in city_kwarg], [])
        else:
            raise ValueError('Especificação de cidade inválida: {city}.\nO formato correto é `Nome da Cidade-UF`')

    @classmethod
    def city_id(cls, state: str, city_name: str):
        try:
            return cls.ID_TABLE[(state, city_name)]
        except KeyError:
            if not cls.UPDATED_CITIES:
                api_lib.update_cities()
                cls.STATES, cls.ID_TABLE = api_lib.load_cities()
                cls.UPDATED_CITIES = True
                return cls.city_id(state, city_name)
            else:
                raise ValueError(f'Cidade não cadastrada: `{city_name} ({state})`.')

    @classmethod
    def get_cities(cls, dates: list, cities) -> list:
        """ Obtém dados a nível municipal
        """
        meta_data = {}
        for date in dates:
            for state, city, city_id in cities:
                meta_data['city_id'] = city_id
                meta_data['state'] = state
                yield cls.build_request(meta_data, date, state=state, city=city)

    @classmethod
    def get_all_cities_in_states(cls, dates, states) -> list:
        """
        """
        meta_data = {}      
        for date in dates:
            for state in states:
                meta_data['state'] = state
                for city in cls.STATES[state]:
                    meta_data['city_id'] = cls.city_id(state, city)
                    yield cls.build_request(meta_data, date, state=state, city=city)

    @classmethod
    def get_states(cls, dates: list, states) -> list:
        """ Obtém dados a nível estadual
        """
        meta_data = {}
        for date in dates:
            for state in states:
                meta_data['state'] = state
                yield cls.build_request(meta_data, date, state=state)
        
    @classmethod
    def get_country(cls, dates: list) -> list:
        """ Obtém dados a nível federal
        """
        meta_data = {'state' : 'Todos'}
        for date in dates:
            yield cls.build_request(meta_data, date)
        
    @classmethod
    @api_lib.time
    def get(cls, **kwargs) -> list:
        """
        """
        ## Reset results and requests
        del cls.results[:]
        del cls.requests[:]

        ## default keyword arguments
        filters = {
            'date' : None,
            'state' : None,
            'city' : None,
            'sync' : False,
        }
        filters.update(kwargs)

        sync = filters['sync']
        dates = cls.get_date_kwarg(filters['date'])
        
        ## Nível Federal
        if filters['state'] is None and filters['city'] is None:
            requests = cls.get_country(dates)
        ## Nível Estadual
        elif filters['city'] is None:
            if filters['state'] in {None, all} or type(filters['state']) in {set, str}:
                states = cls.get_state_kwarg(filters['state'])
                requests = cls.get_states(dates, states)
        ## Nível Municipal
        elif filters['city'] is all and filters['state'] is not None:
            states = cls.get_state_kwarg(filters['state'])
            requests = cls.get_all_cities_in_states(dates, states)
        elif type(filters['city']) in {set, str} and filters['state'] is None:
            cities = cls.get_city_kwarg(filters['city'])
            requests = cls.get_cities(dates, cities)
        else:
            raise ValueError(f"Especificação inválida de localização: (city={filters['city']!r}, state={filters['state']!r})")

        cls.requests.extend(requests)
        
        if sync:
            cls.get_requests()
        else:
            cls.async_get_requests()

        return cls.results
        

    @classmethod
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

        with open(fname, 'w') as file:
            writer = csv.DictWriter(file, fieldnames=cls.CSV_HEADER)
            writer.writeheader()
            for result in results:
                row = {key : (str(result[key]) if key in result else "") for key in cls.CSV_HEADER}
                writer.writerow(row)
