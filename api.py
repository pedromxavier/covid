#!/usr/env/python3
from collections import deque
import csv
import json
import datetime
import itertools

import xlib

class API:

    API_URL = r'https://transparencia.registrocivil.org.br/api/covid-covid-registral'

    UPDATED_CITIES = False

    STATES, ID_TABLE = xlib.load_cities()

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

    request_queue = deque([])

    @classmethod
    def fetch_chart(cls, chart: dict):
        return {f'{cause}_{year}' : chart[year][cause] for cause, year in cls.CAUSE_YEAR}

    @classmethod
    def fetch_data(cls, meta_data: dict, date, **include):
        meta_data['start_date'] = meta_data['end_date'] = str(date)
        chart = xlib.fetch_data(cls.API_URL, meta_data)[0]['chart']
        return {'date' : date, **include, **cls.fetch_chart(chart)}

    @classmethod
    def fetch_request(cls, meta_data:dict, date, **include):
        meta_data['start_date'] = meta_data['end_date'] = str(date)
        return (xlib.fetch_url(cls.API_URL, meta_data), {'date' : date, **include})

    @classmethod
    def enqueue_request(cls, url, data):
        cls.request_queue.appendleft((url, data))

    @classmethod
    def get_request(cls, url, data):
        return {**data, **cls.fetch_chart(xlib.make_request(url)[0]['chart'])}

    @classmethod
    def get_requests(cls):
        while cls.request_queue:
            url, data = cls.request_queue.pop()
            yield cls.get_request(url, data)

    @classmethod
    def get_date_kwarg(cls, date_kwarg: object) -> list:
        if type(date_kwarg) is tuple and len(date_kwarg) == 2:
            start, stop = map(xlib.get_date, date_kwarg)
        elif date_kwarg is all:
            start = datetime.date(2020, 1, 1)
            stop = datetime.date.today()
        elif date_kwarg is None:
            start = datetime.date.today()
            stop = datetime.date.today()
        elif type(date_kwarg) is datetime.date or type(datetime) is str:
            start = stop = xlib.get_date(date_kwarg)
        else:
            raise TypeError(f'Especificação de data inválida: `{date_kwarg}`')
        step = datetime.timedelta(days=1)
        return [date for date in xlib.arange(start, stop, step)]

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
            city, state = xlib.get_city(city_kwarg)
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
                xlib.update_cities()
                cls.STATES, cls.ID_TABLE = xlib.load_cities()
                cls.UPDATED_CITIES = True
                return cls.city_id(state, city_name)
            else:
                raise ValueError(f'Cidade não cadastrada: `{city_name} ({state})`.')

    @classmethod
    def get_cities(cls, dates: list, cities, sync=True) -> list:
        """ Obtém dados a nível municipal
        """
        meta_data = {}
        if sync:
            for date in dates:
                for state, city, city_id in cities:
                    meta_data['city_id'] = city_id
                    meta_data['state'] = state
                    yield cls.fetch_data(meta_data, date, state=state, city=city)   
        else: ## async
            for date in dates:
                for state, city, city_id in cities:
                    meta_data['city_id'] = city_id
                    meta_data['state'] = state
                    url, data = cls.fetch_request(meta_data, date, state=state, city=city)
                    cls.enqueue_request(url, data)

    @classmethod
    def get_all_cities_in_states(cls, dates, states, sync=True) -> list:
        """
        """
        meta_data = {}
        if sync:       
            for date in dates:
                for state in states:
                    meta_data['state'] = state
                    for city in cls.STATES[state]:
                        meta_data['city_id'] = cls.city_id(state, city)
                        yield cls.fetch_data(meta_data, date, state=state, city=city)
        else: ## async
            for date in dates:
                for state in states:
                    meta_data['state'] = state
                    for city in cls.STATES[state]:
                        meta_data['city_id'] = cls.city_id(state, city)
                        url, data = cls.fetch_request(meta_data, date, state=state, city=city)
                        cls.enqueue_request(url, data)

    @classmethod
    def get_states(cls, dates: list, states, sync=True) -> list:
        """ Obtém dados a nível estadual
        """
        meta_data = {}
        if sync:
            for date in dates:
                for state in states:
                    meta_data['state'] = state
                    yield cls.fetch_data(meta_data, date, state=state)
        else: ## async
            for date in dates:
                for state in states:
                    meta_data['state'] = state
                    url, data = cls.fetch_request(meta_data, date, state=state)
                    cls.enqueue_request(url, data)
            return cls.get_requests()

    @classmethod
    def get_country(cls, dates: list, sync=True) -> list:
        """ Obtém dados a nível federal
        """
        meta_data = {'state' : 'Todos'}
        if sync:
            for date in dates:
                yield cls.fetch_data(meta_data, date)
        else:
            for date in dates:
                url, data = cls.fetch_request(meta_data, date)
                cls.enqueue_request(url, data)
            return cls.get_requests()

    @classmethod
    def get(cls, sync=True, **kwargs) -> list:
        """ Filtros (**kwargs):
                date # Busca os resultados de hoje.
                    = (None)
                    # Retorna os resultados de todos os dias (de 01/01 até hoje)
                    | (all)
                    # Busca os resultados do dia especificado.
                    | (datetime.date)
                    # Busca os resultados neste intervalo de dias
                    | (datetime.date, datetime.date)
                state # Retorna os resultados a nível nacional se city for None
                    # Caso contrário, retorna dados a nível municipal.
                    = (None)
                    # Retorna os resultados de todos os estados a nível estadual
                    | (all)
                    # Retorna os resultados a nível estadual para o estado especificado
                    | (str)
                    # Retorna os resultados a nível estadual para os estados especificados
                    | (set) = {str, ... , str}
                city # Retorna os resultados a nível superior
                    = (None)
                    # Retorna os resultados de todas as cidades a nível municipal
                    # (limitado pelo escopo de `state`)
                    | (all)
                    # Retorna os resultados a nível municipal
                    | (str) = f"{city}-{state}"
                    #  
                    | (set) = {f"{city}-{state}",... ,f"{city}-{state}"}

            Exemplo:
            >>> api_get(state={'RJ', 'SP'}) # Busca em todas as cidades do rio e de são paulo
            >>> api_get(state="RJ", city="Rio de Janeiro")
        """
        
        filters = {
            'date' : None,
            'state' : None,
            'city' : None,
        }

        filters.update(kwargs)

        dates = cls.get_date_kwarg(filters['date'])
        
        ## Nível Federal
        if filters['state'] is None and filters['city'] is None:
            return cls.get_country(dates, sync=sync)
        ## Nível Estadual
        elif filters['city'] is None:
            if filters['state'] in {None, all} or type(filters['state']) in {set, str}:
                states = cls.get_state_kwarg(filters['state'])
                return cls.get_states(dates, states, sync=sync)
        ## Nível Municipal
        elif filters['city'] is all and filters['state'] is not None:
            states = cls.get_state_kwarg(filters['state'])
            return cls.get_all_cities_in_states(dates, states, sync=sync)
        elif type(filters['city']) in {set, str} and filters['state'] is None:
            cities = cls.get_city_kwarg(filters['city'])
            return cls.get_cities(dates, cities, sync=sync)
        else:
            raise ValueError(f"Especificação inválida de localização: (city={filters['city']!r}, state={filters['state']!r})")

    @classmethod
    def to_json(cls, fname: str, results: list) -> str:
        if not fname.endswith('.json'):
            fname = f'{fname}.json'

        with open(fname, 'w') as file:
            file.write(json.dumps(results))

    @classmethod
    @xlib.time
    def to_csv(cls, fname: str, results: list):
        if not fname.endswith('.csv'):
            fname = f'{fname}.csv'

        with open(fname, 'w') as file:
            writer = csv.DictWriter(file, fieldnames=cls.CSV_HEADER)
            writer.writeheader()
            for result in results:
                row = {key : (str(result[key]) if key in result else "") for key in cls.CSV_HEADER}
                writer.writerow(row)
