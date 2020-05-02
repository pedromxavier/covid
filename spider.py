import datetime
import json
from urllib.parse import urlencode, urljoin

import scrapy

import xlib

class Spider(scrapy.Spider):
    FILE_NAME = "obitos"
    
    API_URL = "https://transparencia.registrocivil.org.br/api/covid"

    DATE_TYPES = ["data_ocorrido", "data_registro"]

    SEARCH_TYPES = ["death-respiratory", "death-covid"]

    CAUSES = ["pneumonia", "insuficiencia_respiratoria", None] ## None is for covid19

    CITIES = xlib.load_cities("data/cidades.csv")

    def make_request(self, **kwargs):
        meta_data = kwargs.copy()
        
        data = {key:str(value) for key,value in meta_data.items() if value is not None and key is not 'callback'}

        url = urljoin(self.API_URL, f"?{urlencode(data)}")

        request_kwargs = {
            'url' : url,
            'callback' : meta_data['callback'],
            'meta' : {"meta_data": meta_data}
        }

        return scrapy.Request(**request_kwargs)

    # TODO: death-covid &groupBy=gender

    def start_requests(self):
        meta_data = {
            'data_type' : self.DATE_TYPES[0], ## 'data_ocorrido'
            'callback' : self.parse_request
        }
        for date in xlib.arange(datetime.date(2020, 1, 1), datetime.date.today()):
            meta_data['start_date'] = meta_data['end_date'] = date
            for state in self.CITIES:
                meta_data['state'] = state
                for city in self.CITIES[state]:
                    meta_data['city'] = city ## There is not clear way to query by city
                                             ## the query key is still unknown
                    for cause in self.CAUSES:
                        meta_data['cause'] = cause
                        if cause is None:
                            meta_data['search'] = "death-covid"
                        else:
                            meta_data['search'] = "death-respiratory" 
                        yield self.make_request(**meta_data)
                        
    def parse_request(self, response):
        meta_data = response.meta["meta_data"].copy()
        answer = json.loads(response.body)

        meta_data['date']

        meta_data["date"] = meta_data["start_date"]
        date = datetime.fromisoformat(meta_data["date"])
        
        chart_data = answer["chart"]
        if not chart_data:
            meta_data["qtd_2019"] = meta_data["qtd_2020"] = 0
        else:
            if "2019" in chart_data and "2020" in chart_data:
                try:
                    datetime.date(2019, date.month, date.day)
                except ValueError:
                    # This day does not exist on 2019 and the API returned
                    # 2019's next day data.
                    row["qtd_2019"] = 0
                else:
                    row["qtd_2019"] = chart_data["2019"]
                row["qtd_2020"] = chart_data["2020"]
            else:
                row["qtd_2019"] = None
                
                key = list(chart_data.keys())[0]
                day, month = key.split("/")
                assert f"2020-{int(month):02d}-{int(day):02d}" == str(date)
                meta_data["qtd_2020"] = chart_data[key]
        yield meta_data