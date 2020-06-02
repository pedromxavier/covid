import api_lib
import datetime
import warnings
import sys
import os

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

## Default Block size
BLOCK_SIZE = 1024

## Processors
CPU_COUNT = os.cpu_count()

## Asynchronous matters
try:
    import aiohttp
    ASYNC_LIB = True
    del aiohttp
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
        del nest_asyncio
    except ImportError:
        JUPYTER_ASYNC_LIB = False
        warnings.warn('Falha ao importar bilioteca `nest_asyncio`. Requisições assíncronas indisponíveis no Jupyter Notebook.', category=ImportWarning, stacklevel=2)
ASYNC_MODE = ASYNC_LIB and (not IN_JUPYTER or JUPYTER_ASYNC_LIB)