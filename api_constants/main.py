import api_lib
import datetime

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