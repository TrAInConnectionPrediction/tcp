import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import datetime
from helpers import StationPhillip

from pyhafas import HafasClient
from pyhafas.profile import DBProfile

stations = StationPhillip()
hafas = HafasClient(DBProfile())

# location = hafas.locations()


journeys = hafas.journeys(
    origin=stations.get_eva(name='Tübingen Hbf', date=datetime.datetime.now()),
    destination=stations.get_eva(name='Köln Hbf', date=datetime.datetime.now()),
    date=datetime.datetime.now(),
)

print(journeys)