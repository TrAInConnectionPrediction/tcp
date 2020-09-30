import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import sqlalchemy
import pandas as pd
import numpy as np
from helpers.StationPhillip import StationPhillip
import datetime
from helpers.RtdRay import RtdRay
from data_analisys.delay import load_with_delay


if __name__ == '__main__':
    import fancy_print_tcp
    rtd = RtdRay()
    rtd_df = load_with_delay()
    ['ar_pt', 'dp_pt', 'station', 'f', 't', 'o', 'c', 'n', 'distance_to_start', 'distance_to_end']
