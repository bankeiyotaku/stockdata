import os
import mysql.connector as mysql
import pandas as pd
from dotenv import load_dotenv

import refinitiv.data as rd
from refinitiv.data.content import symbol_conversion

from queue import Queue
from threading import Thread
import pandas_market_calendars as mcal

import numpy as np
from datetime import datetime

import time
import math




def InterpolateCallback( df ):
    new_df = df.interpolate(method='time', limit_direction='both', limit_area="inside")
    return new_df




def TransformSeriesToRangeCallback( df ):
    result = df.apply(lambda iterator: ((iterator-iterator.min())/(iterator.max() - iterator.min())).round(2))
    #result = df.apply(TransformCol)
    return result

