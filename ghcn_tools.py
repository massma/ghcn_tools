"""
this module has tools for loading ghcn daily data
see https://www.ncdc.noaa.gov/ghcnd-data-access
all functions return pandas data frames

load station meta data file with the function:

load_ghcn_meta(filename)

and load daily data with:

load_ghcn_daily(filename)

The above funtion splits time by putting days in columns,
for a more traditional timeseries dataframe use:

load_ghcn_timeseries(filename)

But this will be more sparse and less space efficient
for most files.
"""
from calendar import monthrange
import pandas as pd
import numpy as np


# helper functions
def to_int(string, start_idx, end_idx):
    return int(trimmer(string, start_idx, end_idx))

def to_float(string, start_idx, end_idx):
    return float(trimmer(string, start_idx, end_idx))

def trimmer(string, start_idx, end_idx):
    return str(string[start_idx:end_idx]).lower()

FALSE_VALUES = {'str': "", 'float' : np.nan, 'int' : 0}
CONVERSIONS = {'str': trimmer, 'float' : to_float, 'int' : to_int}


# Load meta data file
def load_ghcn_meta(filename):
    """
    loads the station meta data file for ghcn-daily e.g.:
    https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt
    """
    data = [['id', 'lat', 'lon', 'elev', 'state', 'name', 'gsn_flag',
             'hcn_flag', 'wmo_id']]
    start_idxs = [0, 12, 21, 31, 38, 41, 72, 76, 80]
    end_idxs = [11, 20, 30, 37, 40, 71, 75, 79, 85]
    types = ['str', 'float', 'float', 'float', 'str',
             'str', 'str', 'str', 'int']
    f = open(filename, 'r')
    for line in f:
        length = len(line)
        new_data = []
        for typ, start, end in zip(types, start_idxs,
                                   end_idxs):
            if end < length:
                new_data.append(CONVERSIONS[typ](line, start, end))
            else:
                new_data.append(FALSE_VALUES[typ])
        data.append(new_data)
    f.close()
    return pd.DataFrame(data[1:], columns=data[0])


# load daily file
def load_year(line):
    return to_int(line, 11, 15)

def load_month(line):
    return to_int(line, 15, 17)

WEATHER_TYPE_LOOKUP = {'wt01' : 'wt_fog',
                       'wt02' : 'wt_heavy_fog',
                       'wt03' : 'wt_thunder',
                       'wt04' : 'wt_ice',
                       'wt05' : 'wt_hail',
                       'wt06' : 'wt_rime',
                       'wt07' : 'wt_dust_sand',
                       'wt08' : 'wt_smoke',
                       'wt09' : 'wt_blowing_snow',
                       'wt10' : 'wt_tornado',
                       'wt11' : 'wt_high_wind',
                       'wt12' : 'wt_blowing_spray',
                       'wt13' : 'wt_mist',
                       'wt14' : 'wt_drizzle',
                       'wt15' : 'wt_freezing_drizzle',
                       'wt16' : 'wt_rain',
                       'wt17' : 'wt_freezing_rain',
                       'wt18' : 'wt_snow',
                       'wt19' : 'wt_unknown_precip',
                       'wt21' : 'wt_ground_fog',
                       'wt22' : 'wt_ice_fog'}

def load_varname(line):
    varname = trimmer(line, 17, 21)
    if varname in WEATHER_TYPE_LOOKUP:
        return WEATHER_TYPE_LOOKUP[varname]
    return varname

def daily_number_postprocess(number, varname):
    if number == -9999:
        return np.nan
    if varname in ['prcp', 'tmax', 'tmin', 'awnd', 'evap', 'mdev',
                   'mdpr', 'mdtn', 'mdtx', 'mnpn', 'mxpn',
                   'tobs', 'wesd', 'wesf', 'wsf1', 'wsf2',
                   'wsf5', 'wsfg', 'wsfi', 'wsfm']:
        return number/10.
    return number

def load_days(line, varname, year, month):
    ndays = monthrange(year, month)[-1]
    def start_idx(day):
        return 21+day*8
    def end_idx(day):
        return start_idx(day)+5
    return [daily_number_postprocess(to_float(line,
                                              start_idx(day),
                                              end_idx(day)),
                                     varname)
            for day in range(ndays)]

def update_data(data, line):
    var = load_varname(line)
    year = load_year(line)
    month = load_month(line)
    new_data = load_days(line, var, year, month)
    data.append([year, month, var] + new_data)
    return data

def load_ghcn_daily(filename):
    """
    loads the daily data file for e.g.:
    https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/hcn/USC00011084.dly
    does this a little differently to keep time and space small,
    each row is a year, month, and variable, and integer column
    indices provide the day. If you do not want to split time
    over two axis, see transpose_ghcn
    """
    data = [['year', 'month', 'varname'] + list(range(1, 32))]
    f = open(filename, 'r')
    for line in f:
        data = update_data(data, line)
    f.close()
    return pd.DataFrame(data[1:], columns=data[0])


# useful formatting and postprocessing on daily files
def exchange_row_column(df):
    """helper function for transpose_ghcn"""
    year = df.loc[:, 'year'].iloc[0]
    month = df.loc[:, 'month'].iloc[0]
    days = monthrange(year, month)[-1]
    df = df.set_index('varname').loc[:, list(range(1, days+1))].transpose()
    df['year'] = year
    df['month'] = month
    df['day'] = df.index
    return df

def transpose_ghcn(df):
    """exchanges varnames for days"""
    df = df.groupby(['year', 'month']).apply(exchange_row_column)
    return df

def load_ghcn_timeseries(filename):
    """
    like load_ghcn_daily, but returns a timeseries with day
    month, year, and varname columns
    """
    return transpose_ghcn(load_ghcn_daily(filename))
