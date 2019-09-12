from pathlib import Path
import pandas as pd
import logging
from pvlib.location import Location
from irradiance_synth import IrradianceDataset

log = logging.getLogger(__name__)

DATASETS_PATH = Path('datasets')

def _find_prefixed_dat_files(prefix):
    return list(DATASETS_PATH.glob(f"{prefix}_*.dat"))

def load_alice_5s():
    log.info("Reading Alice Springs 5-second irradiance file...")
    loc = Location(-23.7624, 133.8754, altitude=580.0, tz="Australia/Darwin", name='Alice Springs')
    df = pd.concat([
        pd.read_csv(_find_prefixed_dat_files("ASP"),
            index_col='Time', parse_dates=True
        )
    ])[['GHI', 'DNI']].asfreq('5S')
    df.index = df.index.tz_localize(loc.tz)
    df.columns = ['ghi', 'dni']
    return IrradianceDataset(df, location=loc)

def load_alice_5m():
    log.info("Reading Alice 5-minute irradiance data...")
    loc = Location(-23.7624, 133.8754, altitude=580.0, tz="Australia/Darwin", name='Alice Springs')
    df = pd.read_csv(DATASETS_PATH / '101-Site_DKA-WeatherStation.csv.gz', index_col='Timestamp', parse_dates=True)[[
        'DKA.WeatherStation - Global Horizontal Radiation (W/m²)',
        'DKA.WeatherStation - Diffuse Horizontal Radiation (W/m²)'
    ]]
    df.index = df.index.tz_localize(loc.tz)
    df.columns = ['ghi', 'dni']
    df = df.asfreq('5T')
    return IrradianceDataset(df, location=loc)

def load_darwin_5s():
    log.info("Reading Darwin 5-second irradiance file...")
    loc = Location(-12.4417, 130.9215, altitude=10.0, tz="Australia/Darwin", name='Darwin')
    df = pd.concat([
        pd.read_csv(_find_prefixed_dat_files("DRW"),
            index_col='Time', parse_dates=True
        )
    ])[['GHI', 'DNI']].asfreq('5S')
    df.index = df.index.tz_localize(loc.tz)
    df.columns = ['ghi']
    return IrradianceDataset(df, location=loc)

def load_katherine_5s():
    log.info("Reading Katherine 5-second irradiance file...")
    loc = Location(-14.4747, 132.3050, altitude=108.0, tz="Australia/Darwin", name='Katherine')
    df = pd.concat([
        pd.read_csv(_find_prefixed_dat_files("KTR"),
            index_col='Time', parse_dates=True
        )
    ])[['GHI', 'DNI']].asfreq('5S')
    df.index = df.index.tz_localize(loc.tz)
    df.columns = ['ghi', 'dni']
    return IrradianceDataset(df, location=loc)
    
def load_hawaii_3s_csv():
    # TODO: Don't actually need Dask for this. Should remove the dependency.
    from dask.diagnostics import ProgressBar
    import dask.dataframe as ddf
    loc = Location(21.31034, -158.08675, tz='HST', altitude=11)
    df = ddf.read_csv(DATASETS_PATH / 'hawaii_3s' / '*.txt', header=None)
    df.columns = ['S', 'Y', 'DOY', 'HHMM', 'ghi', 'dhi', 'dni']

    log.info("Reading Hawaii 3-second irradiance CSV files (takes up to 10 minutes)...")

    def get_datetime_index(df):
        str_datetime = df[['Y', 'DOY', 'HHMM', 'S']].astype(str).apply(lambda x: ' '.join(x), axis=1)
        datetimes = pd.to_datetime(str_datetime, format='%Y %j %H%M %S')
        return datetimes
    with ProgressBar():
        datetimes = df.map_partitions(get_datetime_index, meta=pd.Series([pd.to_datetime('2019-01-01 00:00:00')]))
        df = df.set_index(datetimes).compute().asfreq('3S')
    df.index = df.index.tz_localize(loc.tz)
    return IrradianceDataset(df[['ghi', 'dhi', 'dni']], location=loc)

def create_hawaii_3s_hdf():
    data = load_hawaii_3s_csv()

    # load the lazily calculated data
    data.k

    # HDF tries to store the metadata, which would be quite cool,
    # but not entirely necessary for my use-case. So I'm converting
    # the IrradianceDataset to a DataFrame before saving.
    pd.DataFrame(data).to_hdf(DATASETS_PATH / 'hawaii_3s.h5', key='data')

def load_hawaii_3s_hdf():
    log.info("Reading Hawaii 3-second irradiance HDF file")
    data = pd.read_hdf(DATASETS_PATH / 'hawaii_3s.h5', key='data')
    loc = Location(21.31034, -158.08675, tz='HST', altitude=11)
    return IrradianceDataset(data, location=loc)

def load_hawaii_3s():
    if not (DATASETS_PATH / 'hawaii_3s.h5').exists():
        create_hawaii_3s_hdf()
    return load_hawaii_3s_hdf()
        