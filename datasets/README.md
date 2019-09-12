# Obtaining the datasets

The script `datasets.py` provides methods for reading publically available irradiance data from a few difference sources. Each function returns the data as an `irradiance_synth.IrradianceDataset` object. However, the datasets themselves are not included in this repository.

To use the example datasets, please follow the instructions below.

## Oahu Solar Measurement Grid 3-second Data

1. Visit https://midcdmz.nrel.gov/apps/sitehome.pl?site=OAHUGRID
2. Download the "3-second RSR 3-Component Irradiance" file(s)
3. Unzip the contents into the folder "datasets/hawaii_3s"
4. Use `datasets.load_hawaii_3s()` to read the data. Note that on the first run, this function will build a HDF5 format file containing the entire dataset, which may take 10 minutes. After this, reading the data will take a few seconds.

## NTSR Project 5-second Data

1. Visit http://dkasolarcentre.com.au/download?location=nt-solar-resource
2. In the "Download by Year" area, choose one or more of the `.dat` files
3. Place the downloaded files in the `datasets/` folder
4. To read the data, choose the corresponding function:
    * `datasets.load_alice_5s()`
    * `datasets.load_katherine_5s()`
    * `datasets.load_darwin_5s()`
    * `datasets.load_tennant_5s()`

## Desert Knowledge Australia 5-Minute Data

1. Visit http://dkasolarcentre.com.au/download?location=alice-springs
2. Find the Historical Weather Data download button
3. Place the downloaded files in the `datasets/` folder
4. Use `datasets.load_alice_15m()` to read the data
