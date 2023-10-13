import requests
import pandas as pd
import numpy as np
from zipfile import ZipFile
from bs4 import BeautifulSoup
from io import BytesIO
import hickle
from tqdm import tqdm
from functools import reduce
import os


class WeatherData:
    def __init__(self):
        self.common = None
        self.city_codes = None

    def list_zipfiles(self, file_extension='zip'):
        list_of_urls = [
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/historical/",
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/recent/",
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/moisture/historical/",
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/moisture/recent/",
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/",
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/",
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/historical/",
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/recent/",
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical/",
            "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/recent/"
        ]

        self.zip_files_urls = []

        for url in tqdm(list_of_urls, desc='save urls from server'):
            page = requests.get(url).text
            soup = BeautifulSoup(page, 'html.parser')
            file_urls = [url + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(file_extension)]
            self.zip_files_urls.extend(file_urls)

        return self.zip_files_urls

    def filter_zipfiles(self):
        zip_hist = [x for x in self.zip_files_urls if 'hist' in x]
        zip_curr = [x for x in self.zip_files_urls if 'akt' in x]

        hist_selected = [x for x in zip_hist if 'air_temp' in x or 'precipitation' in x]
        curr_selected = [x for x in zip_curr if 'air_temp' in x or 'precipitation' in x]

        hist_ids = [x.split('_')[-2] for x in hist_selected]
        curr_ids = [x.split('_')[-2] for x in curr_selected]

        common_ids = list(set(hist_ids).intersection(curr_ids))

        self.common = common_ids
        
        self.filtered_urls = list(hist_selected + curr_selected)

        return self.filtered_urls

    def unpack_zipfiles(self, path_download_folder):
        path_download_folder = 'unpacked_zipfiles'
        self.path = path_download_folder

        self.filter_zipfiles()

        for url in tqdm(self.filtered_urls, desc='download and extract zipfiles'):
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with ZipFile(BytesIO(r.content)) as zip_file:
                    zip_file.extractall(self.path, members=[member for member in zip_file.namelist() if member.endswith('.txt')])

        print('\nDownloaded and extracted data to:', self.path)

    def import_weather_data(self, path):
        self.path = path

        weather_codes = {
            'tu': 'air_temp',
            'ff': 'wind',
            'rr': 'rain',
            'sd': 'sun',
            'n': 'cloudiness'
        }

        self.weather_dict = {}

        for file in tqdm(os.listdir(self.path), desc='load files from disk'):
            filename = os.fsdecode(file)
            filename = filename.replace('produkt', '').replace('stunde', '').replace('.txt', '').split('_')

            if filename[2].startswith('18') and filename[3].startswith('2018'):
                filename[2] = 'hist'
            elif filename[2].startswith('19') and filename[3].startswith('2018'):
                filename[2] = 'hist'
            elif filename[2].startswith('20') and filename[3].startswith('2018'):
                filename[2] = 'hist'
            elif filename[2].startswith('2018') and filename[3].startswith('2019'):
                filename[2] = 'current'
            elif filename[2].startswith('2018') and filename[3].startswith('2020'):
                filename[2] = 'current'
            else:
                print('Problem with', filename[4])

            filename.pop(3)
            filename = '_'.join(filename)

            for key in weather_codes.keys():
                filename = filename.replace(key, weather_codes[key])

            df = pd.read_csv(os.path.join(self.path, file), sep=';', usecols=[0, 1, 3, 4])
            df = df.loc[df['MESS_DATUM'] > 2014123123]

            self.weather_dict[filename] = df

        return self.weather_dict

    def merge_weather_data(self, data_to_merge):
        self.weather_dict = data_to_merge
        self.weather_merged = {}
        self.problem_cities = []

        try:
            self.common
        except AttributeError:
            list_of_urls = [
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/recent'
            ]
            self.list_zipfiles(list_of_urls)
            self.filter_zipfiles()

        try:
            self.city_codes
        except AttributeError:
            self.create_city_dict()

        for city in tqdm(self.city_codes, desc="merge dataframes into one per city"):
            dfs_weather_features = []
            dfs_wind = {}
            dfs_sun = {}
            dfs_cloudiness = {}
            dfs_rain = {}
            dfs_air_temp = {}

            for key, df in self.weather_dict.items():
                if 'wind' in key and city in key:
                    dfs_wind[key] = df
                    if len(dfs_wind) == 2:
                        wind = pd.concat(dfs_wind, sort=False)
                        dfs_weather_features.append(wind)

                elif 'sun' in key and city in key:
                    dfs_sun[key] = df
                    if len(dfs_sun) == 2:
                        sun = pd.concat(dfs_sun, sort=False)
                        dfs_weather_features.append(sun)

                elif 'cloudiness' in key and city in key:
                    dfs_cloudiness[key] = df
                    if len(dfs_cloudiness) == 2:
                        cloudiness = pd.concat(dfs_cloudiness, sort=False)
                        dfs_weather_features.append(cloudiness)

                elif 'rain' in key and city in key:
                    dfs_rain[key] = df
                    if len(dfs_rain) == 2:
                        rain = pd.concat(dfs_rain, sort=False)
                        dfs_weather_features.append(rain)

                elif 'air_temp' in key and city in key:
                    dfs_air_temp[key] = df
                    if len(dfs_air_temp) == 2:
                        air_temp = pd.concat(dfs_air_temp, sort=False)
                        dfs_weather_features.append(air_temp)

            for i, df in enumerate(dfs_weather_features):
                df = df.droplevel(0)
                df = df[~df['MESS_DATUM'].duplicated()]
                df = df[df.columns.drop(list(df.filter(regex='eor')))]
                dfs_weather_features[i] = df

            try:
                df = reduce(lambda left, right: pd.merge(left, right, on=['MESS_DATUM', 'STATIONS_ID']), dfs_weather_features)
                self.weather_merged[city] = df
            except Exception as e:
                print('Problem with:', city, e)
                self.problem_cities.append(city)

        return self.weather_merged

    def create_city_dict(self):
        try:
            self.common
        except AttributeError:
            list_of_urls = [
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/recent'
            ]
            self.list_zipfiles(list_of_urls)
            self.filter_zipfiles()

        common_ids = list(self.common)
        with open('./data/stations_id.txt', 'r') as f:
            lines = f.readlines()
        lines = lines[2:]

        self.city_codes = {}
        for line in lines:
            parts = line.replace(' ', ' ').replace('\n', '').split()
            if parts[0] in common_ids:
                station_id = parts[0]
                station_name = ' '.join(parts[6:])
                self.city_codes[station_id] = station_name

    def clean_weather_data(self, data_to_clean):
        self.weather_merged = data_to_clean
        self.weather_cleaned = {}

        try:
            self.common
        except AttributeError:
            list_of_urls = [
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/cloudiness/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/sun/recent',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/historical',
                'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/recent'
            ]
            self.list_zipfiles(list_of_urls)
            self.filter_zipfiles()

            try:
                self.city_codes
            except AttributeError:
                self.create_city_dict()

        for key, df in tqdm(self.weather_merged.items(), desc="format data"):
            df.columns = df.columns.str.lower().str.strip()
            df.rename(columns={'stations_id': 'stations_name'}, inplace=True)
            temp_city_codes = {int(k): v for k, v in self.city_codes.items()}
            df['stations_name'].replace(temp_city_codes, inplace=True)

            weather_codes = {
                'v_n': 'cloudiness',
                'rf_tu': 'rel_humidity',
                'tt_tu': 'air_temp',
                'f': 'wind_speed',
                'd': 'wind_direction',
                'sd_so': 'sunshine',
                'r1': 'rain',
            }
            df.rename(columns=weather_codes, inplace=True)

            df['date_time'] = pd.to_datetime(df['mess_datum'], format='%Y%m%d%H')
            df = df.set_index('date_time')
            df.sort_index(ascending=True, inplace=True)
            try:
                df.drop(['mess_datum', 'v_n_i', 'rs_ind'], axis=1, inplace=True)
            except:
                pass

            self.weather_cleaned[key] = df

        return self.weather_cleaned

    def interpolate_weather_data(self, data_to_interpolate, false_values):
        self.weather_cleaned = data_to_interpolate
        self.weather_final = {}

        for key, df in tqdm(self.weather_cleaned.items(), desc="interpolate data"):
            df = df.replace({false_values: np.NaN}, inplace=False)
            df = df.asfreq(freq='H')
            df = df.interpolate(method="time")
            df['stations_name'] = df['stations_name'].fillna(method="ffill")
            self.weather_final[key] = df

        return self.weather_final

def download_and_process_data(DWD):
    path_savepoint1 = 'savepoint1'
    path_savepoint2 = 'savepoint2'

    # Ensure directories exist
    if not os.path.exists(path_savepoint1):
        os.makedirs(path_savepoint1)
    if not os.path.exists(path_savepoint2):
        os.makedirs(path_savepoint2)

    DWD.list_zipfiles()
    DWD.unpack_zipfiles(path_savepoint1)
    data_to_clean = DWD.import_weather_data(path_savepoint1)
    data_to_interpolate = DWD.clean_weather_data(data_to_clean)
    DWD.interpolate_weather_data(data_to_interpolate, -999)

    # Save processed data to savepoint 1
    with open(os.path.join(path_savepoint1, 'data_savepoint1.hkl'), 'wb') as f:
        hickle.dump(DWD.weather_final, f)

    # Merge data for savepoint 2
    DWD.merge_weather_data(data_to_clean)

    # Save merged data to savepoint 2
    with open(os.path.join(path_savepoint2, 'data_savepoint2.hkl'), 'wb') as f:
        hickle.dump(DWD.weather_merged, f)

    print("Data processing complete. Savepoints 1 and 2 created.")

def load_data_from_savepoint1(DWD):
    path_savepoint1 = 'savepoint1'

    if not os.path.exists(path_savepoint1):
        print("Savepoint 1 doesn't exist. Please run data processing first.")
        return

    # Load data from savepoint 1
    with open(os.path.join(path_savepoint1, 'data_savepoint1.hkl'), 'rb') as f:
        DWD.weather_final = hickle.load(f)

    print("Data loaded from savepoint 1.")

def load_data_from_savepoint2(DWD):
    path_savepoint2 = 'savepoint2'

    if not os.path.exists(path_savepoint2):
        print("Savepoint 2 doesn't exist. Please run data processing first.")
        return

    # Load data from savepoint 2
    with open(os.path.join(path_savepoint2, 'data_savepoint2.hkl'), 'rb') as f:
        DWD.weather_merged = hickle.load(f)

    print("Data loaded from savepoint 2.")

def init_app():
    DWD = WeatherData()
    choice = input("Select an option (1/2/3/none):\n"
                    "(1) Download and process data\n"
                    "(2) Load from savepoint 1\n"
                    "(3) Load from savepoint 2\n"
                    "Choice: ").strip()

    options = {
        '1': download_and_process_data,
        '2': load_data_from_savepoint1,
        '3': load_data_from_savepoint2,
    }

    if choice in options:
        options[choice](DWD)
    else:
        print("Exit. Please provide a valid option.")

if __name__ == "__main__":
    DWD = WeatherData()
    init_app()
