#!/usr/bin/env python3


import requests
import pandas as pd
import numpy as np
import os
import datetime
import csv
from io import StringIO

class Collector:
    def __init__(self):
        this_dir =  os.path.dirname(os.path.realpath(__file__))
        self.dfs_dir = this_dir + "/data"
        self.dfs = {}
        for f in os.listdir(self.dfs_dir):
            dotidx = f.index('.')
            location = f[0:dotidx]
            self.dfs[location] = pd.read_csv(self.dfs_dir+"/"+f)
            self.dfs[location].set_index('Date time', inplace=True)
            self.dfs[location].index = pd.to_datetime(self.dfs[location].index)

        locations_fpath = this_dir + "/" + "locations.csv"
        if not os.path.isfile(locations_fpath):
            raise(FileNotFoundError(locations_fpath+" not found"))
        else:
            f = open(locations_fpath, "r")
            self.locations = [row.strip() for row in f]

        self.apikey = open(this_dir + "/rapidapi_apikey.txt", "r").read().strip()

    def get_weather_data(self, location):
        url = "https://visual-crossing-weather.p.rapidapi.com/forecast"
        querystring = {"location":location,"aggregateHours":"24","shortColumnNames":"0","unitGroup":"metric","contentType":"csv"}
        headers = {
            'x-rapidapi-host': "visual-crossing-weather.p.rapidapi.com",
            'x-rapidapi-key': self.apikey,
            }
        response = requests.request("GET", url, headers=headers, params=querystring)
        f = StringIO(response.text)
        reader = csv.reader(f, delimiter=',')
        data = [row for row in reader]
        columns = data[0]
        df = pd.DataFrame(data[1:], columns=columns)
        
        #df = pd.read_csv("raw_data.csv")

        df['Date time'] = pd.to_datetime(df['Date time'])
        df.set_index('Date time', inplace=True, drop=True)
        
        print("weather data requested")

        return df

    # updates self.df with forecast of tomorrow if it hasn't been collected already
    def update(self, location):
        today = datetime.datetime.today()
        tomorrow = today + datetime.timedelta(days=1)
        tomorrow = pd.Timestamp(tomorrow.date())

        if location not in self.dfs.keys():
            data = self.get_weather_data(location)
            self.dfs[location] = data[['Wind Speed','Wind Gust', 'Wind Chill']].loc[[tomorrow]]
        else:
            df = self.dfs[location]
            if tomorrow not in df.index:
                data = self.get_weather_data(location)
                tmrw = data.loc[tomorrow]
                df.loc[tomorrow.date()] = tmrw[['Wind Speed', 'Wind Gust', 'Wind Chill']]

    def save_changes(self):
        for location in self.dfs.keys():
            self.dfs[location].to_csv(self.dfs_dir+"/"+location+".csv")
        #self.df.to_csv(self.df_fpath, index=False)

if __name__ == "__main__":
    c = Collector()
    
    for location in c.locations:
        c.update(location)
    
    c.save_changes()
