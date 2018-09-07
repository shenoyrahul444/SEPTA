import pandas as pd
import urllib.request, json
import os

def getDataFromURL(url):

    with urllib.request.urlopen(url) as url:
        # data = json.loads(url.read().decode())
        data = json.loads(url.read())['data']

        return data

if __name__ == "__main__":

    sources = [
        {
         "api":"/api/current/system/latest",
        "description":"Info on all currently running trains.",
        "file_name":"all_current_trains2.json"
        },

        {
            "api": "/api/current/system/latest/stats",
            "description": "latest_system_stats",
            "file_name":"latest_system_stats.json"
        },

        {
            "api": "/api/current/system/totals",
            "description": "list of the total lateness of all running trains, by hour over the last 7 days",
            "file_name":"all_trains_total_lateness.json"
        },

        {
            "api":"/api/current/system/totals",
            "description": "Total hourly lateness for 7 days",
            "file_name":"total_hourly_7_day_lateness.json"
        }
        # ,
        # {
        #     "api":"",
        #     "description": "",
        #     "file_name":""
        # },
    ]
    for source in sources:
        base = "https://www.septastats.com"
        data = getDataFromURL(base+source['api'])
        file_name = "data/"+source['file_name']

        with open(file_name , 'w') as outfile:
            json.dump(data, outfile,indent=2)
            print("Created %s data file" % (file_name))


