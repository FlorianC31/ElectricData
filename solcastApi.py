import requests
import json
from influxInterface import Influxdb
import timeStamp
import sys


class SolcastAPI:
    def __init__(self, config):
        self.api_url = config['SolcastAPI']['api_url']
        api_key = config['SolcastAPI']['api_key']
        self.ressource_id = config['SolcastAPI']['ressource_id']
        self.format = "json"
        self.influx_db = Influxdb(config)
        
        # En-têtes de requête avec la clé d'API
        self.headers = {
            "Authorization": f"Bearer {api_key}"
        }

    def getUrl(self, type):
        return self.api_url + '/' + self.ressource_id + '/' + type

    def getData(self, type, hours = 48):
        params = {
            "format": self.format,
            "hours": hours
        }
        
        
        measurement = "SOLCAST_" + type
        
        last_timestamp = self.influx_db.getLastTimestamp(measurement)
        if last_timestamp == None:
            last_timestamp = "2024-04-10 00:00:00"       
        last_timestamp = timeStamp.getTimestampFromStr(last_timestamp)
        
        if type == "historic":
            type = "estimated_actuals"
                        
        print(measurement, self.getUrl(type))
        
        response = requests.get(self.getUrl(type), headers=self.headers, params=params)
        if response.status_code == 200:
            # Affichage du JSON retourné
            results = response.json()[type]
            points = []
            
            with open(type + '.json', 'w') as new_json_file:
                json.dump(results, new_json_file, indent=4)
        
        #if True:
        #    points = []
        #    with open('forecasts.json', 'r') as json_file:
        #        results = json.load(json_file)
            
            for result in results:                
                timestamp = timeStamp.getTimestampFromStr(result['period_end'])
                
                # if the point is not yet in the influx table
                print(timestamp, last_timestamp)
                if timestamp > last_timestamp:
                    point = {
                        "measurement": measurement,
                        "time": timestamp,
                        "tags": {},
                        "fields": {}
                    }
                    
                    if type == "forecasts":
                        point['tags']['type'] = hours
                        point['fields']['estimated']  = int(result['pv_estimate'] * 1000)
                        point['fields']['estimated10']  = int(result['pv_estimate10'] * 1000)
                        point['fields']['estimated90']  = int(result['pv_estimate90'] * 1000)
                        
                    else:
                        point['fields']['power'] = int(result['pv_estimate'] * 1000)
                    
                    points.append(point)
                    #print(point)
                
            self.influx_db.writePoints(points)
                
            
        else:
            # Affichage du message d'erreur en cas d'échec de la requête
            print(f"Erreur lors de la requête : {response.status_code} - {response.reason}")
        
        
        
        #self.printResponse(response, "forecasts")
        
        
    def getHistoric(self):
        params = {
            "format": self.format
        }

        response = requests.get(self.getUrl("estimated_actuals"), headers=self.headers, params=params)
        
        self.printResponse(response, "estimated_actuals")
            
            
    def printResponse(self, response, type):
        if response.status_code == 200:
            # Affichage du JSON retourné
            datas = response.json()[type]
            
            for data in datas:
                print(data['period_end'], data['pv_estimate'])
            
        else:
            # Affichage du message d'erreur en cas d'échec de la requête
            print(f"Erreur lors de la requête : {response.status_code} - {response.reason}")


if __name__ == "__main__":
    
    #with open('/home/florian/enedis/config.json', 'r') as config_file:
    #    config = json.load(config_file)
    #
    #solcastAPI = SolcastAPI(config)
    #
    #solcastAPI.getData('historic')
    #solcastAPI.getData('forecasts', 24)
    #
    #exit(0)
    
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Utilisation : python solcastApi.py <type('historic' or 'forecasts')> <hours")
    else:
        
        with open('/home/florian/enedis/config.json', 'r') as config_file:
            config = json.load(config_file)
        
        solcastAPI = SolcastAPI(config)
        
        type = sys.argv[1]
        if len(sys.argv) == 2:
            # default value
            hours = 48
        else:
            hours = sys.argv[2]
        
        solcastAPI.getData(type, hours)
