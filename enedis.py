
# source : https://conso.boris.sh/exemples

import json
import requests
from datetime import timedelta, datetime
from influxInterface import Influxdb
import timeStamp
from hp_hc import Hp_hc
from influx2csv import influx2csv
from checkEnedisData import checkEnedisData

class EnedisApi:
    def __init__(self, config):
        self.enedis_source = config['enedisSource']
        self.config = config[self.enedis_source]
        self.usage_point_id = config['usagePointId']
        self.time_zone = config['timeZone']
        self.json_data = {}
        self.hp_hc = Hp_hc(config)

        self.influx_db = Influxdb(config)

        if self.enedis_source == "consoBorisSh":
            self.headers = {
                'Authorization': 'Bearer ' + self.config['token'],
                'from': self.config['from']
            }
        else:            
            self.headers = {'Authorization': self.config['token']}

        self.source = self.config['directSource']
    
    def request(self):
        for measures_group in self.config['MeasuresGroups']:
            table_name = measures_group['table']
            self.json_data[table_name] = {}

            for measure in measures_group:
                url = self.config['url'] + measure['url'] + "?prm=" + self.usage_point_id
                
                # Ecexute request
                response = requests.get(url, headers=self.headers)

                # Store request result
                measure_name = measure['name']
                self.json_data[table_name]['measure_name'] = response.json()    

    def getData(self, type, measure):
        
        table_name = type + "_" + measure
        last_timestamp = self.influx_db.getLastTimestamp(table_name)

        if last_timestamp == None:
            last_timestamp = self.getOlderAvailableDate(type)

        start_date = timeStamp.getTimestampFromStr(last_timestamp, self.time_zone).date() + timedelta(days = 1)

        if start_date == datetime.now().date():
            return {}

        end_date = timeStamp.getEndDate(start_date, self.config['MeasuresGroups'][type]['historyDeadline'])
        

        if self.enedis_source == "consoBorisSh":
            url = (self.config['url'] + 
                self.config['MeasuresGroups'][type][measure] + 
                "?prm=" + 
                self.usage_point_id + 
                "&start=" + 
                start_date.strftime('%Y-%m-%d') + 
                "&end=" + 
                end_date.strftime('%Y-%m-%d'))

        else:
            url = (self.config['url'] + 
                self.config['MeasuresGroups'][type][measure] + 
                "/" + 
                self.usage_point_id + 
                "/start/" + 
                start_date.strftime('%Y-%m-%d') + 
                "/end/" + 
                end_date.strftime('%Y-%m-%d'))
            
        print(url)
        input("Press Enter to continue...")
        response = requests.get(url, headers=self.headers)
        return response.json()
    

    def getOlderAvailableDate(self, type):
        '''get the older available date from Enedis API'''

        print("type:", type)
        timestamp = datetime.now().date()
        timestamp -= timedelta(days = self.config['MeasuresGroups'][type]['historyDeadline'])
        print("getOlderAvailableDate:", timestamp.strftime('%Y-%m-%d %H:%M:%S'))
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')

    
    def getHourlyData(self, measure):        
        points = []

        while True:
            json_data = self.getData("hourly", measure)

            if len(json_data) == 0 or "status" in json_data:
                break
            
            if self.enedis_source == "consoBorisSh":
                readings = json_data['interval_reading']
            else:
                readings = json_data['meter_reading']['interval_reading']

            for reading in readings:
                print("getHourlyData", reading['date'])
                utc_timestamp = timeStamp.getTimestampFromStr(reading['date'], self.time_zone, "utc")

                interval = int(reading['interval_length'][2:4])
                local_timestamp = timeStamp.getTimestampFromStr(reading['date'], config['timeZone'])
                local_start_timestamp = local_timestamp - timedelta(minutes=interval)
                date = local_start_timestamp.date()
                
                point = {
                    "measurement": "hourly_" + measure,
                    "tags": {
                        "source": self.source,
                        "date": date
                    },
                    "time": utc_timestamp,
                    "fields": {
                        "interval_length": interval
                    }
                }

                if (measure == "consommation"):
                    hour_hc_ratio = self.hp_hc.getHourHcRatio(local_timestamp, interval)
                    point['tags']['saison_pleine'] = self.hp_hc.isSaisonPleine(utc_timestamp)
                    point['fields']['HC_value'] = int(round(int(reading['value']) * hour_hc_ratio / 60 * interval, 0))
                    point['fields']['HP_value'] = int(round(int(reading['value']) * (1 - hour_hc_ratio) / 60 * interval, 0))
                else:
                    point['fields']['value'] = int(round(int(reading['value']) / 60 * interval, 0))
        
                points.append(point)

            self.influx_db.writePoints(points)


    def getDailyData(self, measure):        
        points = []
        
        while True:
            json_data = self.getData("daily", measure)
            #with open("temp.json", 'r') as temp_json:
            #    json_data = json.load(temp_json)

            if len(json_data) == 0 or "status" in json_data:
                break

            if self.enedis_source == "consoBorisSh":
                readings = json_data['interval_reading']
            else:
                readings = json_data['meter_reading']['interval_reading']

            for reading in readings:
                utc_timestamp, local_timestamp = timeStamp.getTimestampFromDate(reading['date'], self.time_zone)
                
                day_hc_ratio = self.getDayHcRatio(utc_timestamp)
                if day_hc_ratio == None:
                    break

                point = {
                    "measurement": "daily_" + measure,
                    "tags": {
                        "source": self.source,
                    },
                    "time": utc_timestamp,
                    "fields": {
                    }
                }

                if (measure == "consommation"):
                    if self.hp_hc.isSaisonPleine(utc_timestamp):
                        saison = "pleine"
                        not_saison = "basse"
                    else:
                        saison = "basse"
                        not_saison = "pleine"
                    point['fields'][saison + '_hc'] = int(round(int(reading['value']) * day_hc_ratio, 0))
                    point['fields'][saison + '_hp'] = int(round(int(reading['value']) * (1 - day_hc_ratio), 0))
                    point['fields'][not_saison + '_hc'] = 0
                    point['fields'][not_saison + '_hp'] = 0
                else:
                    point['fields']['value'] = int(reading['value'])
                
                points.append(point)

            self.influx_db.writePoints(points)


    # @brief getHcRatio: get the ratio of heures creuses consumption on totoal consumption of a day
    # @param end_day_ts : timestamp of the end of the day
    # @return : ratio between 0.0 and 1.0
    def getDayHcRatio(self, end_day_ts):
        if not self.hp_hc.isHpHc(end_day_ts):
            return 0.0

        start_day_ts = end_day_ts - timedelta(days = 1)
        query = f"SELECT SUM(HC_value), SUM(HP_value) FROM ENEDIS_hourly_consommation WHERE time > '" + timeStamp.timestamp2str(start_day_ts) + "' AND time <= '" + timeStamp.timestamp2str(end_day_ts) + "'"
        try:
            result = self.influx_db.query(query)[0]
            sum_hc = result['sum']
            sum_hp = result['sum_1']
            return (sum_hc / (sum_hc + sum_hp))
        except IndexError:
            return None


class EnedisData:
    def __init__(self, config):
        self.config = config
        self.points = []        
        self.influx_db = Influxdb(config)

    def addHourlyPoint(self, measurement, enedis_timestamp, value, sourceType, interval, hp_hc):
        utc_timestamp = timeStamp.getTimestampFromStr(enedis_timestamp, config['timeZone'], "utc")
        
        local_timestamp = timeStamp.getTimestampFromStr(enedis_timestamp, config['timeZone'])
        local_start_timestamp = local_timestamp - timedelta(minutes=interval)
        date = local_start_timestamp.date()

        if (sourceType == "Extrapoled"):
            source = config['enedisHistoricCsv']['extrapoledSource']
        else:                
            source = config['enedisHistoricCsv']['directSource']


        point = {
            "measurement": measurement,
            "tags": {
                "source": source,
                "date": date
            },
            "time": utc_timestamp,
            "fields": {}
        }
        
        if(hp_hc == None):
            point['fields']['value'] = int(round(value / 60 * interval, 0))
        else:
            HC_ratio = hp_hc.getHourHcRatio(local_timestamp, interval)
            point['tags']['saison_pleine'] = hp_hc.isSaisonPleine(local_timestamp)
            point['fields']['HC_value'] = int(round(int(value) / 60 * interval * HC_ratio, 0))
            point['fields']['HP_value'] = int(round(int(value) / 60 * interval * (1 - HC_ratio), 0))
        point['fields']['interval_length'] = int(interval)
        if not isinstance(int(interval), int):
            print(local_timestamp, interval, int(interval), isinstance(int(interval), int))
        
        self.points.append(point)


    def addDailyPoint(self, measurement, enedis_timestamp, basse_hc, basse_hp = None, pleine_hc = None, pleine_hp = None):

        utc_timestamp = timeStamp.getTimestampFromStr(enedis_timestamp, config['timeZone'], "utc") + timedelta(days = 1)


        point = {
            "measurement": measurement,
            "tags": {
                "source": config['enedisHistoricCsv']['directSource']
            },
            "time": utc_timestamp,
            "fields": {}
        }

        if measurement == "ENEDIS_daily_consommation":
            point['fields']['basse_hc'] = basse_hc
            point['fields']['basse_hp'] = basse_hp
            point['fields']['pleine_hc'] = pleine_hc
            point['fields']['pleine_hp'] = pleine_hp
        else:
            point['fields']['value'] = basse_hc

        self.points.append(point)


    def insertIntoInflux(self):
        # Insert points series in influxdb
        self.influx_db.writePoints(self.points)
        self.points.clear()


    def clearTable(self, table):
        self.influx_db.clearTable(table)
        print("Delete all date from", table)


    def clearAllTables(self):
        self.influx_db.clearAllTables()
        print("Delete all tables")
                


def enedisHistoricHourlyCsv(config):
    print("enedisHistoricHourlyCsv")
    with open(config['enedisHistoricCsv']['hourly'], 'r', newline='') as csv_file:
        csv_data = csv_file.readlines()

    first_row = True
    enedis_Data = EnedisData(config)
    hp_hc = Hp_hc(config)

    for row in csv_data:
        if first_row:
            first_row = False
            continue

        row_data = row.split(config['enedisHistoricCsv']['separator'])

        enedis_Data.addHourlyPoint("ENEDIS_hourly_consommation", row_data[0], int(row_data[1]), row_data[2], int(row_data[5]), hp_hc)
        if (row_data[3] != "" ):
            enedis_Data.addHourlyPoint("ENEDIS_hourly_production", row_data[0], int(row_data[3]), row_data[4], int(row_data[5]), None)
    enedis_Data.insertIntoInflux()
        

def enedisHistoricDailyCsv(config):
    print("enedisHistoricDailyCsv")
    with open(config['enedisHistoricCsv']['daily'], 'r', newline='') as csv_file:
        csv_data = csv_file.readlines()

    first_row = True
    enedis_Data = EnedisData(config)

    for row in csv_data:
        if first_row:
            first_row = False
            continue

        row_data = row.split(config['enedisHistoricCsv']['separator'])

        enedis_Data.addDailyPoint("ENEDIS_daily_consommation", row_data[0], int(row_data[2]), int(row_data[3]), int(row_data[4]), int(row_data[5]))
        if (row_data[1] != "" ):
            enedis_Data.addDailyPoint("ENEDIS_daily_production", row_data[0], int(row_data[1]))
    enedis_Data.insertIntoInflux()



if __name__ == '__main__':

    # Reading of config file
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    enedis_Data = EnedisData(config)
    
    enedis_Data.clearAllTables()

    enedisHistoricHourlyCsv(config)
    enedisHistoricDailyCsv(config)

    enedis_Api = EnedisApi(config)
    enedis_Api.getHourlyData("consommation")
    enedis_Api.getHourlyData("production")

    enedis_Api.getDailyData("consommation")
    enedis_Api.getDailyData("production")

    #influx2csv("database")
    #checkEnedisData()
