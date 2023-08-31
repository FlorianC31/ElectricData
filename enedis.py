
# source : https://conso.boris.sh/exemples

import json
import requests
from datetime import timedelta, date, datetime
from influxInterface import Influxdb
import timeStamp


class Hp_hc:
    def __init__(self, config):
        self.config = config['HC_HP']
        self.periods = []
        self.time_zone = config['timeZone']

        for period in self.config:
            new_period = {} 
            new_period['start_date'] = timeStamp.getTimestampFromDate(period['start_date']).date()
            new_period['HC_HP_count'] = period['HC_HP_count']
            
            if (period['HC_HP_count']):
                new_period['HC'] = []
                for hc in period['HC']:
                    new_hc = {}
                    new_hc['start'] = timeStamp.getTimestampFromTime(hc['start'])
                    new_hc['end'] = timeStamp.getTimestampFromTime(hc['end'])
                    new_period['HC'].append(new_hc)
                new_period['saison_pleine'] = period['saison_pleine']

            self.periods.append(new_period)

    def getHcRatio(self, timestamp, interval):
        dateEnedis = timestamp.date()
        date0 = date(1900, 1, 1)
        end_time = datetime.combine(date0, timestamp.time())
        start_time = end_time - timedelta(minutes=interval)

        for period in reversed(self.periods):
            if period['start_date'] <= dateEnedis:
                if not period['HC_HP_count']:
                    return 0
                for hc in period['HC']:
                    try:
                        if (end_time < hc['start']):
                            return 0
                        elif (start_time < hc['start'] and end_time > hc['start']):
                            return (end_time - hc['start']) / (end_time - start_time)
                        elif (start_time > hc['start'] and end_time < hc['end']):
                            return 1
                        elif (start_time < hc['end'] and end_time > hc['end']):
                            return (hc['end'] - start_time) / (end_time - start_time)
                        elif (hc == period["HC"][-1]):
                            return 0
                    except TypeError:
                        print(end_time, hc['start'])
        return -1
    
    def isSaisonPleine(self, timestamp):
        date = timestamp.date()
        for period in reversed(self.periods):
            if period["start_date"] >= date:
                if not period['HC_HP_count']:
                    return False
                else:
                    return period['saison_pleine'][date.month - 1]
        return False


class EnedisApi:
    def __init__(self, config):
        self.config = config['consoAPI']
        self.usage_point_id = config['usagePointId']
        self.time_zone = config['timeZone']
        self.json_data = {}

        self.influx_db = Influxdb(config)
        self.headers = {
            'Authorization': 'Bearer ' + self.config['token'],
            'from': self.config['from']
        }
        self.source = self.config['directSource']
    
    def request(self):
        for measures_group in self.config['MeasuresGroups']:
            table_name = measures_group['table']
            self.json_data['table_name'] = {}

            for measure in measures_group:
                url = self.config['url'] + measure['url'] + "?prm=" + self.usage_point_id
                
                # Ecexute request
                response = requests.get(url, headers=self.headers)

                # Store request result
                measure_name = measure['name']
                self.json_data['table_name']['measure_name'] = response.json()

    def getData(self, type, measure):
        table_name = type + "_" + measure
        last_timestamp = self.influx_db.getLastTimestamp(table_name)
        start_date = timeStamp.getTimestampFromStr(last_timestamp, self.time_zone).date()
        end_date = timeStamp.getEndDate(start_date, 
                                        self.config['MeasuresGroups'][type]['historyDeadline']['type'],
                                        self.config['MeasuresGroups'][type]['historyDeadline']['value'])

        url = (self.config['url'] + 
               self.config['MeasuresGroups'][type][measure] + 
               "?prm=" + 
               self.usage_point_id + 
               "&start=" + 
               start_date.strftime('%Y-%m-%d') + 
               "&end=" + 
               end_date.strftime('%Y-%m-%d'))

        response = requests.get(url, headers=self.headers)
        return response.json()

    
    def getHourlyData(self, measure):
        
        json_data = self.getData("hourly", measure)

        points = []
        for reading in json_data['interval_reading']:
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
                    "value": int(reading['value']),
                    "interval_length": interval,
                }
            }
            
            points.append(point)

        self.influx_db.writePoints(points)


    def getDailyData(self, measure):
        
        json_data = self.getData("daily", measure)

        points = []
        for reading in json_data['interval_reading']:
            utc_timestamp = timeStamp.getTimestampFromStr(reading['date'], self.time_zone, "utc")
            point = {
                "measurement": "daily" + measure,
                "tags": {
                    "source": self.source
                },
                "time": utc_timestamp,
                "fields": {}
            }
            
            points.append(point)

        self.influx_db.writePoints(points)


class EnedisData:
    def __init__(self, config):
        self.config = config
        self.points = []

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
            point['fields']['value'] = int(value / 60 * interval)
        else:
            HC_ratio = hp_hc.getHcRatio(local_timestamp, interval)
            point['tags']['saison_pleine'] = hp_hc.isSaisonPleine(local_timestamp)
            point['fields']['HC_value'] = int(value / 60 * interval * HC_ratio)
            point['fields']['HP_value'] = int(value / 60 * interval * (1 - HC_ratio))
        point['fields']['interval_length'] = int(interval)
        if not  isinstance(int(interval), int):
            print(local_timestamp, interval, int(interval), isinstance(int(interval), int))
        
        self.points.append(point)


    def addDailyPoint(self, measurement, enedis_timestamp, basse_hc, basse_hp = None, pleine_hc = None, pleine_hp = None):
        utc_timestamp = timeStamp.getTimestampFromStr(enedis_timestamp, config['timeZone'], "utc")

        point = {
            "measurement": measurement,
            "tags": {
                "source": config['enedisHistoricCsv']['directSource']
            },
            "time": utc_timestamp,
            "fields": {}
        }

        if measurement == "daily_consommation":
            point['fields']['basse_hc'] = basse_hc
            point['fields']['basse_hp'] = basse_hp
            point['fields']['pleine_hc'] = pleine_hc
            point['fields']['pleine_hp'] = pleine_hp
        else:
            point['fields']['value'] = basse_hc

        self.points.append(point)


    def insertIntoInflux(self):
        # Insert points series in influxdb
        influx_db = Influxdb(self.config)
        influx_db.writePoints(self.points)
        self.points.clear()

                


def enedisHistoricHourlyCsv(config):
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

        enedis_Data.addHourlyPoint("hourly_consommation", row_data[0], int(row_data[1]), row_data[2], int(row_data[5]), hp_hc)
        if (row_data[3] != "" ):
            enedis_Data.addHourlyPoint("hourly_production", row_data[0], int(row_data[3]), row_data[4], int(row_data[5]), None)
    enedis_Data.insertIntoInflux()
        

def enedisHistoricDailyCsv(config):
    with open(config['enedisHistoricCsv']['daily'], 'r', newline='') as csv_file:
        csv_data = csv_file.readlines()

    first_row = True
    enedis_Data = EnedisData(config)

    for row in csv_data:
        if first_row:
            first_row = False
            continue

        row_data = row.split(config['enedisHistoricCsv']['separator'])

        enedis_Data.addDailyPoint("daily_consommation", row_data[0], int(row_data[2]), int(row_data[3]), int(row_data[4]), int(row_data[5]))
        if (row_data[1] != "" ):
            enedis_Data.addDailyPoint("daily_production", row_data[0], int(row_data[1]))
    enedis_Data.insertIntoInflux()



if __name__ == '__main__':

    # Reading of config file
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)


    enedisHistoricHourlyCsv(config)
    #enedisHistoricDailyCsv(config)

    enedis_Api = EnedisApi(config)
    #enedis_Api.getDailyData("production")
    #enedis_Api.getDailyData("production")
    #enedis_Api.getDailyData("production")
    
    enedis_Api.getHourlyData("production")
    enedis_Api.getHourlyData("consommation")
    enedis_Api.getHourlyData("production")
    enedis_Api.getHourlyData("consommation")
    enedis_Api.getHourlyData("production")
    enedis_Api.getHourlyData("consommation")
