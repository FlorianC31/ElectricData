import requests
import json
import csv
from influxInterface import Influxdb
from datetime import datetime
from influx2csv import influx2csv

def remove_lists_from_dict(input_dict):
    return {key: value for key, value in input_dict.items() if not isinstance(value, list)}


def getZoneData(api_url, zone):
    try:
        response = requests.get(api_url + str(zone))
        response.raise_for_status()  # Lève une exception pour les erreurs HTTP (4xx et 5xx)
        
        # Extrait le contenu de la réponse
        json_data=response.json()['data'][0]
        return remove_lists_from_dict(json_data)

    except requests.exceptions.RequestException as e:
        print(f"Une erreur s'est produite : {e}")
        return None
    

def addMeasure(config, influx_db):    
    api_url = config['airzone']['api_url']
    zones = config['airzone']['zones']
    delta_min = config['airzone']['delta_min']
    keys = config['airzone']['keys']

    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')

    points = []

    with open("airzone.csv", 'w', encoding='UTF8', newline='', ) as f:
        first = True
        for zone in zones:
            json_data = getZoneData(api_url, zone)
            demand = int(json_data['air_demand'] * 4 + json_data['cold_demand'] * 2 + json_data['heat_demand'] * 1)
            zone = json_data['name'].replace(" ", "_").replace(".", "")
            setpoint = int(json_data['setpoint'] * 10)
            roomtemp = int(json_data['roomTemp'] * 10)

            query = "SELECT * FROM temp_sensors WHERE zone='" + zone + "' ORDER BY time DESC LIMIT 1"
            result = list(influx_db.getFromQuery(query).get_points())

            if len(result) == 0 or \
                demand != result[0]['demand'] or \
                setpoint != result[0]['setpoint'] or \
                abs(setpoint - result[0]['setpoint']) >= config['airzone']['delta_min']:

                if first:
                    writer = csv.DictWriter(f, fieldnames=json_data.keys())
                    writer.writeheader()
                    first = False
                writer.writerow(json_data)

                point = {
                    "measurement": "temp_sensors",
                    "tags": {"zone": zone},
                    "time": timestamp,
                    "fields": {
                        "demand": demand,
                        "setpoint": setpoint,
                        "roomTemp": roomtemp
                    }
                }
                
                points.append(point)
                #print(point)

        if len(points) > 0:
            influx_db.writePoints(points)


def getLastPoint(config, influx_db):
    zone = "Bureau"
    query = "SELECT * FROM temp_sensors WHERE zone='" + zone + "' ORDER BY time DESC LIMIT 1"
    result = influx_db.getFromQuery(query)
    print(list(result.get_points())[0])


if __name__ == '__main__':

    # Reading of config file
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    
    influx_db = Influxdb(config, "airzone_database")

    #influx_db.clearAllTables()

    addMeasure(config, influx_db)
    #getLastPoint(config, influx_db)
