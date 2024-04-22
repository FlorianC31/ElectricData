import requests
import json
import csv
from influxInterface import Influxdb
from datetime import datetime
from pushNotificationApi import sendNotif
import timeStamp


def remove_lists_from_dict(input_dict):
    return {key: value for key, value in input_dict.items() if not isinstance(value, list)}


def getZoneData(api_url, zone):
    try:
        response = requests.get(api_url + "?systemid=1&zoneid=" + str(zone))
        response.raise_for_status()  # Lève une exception pour les erreurs HTTP (4xx et 5xx)
        
        # Extrait le contenu de la réponse
        json_data=response.json()['data'][0]
        return remove_lists_from_dict(json_data)

    except requests.exceptions.RequestException as e:
        print(f"Une erreur s'est produite : {e}")
        return None
    
    
def setTemperature(api_url, zone_id, temperature):
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "systemid": 1,
        "zoneid": zone_id,
        "setpoint": temperature
    }
    response = requests.put(api_url, json=data, headers=headers)
    
    print(api_url)
    print(zone_id)
    print(data)
    
    if response.status_code == 200:
        print("La température de consigne a été modifiée avec succès.")
    else:
        print("Échec de la modification de la température de consigne. Code de statut :", response.status_code, response._content)

    

def addMeasure(config, influx_db):    
    api_url = config['airzone']['api_url']
    zones = config['airzone']['zones']
    delta_min = config['airzone']['delta_min']
    keys = config['airzone']['keys']

    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')
    print("ts:", timestamp)

    points = []

    with open("airzone.csv", 'w', encoding='UTF8', newline='', ) as f:
        first = True
        for zone_id in zones:
            json_data = getZoneData(api_url, zone_id)
            if json_data != None:
                
                demand = int(json_data['air_demand'] * 4 + json_data['cold_demand'] * 2 + json_data['heat_demand'] * 1)
                zone = json_data['name'].replace(" ", "_").replace(".", "")
                setpoint = int(json_data['setpoint'] * 10)
                roomTemp = int(json_data['roomTemp'] * 10)
                
                if (zone_id == 4 and setpoint > 195):
                    setTemperature(api_url, zone_id, 15.0)
                    sendNotif("Bureau en surchauffe", "La consigne de température du bureau est de " + str(setpoint / 10), "")
                    with open("airzoneJournal.log", "a") as log:
                        log.write(timestamp + "; " + str(setpoint / 10) + "\n")
                    

                query = "SELECT * FROM AIRZONE_sensors WHERE zone='" + zone + "' ORDER BY time DESC LIMIT 1"
                result = list(influx_db.getFromQuery(query).get_points())

                if len(result) == 0 or \
                    demand != result[0]['demand'] or \
                    setpoint != result[0]['setpoint'] or \
                    abs(roomTemp - result[0]['roomTemp']) >= config['airzone']['delta_min'] * 10:

                    if first:
                        writer = csv.DictWriter(f, fieldnames=json_data.keys())
                        writer.writeheader()
                        first = False
                    writer.writerow(json_data)

                    point = {
                        "measurement": "AIRZONE_sensors",
                        "tags": {"zone": zone},
                        "time": timeStamp.getCurrentTimestamp(config['timeZone']),
                        "fields": {
                            "demand": demand,
                            "setpoint": setpoint,
                            "roomTemp": roomTemp
                        }
                    }
                    
                    points.append(point)
                    print(point)
                else:
                    print(zone)
                    print(result[0]['time'])
                    print("New demand:", demand, "- Previous demand:", result[0]['demand'])
                    print("New setpoint:", setpoint, "- Previous setpoint:", result[0]['setpoint'])
                    print("New roomTemp:", roomTemp, "- Previous roomTemp:", result[0]['roomTemp'])


        if len(points) > 0:
            influx_db.writePoints(points)


def getLastPoint(config, influx_db):
    zone = "Bureau"
    query = "SELECT * FROM AIRZONE_sensors WHERE zone='" + zone + "' ORDER BY time DESC LIMIT 1"
    result = influx_db.getFromQuery(query)
    print(list(result.get_points())[0])


if __name__ == '__main__':
    print(datetime.now())
    # Reading of config file
    with open('/home/florian/enedis/config.json', 'r') as config_file:
        config = json.load(config_file)
    
    influx_db = Influxdb(config)

    #influx_db.clearAllTables()

    addMeasure(config, influx_db)
    #getLastPoint(config, influx_db)
