import xlrd
from datetime import datetime, timedelta
import os
import timeStamp
import json
from influxInterface import Influxdb


# Chemin vers votre fichier Excel .xls
chemin_dossier = "~/enedis/energie/"
fichier_prefixe = "Energy for 215000029878 in "


firstDay = "2024-01-22"
lastDay = "2024-01-23"
date_TS = datetime.strptime(firstDay, '%Y-%m-%d')
lastDate_TS = datetime.strptime(lastDay, '%Y-%m-%d')

with open('config.json', 'r') as config_file:
    config = json.load(config_file)

influx_db = Influxdb(config)

while (date_TS != lastDate_TS):
    chemin_fichier = os.path.expanduser(os.path.join(chemin_dossier, fichier_prefixe + date_TS.strftime("%Y-%m-%d") + ".xls"))

    workbook = xlrd.open_workbook(chemin_fichier)

    # Sélectionner la première feuille de calcul
    worksheet = workbook.sheet_by_index(0)
    
    points = []

    # Lire les données ligne par ligne
    for row_index in range(1, worksheet.nrows - 1):
        row_data = worksheet.row_values(row_index)
        #print(date_TS.strftime("%Y-%m-%d"), row_data)
        
        timestampStr = date_TS.strftime("%Y-%m-%d ") + row_data[0] + ":00"
        timestamp = timeStamp.roundTimestamp(timeStamp.getTimestampFromStr(timestampStr, config['timeZone'], "utc"))
        print(timestamp)
        
        point = {
            "measurement": "ECU_energy",
            "tags": {"panel": 0},             # unsigned char (1octet)
            "time": timestamp,
            "fields": {
                "energy": int(float(row_data[1]) * 1000),
                "temperature": 0,
                "dc_voltage": 0,
                "dc_current": 0,
                "dc_power": 0
            }
        }
        points.append(point)
        #print(point)
    influx_db.writePoints(points)        

    date_TS += timedelta(days=1)
