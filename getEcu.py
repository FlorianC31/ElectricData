from requests import post
from datetime import datetime
import json

# Remplacez avec la date que vous voulez
date_string = "2023-11-15"

url = 'http://192.168.1.33/index.php/meter/old_meter_power_graph'
args = {'date': date_string}

response = post(url, data=args)
if response.status_code == 200:
    json_data = response.json()
    json_data = json.dumps(json_data, indent=4)
    data = json.loads(json_data)

else:
    print(f"La requête a échoué avec le code d'état : {response.status_code}")

results = []
i = 0

for point in data['power1']:
    result = {}
    result['timestamp'] = point['time'] /1000
    result['producted'] = point['powerA']

    if (point['time'] != data['power2'][i]['time']):
        print("ERROR: Timestamps of power 1 and 2 do not correspond")
        break

    result['enedis'] = data['power2'][i]['powerA']

    results.append(result)

    i += 1

for result in results:
    timeStamp = datetime.fromtimestamp(result['timestamp'])
    print(timeStamp.strftime("%Y-%m-%d %H:%M:%S"), ";", result['producted'], ";", result['enedis'])
