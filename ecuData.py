import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from influxInterface import Influxdb
import timeStamp
from datetime import datetime, timedelta
    
class EcuData:
    def __init__(self, config):
        self.config = config["apsystems"]
        self.timeZone = config["timeZone"]
        self.influx_db = Influxdb(config)
        self.panelsId = {}
        self.lastTimestamp = None
        self.voltage = 0
        self.freq = 0
        self.energyData = []
        self.list_header = []
        self.powerData = []
        self.points = []
        
        i = 1
        for inverter in self.config["inverters_id"]:
            for panel in ("-1", "-2"):
                self.panelsId[str(inverter) + panel] = i
                i+=1
                
    def getEnergies(self, date):
        # source of the form: http://192.168.1.33/index.php/hidden/export_file
        url = self.config["inverter_url"]

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        requestedDate = {
            'start_time': date + " 00:00:00",
            'end_time': date + " 23:59:59"
        }
        print(requestedDate)

        response = requests.post(url, data=requestedDate, headers=headers)

        if response.status_code != 200:
            print(f"La requête a échoué avec le code d'état : {response.status_code}")
            exit()


        # Utiliser la date de début comme nom de fichier Excel
        #excel_file_name = start_time.replace(":", "").replace(" ", "_") + ".xls"



        # for getting the header from
        # the HTML file
        soup = BeautifulSoup(response.content,'html.parser')
        header = soup.find_all("table")[0].find("tr")

        for items in header:
            try:
                self.list_header.append(items.get_text())
            except:
                continue
        
        #print(self.list_header)


        # for getting the data
        HTML_data = soup.find_all("table")[0].find_all("tr")[1:]

        for element in HTML_data:
            sub_data = []
            for sub_element in element:
                try:
                    sub_data.append(sub_element.get_text())
                except:
                    continue
            self.energyData.append(sub_data)
        
        self.setPoints()
        self.savePoints()
            
    
    def getPower(self, date):
        url = self.config["power_url"]
        args = {'date': date}

        response = requests.post(url, data=args)
        if response.status_code == 200:
            json_data = response.json()
            json_data = json.dumps(json_data, indent=4)
            data = json.loads(json_data)

        else:
            print(f"La requête a échoué avec le code d'état : {response.status_code}")       
            
        self.powerData = {}
        i = 0

        for point in data['power1']:
            result = {}
            result['timestamp'] = point['time'] /1000
            result['producted'] = point['powerA']

            if (point['time'] != data['power2'][i]['time']):
                print("ERROR: Timestamps of power 1 and 2 do not correspond")
                break

            result['enedis'] = data['power2'][i]['powerA']
            timestamp = datetime.fromtimestamp(result['timestamp'])
            timestamp = timeStamp.changeTimeZone(timestamp, self.timeZone)
            
            i += 1

            
        
    def printMaxEnegergy(self):

        points = {}

        for row in self.energyData:
            timestamp = row[9]
            if timestamp not in points:
                points[timestamp] = {}

            panelId = (row[0], row[1])
            points[timestamp][panelId] = {}
            for i in range(2,9):
                points[timestamp][panelId][self.list_header[i]] = row[i]
            #print(timestamp, panelId, points[timestamp][panelId])

        totalEnergy = 0
        for timestamp in points:
            nbPanels = len(points[timestamp])
            first = True
            for panelId in points[timestamp]:
                if first:
                    total = {}
                    for measure in points[timestamp][panelId]:
                        total[measure] = float(points[timestamp][panelId][measure])
                    first = False
                else:
                    for measure in points[timestamp][panelId]:
                        total[measure] += float(points[timestamp][panelId][measure])

            total['DC Voltage(V)'] /= nbPanels
            total['Grid Frequency(Hz)'] /= nbPanels
            total['Temperature(oC)'] /= nbPanels
            total['Grid Voltage(V)'] /= nbPanels

            totalEnergy += total['Energy(kWh)'] 

            points[timestamp]['total'] = total

            print(timestamp, points[timestamp]['total'])

        print("Total Energy of the day (kWh):", totalEnergy)

        
    def exportCsv(self, csvFile):
        
        # Storing the data into Pandas DataFrame
        dataFrame = pd.DataFrame(data = self.energyData, columns = self.list_header)
        
        # Converting Pandas DataFrame into CSV file
        dataFrame.to_csv(csvFile)


    def getACdata(self):
        timestamp = timeStamp.roundTimestamp(timeStamp.getTimestampFromStr(self.lastTimestamp, self.timeZone, "utc"))
        point = {
            "measurement": "ECU_ac_data",
            "time": timestamp,
            "fields": {
                "AC_freq": int(self.freq * 100 / (len(self.config["inverters_id"]) * 2)),
                "AC_voltage": int(self.voltage * 10 / (len(self.config["inverters_id"]) * 2))
            }
        }
        self.points.append(point)


    def setPoints(self):
        self.points = []
        
        for line in self.energyData:
        
            # processing of AC freq and voltage (mean for all panels)
            if line[9] != self.lastTimestamp:                
                # compute and save previous timestamp freq and voltage (mean of all panels)
                if self.lastTimestamp is not None:
                    self.getACdata()
                
                # initialization of a new timesamp
                self.lastTimestamp = line[9]
                self.voltage = 0
                self.freq = 0

            # sum data for current timestamp
            self.voltage += float(line[7])
            self.freq += float(line[5])
            
            # procession of DC parameters (for each panel)    
            timestamp = timeStamp.roundTimestamp(timeStamp.getTimestampFromStr(line[9], self.timeZone, "utc"))
            
            panelId = self.panelsId[line[0].replace(" ", "") + "-" + line[1]]
            point = {
                "measurement": "ECU_energy",
                "tags": {"panel": panelId},             # unsigned char (1octet)
                "time": timestamp,
                "fields": {
                    "energy": int(float(line[8]) * 1000000),
                    "temperature": int(float(line[6])),
                    "dc_voltage": int(float(line[2]) * 10),
                    "dc_current": int(float(line[3]) * 10),
                    "dc_power": int(float(line[4]))
                }
            }
            self.points.append(point)
            #print(point)
            
        # call the AC data function one last time to process the last timestamp
        self.getACdata()    
    
    def savePoints(self):        
        self.influx_db.writePoints(self.points)
    

if __name__ == "__main__":
    with open('/home/florian/enedis/config.json', 'r') as config_file:
        config = json.load(config_file)
    ecuData = EcuData(config)
    
    yesterday = datetime.now() - timedelta(days=1)        
    ecuData.getEnergies(yesterday.strftime("%Y-%m-%d"))
    #print(yesterday.strftime("%Y-%m-%d"))
    