"""
File: getEcu.py
Author: Florian CHAMPAIN
Description: This file contains functions for getting power and energy data from APSystems ECU-C
Date Created: 2024-04-09
"""


import requests
import json
from influxInterface import Influxdb
import timeStamp
from datetime import datetime, timedelta
from ctrlECS import EcsRelay
from hp_hc import Hp_hc


HP = 1
HC = 0

ON = 1
OFF = 0

class EcuData:
    """This class is used for getting power and energy data from APSystems ECU-C
    """
    def __init__(self, config):
        self.config = config["apsystems"]
        self.timeZone = config["timeZone"]
        self.ecs_threshold_on = config["ecs"]["threshold_on"]
        self.ecs_threshold_off = config["ecs"]["threshold_off"]
        self.influx_db = Influxdb(config)
        self.points = []
        self.hp_hc = Hp_hc(config)
        self.ecs_relay = EcsRelay()

    
    def getPower(self, date, isToday):
        """Method to get the power data of specific date

        Args:
            date (timestamp): date to extract power from ECU
            isToday (bool): true if the date is today
        """
        if isToday:
            lastTimestamp = timeStamp.getTimestampFromStr(self.influx_db.getLastTimestamp("ECU_power"))
            lastTimestamp = timeStamp.changeTimeZone(lastTimestamp, self.timeZone)  

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
        
        self.points = []

        for line in data['power1']:
            result = {}
            result['timestamp'] = line['time'] /1000
            result['producted'] = line['powerA']

            if (line['time'] != data['power2'][i]['time']):
                print("ERROR: Timestamps of power 1 and 2 do not correspond")
                break

            result['enedis'] = data['power2'][i]['powerA']
            timestamp = datetime.fromtimestamp(result['timestamp'])
            timestamp = timeStamp.changeTimeZone(timestamp, self.timeZone)
            timestamp = timeStamp.roundTimestamp(timestamp)

            i += 1

            if not isToday or timestamp > lastTimestamp:

                point = {
                    "measurement": "ECU_power",
                    "time": timestamp,
                    "fields": {
                        "producted": int(result['producted']),
                        "enedis": int(result['enedis'])
                    }
                }
                            
                self.points.append(point)

        self.savePoints()

    
    def savePoints(self):
        """Save the points in influx
        """
        self.influx_db.writePoints(self.points)


    def getAllData(self):
        timestamp_str = self.influx_db.getLastTimestamp("ECU_power")
        timestamp = timeStamp.getTimestampFromStr(timestamp_str)
        timestamp = timeStamp.changeTimeZone(timestamp, self.timeZone)

        while(timestamp.date() < datetime.now().date()):
            self.getPower(timestamp.strftime("%Y-%m-%d"), False)
            timestamp += timedelta(days=1)
            #time.sleep(0.01)
        
        self.getPower(timestamp.strftime("%Y-%m-%d"), True)
            
    
    def calEcs(self):
        """Calculate if the ECS needs to be turn ON or OFF in function of the time of the day and the power exchanged with Enedis
        """
        point = self.influx_db.getLastPoint("ECU_power")
        timestamp = timeStamp.getCurrentTimestamp(self.timeZone)
        local_timestamp = timeStamp.changeTimeZone(timestamp, self.timeZone)

        if (timestamp - point["time"] <= timedelta(minutes=5)):
            
            ecs_current_state = self.influx_db.getLastPoint("ECS_relay")["active"]
            hchp_current_state = self.influx_db.getLastPoint("ENEDIS_hphc")["hp"]

            if self.hp_hc.isHp(local_timestamp):
                print("New hphc: heures pleines")
            else:                
                print("New hphc: heures creuses")
            
            if hchp_current_state == HP:     # Current state : Heures pleines
                if not self.hp_hc.isHp(local_timestamp):
                    hchp_current_state = HC
                    self.addHpHcPoint(timestamp, HC)     # Switching to heures creuses
                    if local_timestamp.strftime("%H") < 10:     # Disable the ECS at night 
                        self.setRelayPoint(timestamp, ON)    # turning on ECS
                    
            
            elif hchp_current_state == HC:                           # Current state : heures creuses
                if self.hp_hc.isHp(local_timestamp):
                    hchp_current_state = HP
                    self.addHpHcPoint(timestamp, HP)     # Switching to heures pleines

            
            print("Production:", point['producted'])
            print("Consommation:", point['enedis'])
            

            if hchp_current_state == HP:     # If the new state is heures pleines
                print("Heures Pleines")

                if ecs_current_state == ON:
                    print("ecs_current_state == ON")
                else:
                    print("ecs_current_state == OFF")

                print(self.ecs_threshold_off, point['enedis'], self.ecs_threshold_on)
                
                # If the ECS is on and the enedis power is higher to threshold_off, then turn off the ECS
                if ecs_current_state == ON and (point['enedis'] > self.ecs_threshold_off):
                    self.setRelayPoint(timestamp, OFF)    # turning off ECS                        
                
                # If the ECS is off and the enedis power is lower to threshold_off, then turn on the ECS
                if ecs_current_state == OFF and (point['enedis'] < self.ecs_threshold_on):
                    self.setRelayPoint(timestamp, ON)    # turning on ECS
            else:
                print("Heures Creuses")
                
            if ecs_current_state == ON:
                print("ECS ON")
            else:
                print("ECS OFF")
                
            
        
    def setRelayPoint(self, timestamp, active):
        """method to save in influxdb the change of state of the ECS relay

        Args:
            timestamp (timestamp): current timestamp
            active (int): new state of the ECS (0 for OFF and 1 for ON)
        """
        if active == 1:
            print("Turn ECS on")
            self.ecs_relay.turnRelayOn("On")
        else:
            print("Turn ECS off")
            self.ecs_relay.turnRelayOn("Off")
            
        point = {
            "measurement": "ECS_relay",
            "tags": {},
            "time": timestamp,
            "fields": {
                "active": active,
            }
        }
        
        self.points.append(point)
        self.savePoints()
        
        
    def addHpHcPoint(self, timestamp, state):
        """method to add the new state of heures pleines / heures creuses in influxdb

        Args:
            timestamp (timestamp): current timestamp
            state (int): new state of hphc (0 for HC and 1 for HP)
        """
        self.points = []
        point = {
            "measurement": "ENEDIS_hphc",
            "tags": {},
            "time": timestamp,
            "fields": {
                "hp": state,
            }
        }
        self.points.append(point)
        print(point)
        self.savePoints()


if __name__ == "__main__":
    with open('/home/florian/enedis/config.json', 'r') as config_file:
        config = json.load(config_file)
    ecuData = EcuData(config)

    ecuData.getAllData()
    ecuData.calEcs()
    #ecuData.getPower("2023-05-29")
    
    #ecuData.addHpHcPoint(timeStamp.getTimestampFromStr("2024-04-09 00:00:00", config["timeZone"]), HP)
    #ecuData.addHpHcPoint(timeStamp.getTimestampFromStr("2024-04-09 01:10:00", config["timeZone"]), HC)
    #ecuData.addHpHcPoint(timeStamp.getTimestampFromStr("2024-04-09 07:40:00", config["timeZone"]), HP)
    #ecuData.addHpHcPoint(timeStamp.getTimestampFromStr("2024-04-09 12:10:00", config["timeZone"]), HC)
    #ecuData.addHpHcPoint(timeStamp.getTimestampFromStr("2024-04-09 13:40:00", config["timeZone"]), HP)
    #ecuData.addHpHcPoint(timeStamp.getTimestampFromStr("2024-04-10 01:10:00", config["timeZone"]), HC)
    #ecuData.addHpHcPoint(timeStamp.getTimestampFromStr("2024-04-10 07:40:00", config["timeZone"]), HP)

    #ecuData.setRelayPoint(timeStamp.getTimestampFromStr("2024-04-10 07:40:00", config["timeZone"]), OFF)
    