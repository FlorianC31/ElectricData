"""
File: influxInterface.py
Author: Florian CHAMPAIN
Description: This file contains functions for influx database interface
Date Created: 2024-04-09
"""


from influxdb import InfluxDBClient
import timeStamp

class Influxdb:
    """This class contains functions for influx database interface
    """
    def __init__(self, config):
        """Create a new influxdb interface

        Args:
            config (json dict): configuration data
            database (string): Name of the database to connect
        """
        self.client = client = InfluxDBClient(host=config['influxDb']['host'], 
                                              port=config['influxDb']['port'], 
                                              username=config['influxDb']['user'], 
                                              password=config['influxDb']['password'], 
                                              database=config['influxDb']['database'])
        self.database = config['influxDb']['database']
    
    
    def writePoints(self, points):
        """Write points in the active influx database

        Args:
            points (list of json point): Lists of the points to insert in the active influx database
        """
        if not self.client.write_points(points):
            print("ERROR while writing points into influx")
        
        
    def close(self):
        """Close the influx database
        """
        self.client.close()


    def getLastTimestamp(self, measure_name):
        """Get the timestamp of the last point of a measure

        Args:
            measure_name (string): Name of the measure

        Returns:
            timestamp: timestamp of the last point of a measure
        """
        # Query pour récupérer le dernier point de la mesure
        query = 'SELECT * FROM ' + measure_name + ' ORDER BY time DESC LIMIT 1'
        # Exécution de la requête
        result = self.client.query(query)

        if len(result) == 0:
            return None

        # Récupération du timestamp du dernier point
        last_point = list(result.get_points())[0]  # Récupère le premier (et unique) point du résultat
        return last_point['time']
    
    
    def getLastPoint(self, measure_name):
        """Get te last point of a measure in the active influx database

        Args:
            measure_name (string): name of the measure

        Returns:
            dict: last point of the 
        """
        
        query = 'SELECT * FROM ' + measure_name + ' ORDER BY time DESC LIMIT 1'
        result = self.client.query(query)
        
        if len(result) == 0:
            return None

        last_point = list(result.get_points())[0]  # Récupère le premier (et unique) point du résultat
        last_point["time"] = timeStamp.getTimestampFromStr(last_point["time"])
        return last_point


    def getFromQuery(self, query):
        """Execute a influx query and return the result

        Args:
            query (string): influx query to execute

        Returns:
            json: result of the query
        """
        return self.client.query(query)
    
    
    def clearTable(self, table):
        """Delete all the points of a table(measurement)

        Args:
            table (string): table(measurement) where delete all the points
        """
        query = "DELETE FROM " + table
        self.client.query(query)

    def query(self, query):
        """OBSOLETE: Old function to return the last point from a influxdb query (used only in enedys.py and checkEnedisData.py)

        Args:
            query (_type_): _description_

        Returns:
            _type_: _description_
        """
        result = list(self.client.query(query))
        if len(result) > 0:
            return result[0]
        else:
            return []
    
    def clearAllTables(self):
        """Delete a influx database and recreate it
        """
        query = "DROP DATABASE " + self.database
        self.client.query(query)
        query = "CREATE DATABASE " + self.database
        self.client.query(query)
