from influxdb import InfluxDBClient

class Influxdb:
    def __init__(self, config):
        self.client = client = InfluxDBClient(host=config['influxDb']['host'], 
                                              port=config['influxDb']['port'], 
                                              username=config['influxDb']['user'], 
                                              password=config['influxDb']['password'], 
                                              database=config['influxDb']['database'])
    
    def writePoints(self, points):
        if not self.client.write_points(points):
            print("ERROR while writing points into influx")
        
    def close(self):
        self.client.close()

    def getLastTimestamp(self, measure_name):
        # Query pour récupérer le dernier point de la mesure
        query = 'SELECT * FROM ' + measure_name + ' ORDER BY time DESC LIMIT 1'
        # Exécution de la requête
        result = self.client.query(query)

        # Récupération du timestamp du dernier point
        last_point = list(result.get_points())[0]  # Récupère le premier (et unique) point du résultat
        return last_point['time']

    def getFromQuery(self, query):
        return self.client.query(query)