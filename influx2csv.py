from influxInterface import Influxdb
import json
import csv

def influx2csv(database):
    print("Export influx tables to csv files")
    ''' Export all date from each table in csv files
    '''
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    
    influx_db = Influxdb(config, database)

    tables = influx_db.getFromQuery("SHOW MEASUREMENTS").get_points()
    for table in tables:
        print(table['name'])
        measures = influx_db.getFromQuery("SELECT * FROM " + table['name']).get_points()
        #test = list(measures)[0] 
        #print(test)

        with open(config['influxDb'][database] + "_" + table['name'] + ".csv", 'w', encoding='UTF8', newline='', ) as f:
            writer = csv.DictWriter(f, fieldnames=next(measures).keys())
            writer.writeheader()
            writer.writerows(measures)

if __name__ == "__main__":
    influx2csv("database")
