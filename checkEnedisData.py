from datetime import timedelta, datetime
from influxInterface import Influxdb
from dateutil.parser import parse
import json
import timeStamp
from hp_hc import Hp_hc

TOLERANCE = 100

def checkEnedisData():
    print("Check Enedis data")
    # Reading of config file
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    hp_hc = Hp_hc(config)
    influx_db = Influxdb(config)
    result = influx_db.query("SELECT * FROM ENEDIS_hourly_consommation ORDER BY time ASC LIMIT 1")
    start_day_ts = parse(result[0]['time'])
    end_day_ts = start_day_ts + timedelta(days = 1)

    while end_day_ts.date() != datetime.now().date():

        query = f"SELECT SUM(HC_value), SUM(HP_value) FROM ENEDIS_hourly_consommation WHERE time >= '" + timeStamp.timestamp2str(start_day_ts) + "' AND time < '" + timeStamp.timestamp2str(end_day_ts) + "'"
        hourly_result = influx_db.query(query)
        if len(hourly_result) == 0:
            hourly_hc = 0
            hourly_hp = 0
        else:
            hourly_hc = hourly_result[0]['sum']
            hourly_hp = hourly_result[0]['sum_1']

        query = f"SELECT * FROM ENEDIS_daily_consommation WHERE time >= '" + timeStamp.timestamp2str(start_day_ts) + "' AND time < '" + timeStamp.timestamp2str(end_day_ts) + "'"
        daily_result = influx_db.query(query)

        if len(daily_result) == 0:
            daily_hc = 0
            daily_hp = 0            
        else:
            if hp_hc.isSaisonPleine(end_day_ts):
                saison = "pleine"
            else:
                saison = "basse"
            daily_hc = daily_result[0][saison + "_hc"]
            daily_hp = daily_result[0][saison + "_hp"]

        if abs(hourly_hc - daily_hc) > TOLERANCE:
            print("Date:", end_day_ts.date().strftime("%Y-%m-%d"), " - hourly_HC =", str(hourly_hc), " - daily_HC =", str(daily_hc), " - delta_HC =", str(abs(hourly_hc - daily_hc)))


        if abs(hourly_hp - daily_hp) > TOLERANCE or hourly_hp == 0:
            print("Date:", end_day_ts.date().strftime("%Y-%m-%d"), " - hourly_HP =", str(hourly_hp), " - daily_HP =", str(daily_hp), " - delta_HP =", str(abs(hourly_hp - daily_hp)))

        start_day_ts = end_day_ts
        end_day_ts = start_day_ts + timedelta(days = 1)


if __name__ == "__main__":
    checkEnedisData()