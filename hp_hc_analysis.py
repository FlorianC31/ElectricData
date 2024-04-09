from influxInterface import Influxdb
from hp_hc import Hp_hc
import timeStamp
import json
import pytz


def checkProduction():
    # Reading of config file
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    influx_db = Influxdb(config)

    daily_points = influx_db.getFromQuery("SELECT * FROM ENEDIS_daily_production").get_points()

    nb_points = 0

    for daily_point in daily_points:
        date = timeStamp.getTimestampFromStr(daily_point['time'], "utc", config['timeZone'])
        date = date.date().strftime('%Y-%m-%d')
        hourly_points = influx_db.getFromQuery("SELECT * FROM ENEDIS_hourly_production WHERE date='" + date + "'").get_points()

        daily_value = daily_point['value']
        sum_hourly_value = 0
        #print(daily_point['time'])

        for hourly_point in hourly_points:
            sum_hourly_value += hourly_point['value']
            #print(hourly_point['time'], hourly_point['date'], hourly_point['value'], sum_hourly_value)

        delta = daily_value - sum_hourly_value

        print(date + "," + daily_value + "," + sum_hourly_value + "," + delta)

        nb_points += 1
        if nb_points == 10:
            break


def checkConsommation():
    # Reading of config file
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    influx_db = Influxdb(config)

    daily_points = influx_db.getFromQuery("SELECT * FROM daily_consommation").get_points()

    nb_points = 0

    #print("date,daily_value_hp,sum_hourly_value_hp,delta_hp,daily_value_hc,sum_hourly_value_hc,delta_hc")

    delta_max_hp = 0
    delta_min_hp = 0
    delta_max_hc = 0
    delta_min_hc = 0

    for daily_point in daily_points:
        date = timeStamp.getTimestampFromStr(daily_point['time'], "utc", config['timeZone'])
        date_str = date.date().strftime('%Y-%m-%d')
        hourly_points = influx_db.getFromQuery("SELECT * FROM ENEDIS_hourly_consommation WHERE date='" + date_str + "'").get_points()
        #print("SELECT * FROM ENEDIS_hourly_consommation WHERE date='" + date_str + "'")


        daily_value_hp = int(daily_point['basse_hp'] + daily_point['pleine_hp'])
        daily_value_hc = int(daily_point['basse_hc'] + daily_point['pleine_hc'])
        sum_hourly_value_hp = 0
        sum_hourly_value_hc = 0

        is_hourly = False


        for hourly_point in hourly_points:
            #print(hourly_point)
            is_hourly = True
            sum_hourly_value_hp += hourly_point['HP_value']
            sum_hourly_value_hc += hourly_point['HC_value']

        delta_hp = daily_value_hp - sum_hourly_value_hp
        delta_hc = daily_value_hc - sum_hourly_value_hc

        hc_start_date = timeStamp.getTimestampFromDate(config['HC_HP'][1]['start_date'])

        if is_hourly and date >= pytz.utc.localize(hc_start_date):
            #print(date_str + "," + str(daily_value_hp) + "," + str(sum_hourly_value_hp) + "," + str(delta_hp) + "," + str(daily_value_hc) + "," + str(sum_hourly_value_hc) + "," + str(delta_hc))
            
            if delta_hp > delta_max_hp:
                delta_max_hp = delta_hp
                delta_max_hp_date = date_str
                if delta_max_hp > 100:
                    print(date_str + "," + str(daily_value_hp) + "," + str(sum_hourly_value_hp) + "," + str(delta_hp) + "," + str(daily_value_hc) + "," + str(sum_hourly_value_hc) + "," + str(delta_hc))

            if delta_hc > delta_max_hc:
                delta_max_hc = delta_hc
                delta_max_hc_date = date_str
                if delta_max_hc > 100:
                    print(date_str + "," + str(daily_value_hp) + "," + str(sum_hourly_value_hp) + "," + str(delta_hp) + "," + str(daily_value_hc) + "," + str(sum_hourly_value_hc) + "," + str(delta_hc))
                
            if delta_hp < delta_min_hp:
                delta_min_hp = delta_hp
                delta_min_hp_date = date_str
                if delta_min_hp < -100:
                    print(date_str + "," + str(daily_value_hp) + "," + str(sum_hourly_value_hp) + "," + str(delta_hp) + "," + str(daily_value_hc) + "," + str(sum_hourly_value_hc) + "," + str(delta_hc))

            if delta_hc < delta_min_hc:
                delta_min_hc = delta_hc
                delta_min_hc_date = date_str
                if delta_min_hc < -100:
                    print(date_str + "," + str(daily_value_hp) + "," + str(sum_hourly_value_hp) + "," + str(delta_hp) + "," + str(daily_value_hc) + "," + str(sum_hourly_value_hc) + "," + str(delta_hc))

    print("")
    print("delta_max_hp:", delta_max_hp_date, delta_max_hp)
    print("delta_max_hc:", delta_max_hc_date, delta_max_hc)
    print("delta_min_hp:", delta_min_hp_date, delta_min_hp)
    print("delta_min_hc:", delta_min_hc_date, delta_min_hc)


def extrapoleConsommationDay(timestamp_str, coeff_hc, coeff_hp):
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    utc_timestamp = timeStamp.getTimestampFromStr(timestamp_str, config['timeZone'], "utc")
    local_timestamp = timeStamp.getTimestampFromStr(timestamp_str, config['timeZone'])
    date_str = local_timestamp.date().strftime('%Y-%m-%d')

    hp_hc = Hp_hc(config)
    isSaisonPleine = hp_hc.isSaisonPleine(local_timestamp)

    basse_hc = 0
    basse_hp = 0
    pleine_hc = 0
    pleine_hp = 0

    
    influx_db = Influxdb(config)

    print("SELECT * FROM ENEDIS_hourly_consommation WHERE date='" + date_str + "'")
    hourly_points = influx_db.getFromQuery("SELECT * FROM ENEDIS_hourly_consommation WHERE date='" + date_str + "'").get_points()
    for hourly_point in hourly_points:
        if isSaisonPleine:
            pleine_hp += hourly_point['HP_value']
            pleine_hc += hourly_point['HC_value']
        else:
            basse_hp += hourly_point['HP_value']
            basse_hc += hourly_point['HC_value']

    point = {
        "measurement": "daily_consommation",
        "tags": {
            "source": config['consoAPI']['extrapoledSource']
        },
        "time": utc_timestamp,
        "fields": {
            "basse_hc": round(basse_hc * coeff_hc),
            "basse_hp": round(basse_hp * coeff_hp),
            "pleine_hc": round(pleine_hc * coeff_hc),
            "pleine_hp": round(pleine_hp * coeff_hp)
        }
    }
    
    influx_db.writePoints([point])
    print(point)



if __name__ == "__main__":
    extrapoleConsommationDay("2022-03-26T00:00:00+01:00", 0.978628673196794, 1.00510899182561)
    extrapoleConsommationDay("2022-03-27T00:00:00+01:00", 0.978628673196794, 1.00510899182561)
    extrapoleConsommationDay("2023-03-25T00:00:00+01:00", 0.955475074563272, 1.0144820650352)
    extrapoleConsommationDay("2023-03-26T00:00:00+01:00", 0.955475074563272, 1.0144820650352)
    checkConsommation()
    