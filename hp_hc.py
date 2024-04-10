from datetime import timedelta, date, datetime
import timeStamp
import json

class Hp_hc:
    '''
    Class to manage heures creuses / heures pleines
    '''
    def __init__(self, config):
        self.config = config['HC_HP']
        self.periods = []
        self.time_zone = config['timeZone']

        for period in self.config:
            new_period = {} 
            new_period['start_date'] = timeStamp.getTimestampFromStr(period['start_date'], config['timeZone'], "utc")
            new_period['HC_HP_count'] = period['HC_HP_count']
            
            if (period['HC_HP_count']):
                new_period['HC'] = []
                for hc in period['HC']:
                    new_hc = {}
                    new_hc['start'] = timeStamp.getTimestampFromTime(hc['start'])
                    new_hc['end'] = timeStamp.getTimestampFromTime(hc['end'])
                    new_period['HC'].append(new_hc)
                new_period['saison_pleine'] = period['saison_pleine']

            self.periods.append(new_period)

    def getHourHcRatio(self, timestamp, interval):
        '''
        Get the ratio of heure creuse in 1 hour
        '''
        date0 = date(1900, 1, 1)
        end_time = datetime.combine(date0, timestamp.time())
        start_time = end_time - timedelta(minutes=interval)

        period = self.getPeriodFromDate(timestamp)
        if period is None:
            return -1   

        if not period['HC_HP_count']:
            return 0.0
        for hc in period['HC']:
            try:
                if (end_time < hc['start']):
                    return 0.0
                elif (start_time < hc['start'] and end_time > hc['start']):
                    return (end_time - hc['start']) / (end_time - start_time)
                elif (start_time > hc['start'] and end_time < hc['end']):
                    return 1.0
                elif (start_time < hc['end'] and end_time > hc['end']):
                    return (hc['end'] - start_time) / (end_time - start_time)
                elif (hc == period["HC"][-1]):
                    return 0.0
            except TypeError:
                print(end_time, hc['start'])
    
    
    def isSaisonPleine(self, timestamp):
        period = self.getPeriodFromDate(timestamp)
        if period is None:
            return False      
        if not period['HC_HP_count']:
            return False
        else:
            return period['saison_pleine'][timestamp.month - 1]
    
    
    def isHpHc(self, timestamp):
        period = self.getPeriodFromDate(timestamp)
        if period is None:
            return False
        return period["HC_HP_count"]
        

    def getPeriodFromDate(self, timestamp):
        utc_timestamp = timeStamp.changeTimeZone(timestamp, "utc")

        for period in reversed(self.periods):
            if period["start_date"] < utc_timestamp:
                return period
        return None
    

    def isHp(self, timestamp):

        period = self.getPeriodFromDate(timestamp)
        if period is None:
            return True

        if not period['HC_HP_count']:
            return True
        for hc in period['HC']:
            if (timestamp.time() >= hc["start"].time() and timestamp.time() < hc["end"].time()):
                print(hc["start"].time(), timestamp.time(), hc["end"].time())
                return False
            
        return True


if __name__ == "__main__":
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    hp_hc = Hp_hc(config)

    #timestamp = timeStamp.getTimestampFromStr("2021-10-13 20:00:00 ", "Europe/Paris", "Europe/Paris")
#
    #for i in range(0, 50):
    #    period = hp_hc.getPeriodFromDate(timestamp)
    #    print(timestamp, period['start_date'])
    #    timestamp += timedelta(minutes = 30)



    #timeStamp.changeTimeZone(utc_timestamp, "Europe/Paris")
    #print("utc_timestamp:", timeStamp.changeTimeZone(utc_timestamp, "GMT+1").date())
#
    #timestamp = timeStamp.getTimestampFromStr("2023-11-17 00:00:00")
    #print("timestamp:", timestamp.date())
    #
    local_timestamp = timeStamp.getTimestampFromStr("2023-06-17 00:00:00", "Europe/Paris", "Europe/Paris")
    print(local_timestamp, hp_hc.isHp(local_timestamp))
    local_timestamp = timeStamp.getTimestampFromStr("2023-06-17 02:00:00", "Europe/Paris", "Europe/Paris")
    print(local_timestamp, hp_hc.isHp(local_timestamp))
    local_timestamp = timeStamp.getTimestampFromStr("2023-06-17 08:00:00", "Europe/Paris", "Europe/Paris")
    print(local_timestamp, hp_hc.isHp(local_timestamp))
    local_timestamp = timeStamp.getTimestampFromStr("2023-06-17 13:00:00", "Europe/Paris", "Europe/Paris")
    print(local_timestamp, hp_hc.isHp(local_timestamp))
    local_timestamp = timeStamp.getTimestampFromStr("2023-06-17 14:00:00", "Europe/Paris", "Europe/Paris")
    print(local_timestamp, hp_hc.isHp(local_timestamp))
    local_timestamp = timeStamp.getTimestampFromStr("2017-06-17 02:00:00", "Europe/Paris", "Europe/Paris")
    print(local_timestamp, hp_hc.isHp(local_timestamp))
