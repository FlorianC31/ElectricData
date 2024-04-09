"""
File: timeStamp.py
Author: Florian CHAMPAIN
Description: This file contains functions for influx database interface
Date Created: 2024-04-09
"""


from datetime import datetime, timedelta
import pytz



def getTimestampFromStr(timestamp_str, source_time_zone = "utc", dest_time_zone = None):
    """Transform a string timestamp in actual timestamp and optionally transfer its timezone

    Args:
        timestamp_str (string): timestamp in text format
        source_time_zone (string, optional): source time zone. Defaults to "utc"
        dest_time_zone (string, optional): destination time zone. Defaults to None (no timezone transfer).

    Returns:
        timestamp: timestamp located in a time_zone
    """
    try:
        timestamp = datetime.strptime(timestamp_str[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        try:
            timestamp = datetime.strptime(timestamp_str[:19], "%Y-%m-%d %H:%M:%S")
        except:
            timestamp = datetime.strptime(timestamp_str[:19], "%Y/%m/%d %H:%M:%S")
    if source_time_zone == "utc":
        source_tz = pytz.utc
    else:
        source_tz = pytz.timezone(source_time_zone)
    source_timestamp = source_tz.localize(timestamp)

    if source_time_zone != dest_time_zone and dest_time_zone != None:
        if dest_time_zone == "utc":
            dest_tz = pytz.utc
        else:
            dest_tz = pytz.timezone(dest_time_zone)
        return source_timestamp.astimezone(dest_tz)
    else:
        return source_timestamp
    
    
def getTimestampFromTime(time_str):
    """Generate a timestamp from a string time

    Args:
        time_str (string): string time with "%H:%M:%S" format

    Returns:
        timestamp: timestamp without timezone localisation
    """
    return datetime.strptime(time_str, "%H:%M:%S")


def getTimestampFromDate(date_str, timezone):
    """Generate a timestamp from a string date

    Args:
        date_str (string): date time with "%Y-%m-%d" format
        timezone (string): timestamp iwithout timezone localisation

    Returns:
        timestamp, timestamp: timestamp in UTC, timestamp in local timezone
    """
    start_of_day_ts_str = date_str + " 00:00:00"
    return (getTimestampFromStr(start_of_day_ts_str, timezone, "utc") + timedelta(days = 1),
            getTimestampFromStr(start_of_day_ts_str, timezone, timezone) + timedelta(days = 1))


def getEndDate(start_date, delta_value):
    """Get the end date from a start date and an delta time

    Args:
        start_date (timestamp): the start date
        delta_value (int): value of the delta (in days)

    Returns:
        timestamp: end date
    """
    end_date = start_date + timedelta(days=delta_value)
    return min(end_date, datetime.now().date())


def timestamp2str(timestamp):
    """Get the iso string format of the timestamp

    Args:
        timestamp (timestamp): timestamp to convert

    Returns:
        string: string converted timestamp
    """
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def changeTimeZone(timestamp, new_tz):
    """Overwrite the timezone of a timestamp

    Args:
        timestamp (timestamp): source timestamp
        new_tz (string): time zone code

    Returns:
        timestamp: timestamp located in the new timezone
    """
    if new_tz == "utc":
        tz = pytz.utc
    else:
        tz = pytz.timezone(new_tz)

    return timestamp.astimezone(tz)


def roundTimestamp(timestamp):
    """Round a timestamp at 1/1000 pf a day (1.44 minutes) in order to make timestamp matching easier

    Args:
        timestamp (timestamp): input timestamp

    Returns:
        timestamp: rounded timestamp
    """
    ts_float = round(timestamp.timestamp()/(3600*24), 3)
    newTS = datetime.fromtimestamp(int(ts_float*3600*24))
    return newTS.astimezone(timestamp.tzinfo)


def getCurrentTimestamp(tz):
    """Get the timestamp of the current date/time located in a specific timezone

    Args:
        tz (string): time zone code

    Returns:
        timestamp: current timestamp located in the specific timezone
    """
    timestamp = datetime.now()
    timestamp = changeTimeZone(roundTimestamp(timestamp), tz)
    timestamp = changeTimeZone(roundTimestamp(timestamp),"utc")
    return timestamp
    
    


if __name__ == '__main__':
    #time_str = "01:10:00"
    #timestamp = getTimeStampFromTime(time_str)
    #print(timestamp)
    #


    #date_str_list = ["2021-10-28T00:00:00+02:00",
    #"2021-10-29T00:00:00+02:00",
    #"2021-10-30T00:00:00+02:00",
    #"2021-10-31T00:00:00+02:00",
    #"2021-11-01T23:00:00+01:00",
    #"2021-11-02T23:00:00+01:00",
    #"2021-11-03T23:00:00+01:00",
    #"2021-11-04T23:00:00+01:00",
    #"2021-11-05T23:00:00+01:00"]
#
    #for date_str in date_str_list:
    #    print(date_str)
    #    utc_timestamp = getTimestampFromStr(date_str, "Europe/Paris", "utc")
    #    print(utc_timestamp)
    #    print("")

    

    #timestamp = getTimestampFromDate(date_str, "Europe/Paris")

    #local_timestamp = getTimestampFromStr("2021-08-21T03:00:00+02", "Europe/Paris")
    #print("UTC:", utc_timestamp, " - local:",  local_timestamp)
    #test = {"UTC" : utc_timestamp, "local" :  local_timestamp}
    #print(test)

    #timestamp = getTimestampFromStr("2023-07-23 12:00:00", "Europe/Paris", "utc")
    #print(timestamp)
    print(getCurrentTimestamp("Europe/Paris"))



