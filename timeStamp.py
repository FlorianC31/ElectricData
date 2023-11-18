from datetime import datetime, timedelta
import pytz


# @brief getTimestampFromStr: transform a string timestamp in actual timestamp and optionally transfer its timezone
# @param timestamp_str : string timestamp
# @param source_time_zone : (optional) source time zone, default : None (corresponding to UTC)
# @param dest_time_zone : (optional) destination time zone, default : None (no timezone transfer)
# @return : a timestamp in local time zone
def getTimestampFromStr(timestamp_str, source_time_zone = None, dest_time_zone = None):
    try:
        timestamp = datetime.strptime(timestamp_str[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        timestamp = datetime.strptime(timestamp_str[:19], "%Y-%m-%d %H:%M:%S")
    if source_time_zone == None or source_time_zone == "utc":
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
    
    
# @brief getTimestampFromTime: generate a timestamp from a string time
# @param time_str : string time with "%H:%M:%S" format
# @return : a timestamp without timezone localisation
def getTimestampFromTime(time_str):
    return datetime.strptime(time_str, "%H:%M:%S")


# @brief getTimestampFromDate: generate a timestamp from a string date
# @param date_str : date time with "%Y-%m-%d" format
# @return : a timestamp iwithout timezone localisation
def getTimestampFromDate(date_str, timezone):
    start_of_day_ts_str = date_str + " 00:00:00"
    return (getTimestampFromStr(start_of_day_ts_str, timezone, "utc") + timedelta(days = 1),
            getTimestampFromStr(start_of_day_ts_str, timezone, timezone) + timedelta(days = 1))


# @brief getEndDate: get the end date from a start date and an delta time
# @delta_type: type of the delta ("months" or "days")
# @delta_value: the value of the delta
def getEndDate(start_date, delta_value):
    end_date = start_date + timedelta(days=delta_value)
    return min(end_date, datetime.now().date())


# @brief timestamp2str: get the iso string format of the timestamp
# @timestamp: timestamp to convert
def timestamp2str(timestamp):
    return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def changeTimeZone(timestamp, new_tz):

    if new_tz == "utc":
        tz = pytz.utc
    else:
        tz = pytz.timezone(new_tz)

    return timestamp.astimezone(tz)



if __name__ == '__main__':
    #time_str = "01:10:00"
    #timestamp = getTimeStampFromTime(time_str)
    #print(timestamp)
    #


    date_str_list = ["2021-10-28T00:00:00+02:00",
    "2021-10-29T00:00:00+02:00",
    "2021-10-30T00:00:00+02:00",
    "2021-10-31T00:00:00+02:00",
    "2021-11-01T23:00:00+01:00",
    "2021-11-02T23:00:00+01:00",
    "2021-11-03T23:00:00+01:00",
    "2021-11-04T23:00:00+01:00",
    "2021-11-05T23:00:00+01:00"]

    for date_str in date_str_list:
        print(date_str)
        utc_timestamp = getTimestampFromStr(date_str, "Europe/Paris", "utc")
        print(utc_timestamp)
        print("")


    #timestamp = getTimestampFromDate(date_str, "Europe/Paris")

    #local_timestamp = getTimestampFromStr("2021-08-21T03:00:00+02", "Europe/Paris")
    #print("UTC:", utc_timestamp, " - local:",  local_timestamp)
    #test = {"UTC" : utc_timestamp, "local" :  local_timestamp}
    #print(test)

    #timestamp = getTimestampFromStr("2023-07-23 12:00:00", "Europe/Paris", "utc")
    #print(timestamp)
    #timestamp = getTimestampFromStr("2023-11-23 12:00:00", "Europe/Paris", "utc")
    #print(timestamp)
