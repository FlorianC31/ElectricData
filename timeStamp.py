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
def getTimestampFromDate(date_str):    
    return datetime.strptime(date_str, "%Y-%m-%d")

# @brief getEndDate: get the end date from a start date and an delta time
# @delta_type: type of the delta ("months" or "days")
# @delta_value: the value of the delta
def getEndDate(start_date, delta_type, delta_value):
    if (delta_type == "months"):
        end_date = start_date + timedelta(days=delta_value*30)
    else:
        end_date = start_date + timedelta(days=delta_value)

    return min(end_date, datetime.now().date())


if __name__ == '__main__':
    #time_str = "01:10:00"
    #timestamp = getTimeStampFromTime(time_str)
    #print(timestamp)
    #
    #date_str = "2023-07-23"
    #timestamp = getTimeStampFromDate(date_str)
    #print(timestamp)

    local_timestamp = getTimestampFromStr("2021-08-21T03:00:00+02", "Europe/Paris")
    utc_timestamp = getTimestampFromStr("2021-08-21T03:00:00+02", "Europe/Paris", "utc")
    print("UTC:", utc_timestamp, " - local:",  local_timestamp)
    test = {"UTC" : utc_timestamp, "local" :  local_timestamp}
    print(test)

