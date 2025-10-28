import re
import calendar
from datetime import datetime
from dateutil.relativedelta import relativedelta

def extract_version(filename):
    try:
        return re.search("(fv[0-9]{1}.[0-9]{1})",filename).group()
    except:
        return 'Unknown'

def end_of_month(dt):
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    return dt.replace(day=last_day)

def extract_times_from_file(geotiff_file, interval):

    filename = geotiff_file.split('/')[-1]

    ymd_two    = re.search("([0-9]{2}[0-9]{2}[0-9]{4})_([0-9]{2}[0-9]{2}[0-9]{4})",filename)
    yyyymmdd   = re.search("([0-9]{2}[0-9]{2}[0-9]{4})",filename)
    yyyy       = re.search("(?<=[^a-zA-Z0-9])([0-9]{4})(?=[^a-zA-Z0-9])",filename)
    yyyy_yyyy  = re.search("([0-9]{4})-([0-9]{4})",filename)
    resolution = re.search("(?<=[^a-zA-Z0-9])(P[0-9]{1,2}Y)(?=[^a-zA-Z0-9])", filename)
    y1 = None
    end_datetime = None

    resolutions = {'Y':'years','M':'months','D':'days'}

    if ymd_two is not None:
        start_datetime = datetime.strptime(
            ymd_two.group().split('_')[0],"%Y%m%d"
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_datetime = datetime.strptime(
            ymd_two.group().split('_')[1],"%Y%m%d"
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif yyyymmdd is not None:
        dt_object = datetime.strptime(yyyymmdd.group(),"%Y%m%d")
        start_datetime = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif yyyy_yyyy is not None:
        y0, y1 = yyyy_yyyy.group().split('-')
        dt_object = datetime.strptime(y0,"%Y")
        start_datetime = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif yyyy is not None:
        dt_object = datetime.strptime(yyyy.group(),"%Y")
        start_datetime = dt_object.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        return None,None
    
    if end_datetime is not None:
        pass
    elif interval == 'month':
        end_datetime = end_of_month(dt_object).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif y1 is not None:
        fdt_object = datetime.strptime(y1,"%Y")
        end_datetime = (fdt_object + relativedelta(years=1) - relativedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif resolution is not None:
        # P1Y example
        r = resolution.group()
        end_datetime = (dt_object + relativedelta(
            **{resolutions[r[-1]]:int(r[1:-1])}
        ) - relativedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        # Day
        end_datetime = dt_object.strftime("%Y-%m-%d") + "T23:59:59Z"

    print(filename, start_datetime, end_datetime)
    
    return start_datetime, end_datetime