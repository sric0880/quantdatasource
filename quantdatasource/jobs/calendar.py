from quantcalendar import CalendarAstock, CalendarCTP, timestamp_s
from quantdata import mongo_get_data


def get_astock_calendar():
    days = mongo_get_data("quantcalendar", "cn_stock")
    dates_arr = [(timestamp_s(day["_id"]), day["status"]) for day in days]
    CalendarAstock.Init(dates_arr)
    return CalendarAstock()


def get_ctpfuture_calendar():
    days = mongo_get_data("quantcalendar", "cn_future")
    dates_arr = [(timestamp_s(day["_id"]), day["status"]) for day in days]
    sessions = mongo_get_data("quantcalendar", "cn_future_sessions")
    sessions = [(s["_id"].encode("ascii"), s["market_time"]) for s in sessions]
    CalendarCTP.Init(dates_arr, sessions)
    return CalendarCTP("")
