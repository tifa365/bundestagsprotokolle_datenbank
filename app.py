import os
import json
import csv
import io
from datetime import datetime, timedelta
from typing import List, Dict, Any
import xml.etree.ElementTree as ET
import icalendar
import pytz
from flask import Flask, request, Response
import sqlite3

app = Flask(__name__)

DB_NAME = "bundestag_agenda.db"
PURGE_DB = False  # Set this to True if you want to enable the purge functionality


def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def get_monday_of_iso_week(week: int, year: int) -> datetime:
    simple = datetime(year, 1, 4)  # January 4th is always in week 1
    return simple + timedelta(weeks=week - 1, days=-simple.weekday())


def format_date(date: datetime) -> str:
    return date.strftime("%Y%m%dT%H%M%SZ")


def get_week_number(date: datetime) -> int:
    return date.isocalendar()[1]


def generate_uid(date: datetime, summary: str, suffix: str) -> str:
    base = format_date(date)
    return f"{base}-{summary.lower().replace(' ', '-')[:30]}{suffix}"


@app.route("/bt-to/", defaults={"path": ""})
@app.route("/bt-to/<path:path>")
def handle_request(path):
    with get_db_connection() as conn:
        if path in ("", "/"):
            return serve_documentation()
        elif path == "data-list":
            return serve_data_list(conn)
        elif path in ("ical", "ics", "json", "xml", "csv"):
            return serve_agenda(conn, path, request.args)
        elif path == "purge" and PURGE_DB:
            return purge_db(conn)
        else:
            return "Not Found", 404


def serve_documentation():
    html = """
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Bundestag Tagesordnung API</title>
    </head>
    <body>
        <h1>Bundestag Tagesordnung API</h1>
        <p>This API provides access to the agenda of the German Bundestag.</p>
        <h2>Available Endpoints:</h2>
        <ul>
            <li>/bt-to/ical - iCal format</li>
            <li>/bt-to/json - JSON format</li>
            <li>/bt-to/xml - XML format</li>
            <li>/bt-to/csv - CSV format</li>
            <li>/bt-to/data-list - List of available data</li>
        </ul>
    </body>
    </html>
    """
    return html, 200, {"Content-Type": "text/html; charset=UTF-8"}


def serve_data_list(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT DISTINCT year, week FROM agenda_items ORDER BY year DESC, week DESC"
    )
    results = cursor.fetchall()

    data = {}
    for year, week in results:
        if year not in data:
            data[year] = []
        data[year].append(week)

    return json.dumps(data), 200, {"Content-Type": "application/json"}


def serve_agenda(conn, format: str, params: dict):
    year = int(params.get("year", datetime.now().year))
    week = params.get("week")
    month = params.get("month")
    day = params.get("day")
    status = params.get("status")
    include_na = params.get("na") == "true"
    na_alarm = params.get("naAlarm") == "true"
    show_sw = params.get("showSW") == "true"

    current_date = datetime.now()
    current_week = get_week_number(current_date)

    if year > current_date.year or (
        year == current_date.year and week and int(week) > current_week
    ):
        return "Keine Daten für zukünftige Wochen", 400, {"Content-Type": "text/plain"}

    agenda_items = get_agenda_items(conn, year, week, month, day)

    if status:
        agenda_items = [
            item
            for item in agenda_items
            if item.get("status") and status in item["status"]
        ]

    data, content_type = format_agenda_response(
        format, agenda_items, include_na, na_alarm, show_sw
    )
    return data, 200, {"Content-Type": content_type}


def get_agenda_items(
    conn, year: int, week: str = None, month: str = None, day: str = None
) -> List[Dict[str, Any]]:
    cursor = conn.cursor()
    if week:
        cursor.execute(
            "SELECT * FROM agenda_items WHERE year = ? AND week = ?", (year, int(week))
        )
    elif month:
        cursor.execute(
            "SELECT * FROM agenda_items WHERE year = ? AND substr(start, 6, 2) = ?",
            (year, month.zfill(2)),
        )
    elif day:
        cursor.execute(
            "SELECT * FROM agenda_items WHERE year = ? AND substr(start, 6, 2) = ? AND substr(start, 9, 2) = ?",
            (year, month.zfill(2), day.zfill(2)),
        )
    else:
        cursor.execute("SELECT * FROM agenda_items WHERE year = ?", (year,))

    return [dict(row) for row in cursor.fetchall()]


def format_agenda_response(
    format: str,
    agenda_items: List[Dict[str, Any]],
    include_na: bool,
    na_alarm: bool,
    show_sw: bool,
) -> tuple:
    if format in ("ical", "ics"):
        data = create_ical(agenda_items, include_na, na_alarm, show_sw)
        content_type = "text/calendar; charset=utf-8"
    elif format == "json":
        data = json.dumps(agenda_items)
        content_type = "application/json; charset=utf-8"
    elif format == "xml":
        data = create_xml(agenda_items)
        content_type = "application/xml; charset=utf-8"
    elif format == "csv":
        data = create_csv(agenda_items)
        content_type = "text/csv; charset=utf-8"
    else:
        raise ValueError(f"Unsupported format: {format}")

    return data, content_type


def create_ical(
    agenda_items: List[Dict[str, Any]], include_na: bool, na_alarm: bool, show_sw: bool
) -> str:
    cal = icalendar.Calendar()
    cal.add("version", "2.0")
    cal.add("prodid", "-//hutt.io//api.hutt.io/bt-to//")
    cal.add("calscale", "GREGORIAN")
    cal.add("x-wr-timezone", "Europe/Berlin")
    cal.add("x-wr-calname", "Tagesordnung Bundestag")
    cal.add(
        "description",
        "Dieses iCal-Feed stellt die aktuelle Tagesordnung des Plenums des Deutschen Bundestages zur Verfügung.",
    )
    cal.add("source", "https://api.hutt.io/bt-to/ical")
    cal.add("color", "#808080")

    berlin_tz = pytz.timezone("Europe/Berlin")
    weeks_with_items = set()

    for item in agenda_items:
        dtstart = datetime.fromisoformat(item["start"])
        dtend = datetime.fromisoformat(item["end"])

        if dtend <= dtstart:
            dtend = dtstart + timedelta(minutes=1)

        week_number = get_week_number(dtstart)
        weeks_with_items.add(f"{dtstart.year}-{week_number}")

        event = icalendar.Event()
        event.add("uid", item["uid"])
        event.add("dtstamp", datetime.utcnow())
        event.add("dtstart", dtstart.replace(tzinfo=berlin_tz))
        event.add("dtend", dtend.replace(tzinfo=berlin_tz))
        event.add(
            "summary",
            f"{item['top']}: {item['thema']}" if item["top"] else item["thema"],
        )
        event.add("description", item["beschreibung"])
        if item["url"]:
            event.add("url", item["url"])
        cal.add_component(event)

        if include_na and item["namentliche_abstimmung"]:
            na_event = create_na_event(item, dtend, berlin_tz, na_alarm)
            cal.add_component(na_event)

    if show_sw:
        add_sitting_week_events(cal, weeks_with_items)

    return cal.to_ical().decode("utf-8")


def create_na_event(
    item: Dict[str, Any], dtend: datetime, berlin_tz: pytz.timezone, add_alarm: bool
) -> icalendar.Event:
    na_start = dtend
    na_end = na_start + timedelta(minutes=15)

    na_event = icalendar.Event()
    na_event.add(
        "uid", generate_uid(na_start, f"Namentliche Abstimmung: {item['thema']}", "")
    )
    na_event.add("dtstamp", datetime.utcnow())
    na_event.add("dtstart", na_start.replace(tzinfo=berlin_tz))
    na_event.add("dtend", na_end.replace(tzinfo=berlin_tz))
    na_event.add("summary", f"Namentliche Abstimmung: {item['thema']}")
    na_event.add(
        "description",
        f"Namentliche Abstimmung zu {item['top']}: {item['thema']}.\n\n{item['beschreibung']}",
    )
    if item["url"]:
        na_event.add("url", item["url"])

    if add_alarm:
        alarm = icalendar.Alarm()
        alarm.add("trigger", timedelta(minutes=-15))
        alarm.add("action", "DISPLAY")
        alarm.add(
            "description",
            f"Erinnerung: Namentliche Abstimmung {item['top']}: {item['thema']}",
        )
        na_event.add_component(alarm)

    return na_event


def add_sitting_week_events(cal: icalendar.Calendar, weeks_with_items: set):
    for week in weeks_with_items:
        year, week_number = map(int, week.split("-"))
        monday = get_monday_of_iso_week(week_number, year)
        friday = monday + timedelta(days=4)

        sw_event = icalendar.Event()
        sw_event.add("uid", generate_uid(monday, "Sitzungswoche", ""))
        sw_event.add("dtstamp", datetime.utcnow())
        sw_event.add("dtstart", monday.date())
        sw_event.add("dtend", (friday + timedelta(days=1)).date())
        sw_event.add("summary", "Sitzungswoche")
        cal.add_component(sw_event)


def create_xml(agenda_items: List[Dict[str, Any]]) -> str:
    root = ET.Element("agenda")
    for item in agenda_items:
        event = ET.SubElement(root, "event")
        for key, value in item.items():
            if value is not None:
                ET.SubElement(event, key).text = str(value)
    return ET.tostring(root, encoding="unicode")


def create_csv(agenda_items: List[Dict[str, Any]]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=agenda_items[0].keys())
    writer.writeheader()
    writer.writerows(agenda_items)
    return output.getvalue()


def purge_db(conn):
    conn.execute("DELETE FROM agenda_items")
    conn.commit()
    return "Database purged", 200


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)
