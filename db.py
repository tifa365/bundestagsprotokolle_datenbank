import sqlite3

DB_NAME = "bundestag_agenda.db"


def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS agenda_items
                 (id INTEGER PRIMARY KEY,
                  year INTEGER,
                  week INTEGER,
                  start TEXT,
                  end TEXT,
                  top TEXT,
                  thema TEXT,
                  beschreibung TEXT,
                  url TEXT,
                  status TEXT,
                  namentliche_abstimmung INTEGER,
                  uid TEXT,
                  dtstamp TEXT)"""
    )
    conn.commit()
    return conn
