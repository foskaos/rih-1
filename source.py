import re
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
import httpx
from datetime import datetime
from dataclasses import dataclass
import sqlite3
import os

DATABASE_FILE = "rih.db"


def init_db():
    if not os.path.exists(DATABASE_FILE):
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()
        # create episodes table
        ep_table_sql = """
        CREATE TABLE episodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        description TEXT,
        pub_date TEXT,
        url TEXT,
        number INTEGER,
        created_dt default current_timestamp
       )
        """
        down_load_table_sql = """
        CREATE TABLE episode_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT,
        episode_id INTEGER NOT NULL,
        downloaded_dt default CURRENT_TIMESTAMP,
        FOREIGN KEY(episode_id) REFERENCES episodes(id)
        )
        """
        cursor.execute(ep_table_sql)
        conn.commit()
        cursor.execute(down_load_table_sql)
        conn.commit()
        conn.close()


def get_db_connection():
    return sqlite3.connect(DATABASE_FILE)


@dataclass
class Episode:
    title: str
    description: str
    date: datetime
    url: str
    number: int | None

    def date_as_sqlite_text(self):
        """YYYY-MM-DD HH:MM:SS.SSS"""

        return self.date.isoformat()
    
    def to_sql(self):

        return (self.title, self.description, self.date_as_sqlite_text(), self.url, self.number)

class EpisodeParser:
    @staticmethod
    def parse_pub_date(pub_date: str) -> datetime:
        date_pattern = re.compile(
            r"(\d\d)\s(\w\w\w)\s(\d\d\d\d)\s(\d\d:\d\d:\d\d)", re.IGNORECASE
        )

        parsed_components = date_pattern.search(pub_date)
        months_lookup = {
            "Jan": 1,
            "Feb": 2,
            "Mar": 3,
            "Apr": 4,
            "May": 5,
            "Jun": 6,
            "Jul": 7,
            "Aug": 8,
            "Sep": 9,
            "Oct": 10,
            "Nov": 11,
            "Dec": 12,
        }

        year = int(parsed_components.group(3))
        month = months_lookup[parsed_components.group(2)]
        day = int(parsed_components.group(1))

        hour, minute, seconds = parsed_components.group(4).split(":")
        parsed_date = datetime(
            year=year, month=month, day=day, hour=int(hour), minute=int(minute)
        )

        return parsed_date

    @classmethod
    def parse(cls, element: Element):
        title = element.find("title").text
        description = element.find("description").text
        url = element.find("enclosure").attrib.get("url")
        try:
            number = element.find(
                "{http://www.itunes.com/dtds/podcast-1.0.dtd}episode"
            ).text
        except AttributeError:
            number = None
        date = cls.parse_pub_date(element.find("pubDate").text)

        return Episode(
            title=title, description=description, url=url, number=number, date=date
        )


