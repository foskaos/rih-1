from source import EpisodeParser, init_db, get_db_connection 
from xml.etree import ElementTree
import httpx
from datetime import datetime

def main(db):
    print("Hello from rest-is-hist!")
    rss_url = "https://feeds.megaphone.fm/GLT4787413333"
    response = httpx.get(rss_url)
    
    
    if response.status_code == 200:
        rss_feed = response.text
        # parse the RSS feed using xml.etree.ElementTree
    
        root = ElementTree.fromstring(rss_feed)
        episodes_xml = root.findall("./channel/item")
        parser = EpisodeParser()
        eps = [parser.parse(episode) for episode in episodes_xml]
        insert_data = [e.to_sql() for e in eps]
        cur = db.cursor()
        cur.executemany("INSERT INTO episodes (title,description,pub_date,url,number) VALUES(?, ?, ?, ?, ?) ", insert_data)
        db.commit()
        
if __name__ == "__main__":
    init_db()
    db = get_db_connection()
    main(db)

