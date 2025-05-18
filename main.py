import re
from source import EpisodeParser, init_db, get_db_connection, Episode
from xml.etree import ElementTree
import httpx
import time
from datetime import datetime
from pathlib import Path

rihc = 'https://therestishistory.supportingcast.fm/content/eyJ0IjoicCIsImMiOiIxNDc3IiwidSI6Ijg2MzM0OCIsImQiOiIxNjM0OTQwODcyIiwiayI6MjY3fXwzMjIzYTc1NWIxYzRiMzAxYTg4ZTU3YTgyZTg2MDRkZGMzMTk5MmNkOWNlZDM3ZDM3YmY4YzliYzUzZDYyMWMw.rss'
# TODO: cross reference from rest is history normal feed for missing episode numbers


def get_episodes_from_rss(rss_url):
    
    #rss_url = "https://feeds.megaphone.fm/GLT4787413333"
    response = httpx.get(rss_url)
    
    
    if response.status_code == 200:
        rss_feed = response.text
        # parse the RSS feed using xml.etree.ElementTree
    
        root = ElementTree.fromstring(rss_feed)
        episodes_xml = root.findall("./channel/item")
        parser = EpisodeParser()
        eps = [parser.parse(episode) for episode in episodes_xml]
        return eps

def register_episode_download(episode_id, filename,db):
    cur = db.cursor()
    cur.execute("INSERT INTO episode_files (episode_id, filename) VALUES(?, ?) ", (episode_id,filename))
    db.commit()

def sanitize_filename(filename):
    # Replace invalid filename characters with underscore
    return re.sub(r'[<>:"/\\|?*\0]', '-', filename)

def download_episode_audio_file(url, title):
    r = httpx.get(url, timeout=120, follow_redirects=True)
    # TODO: create some kind of filesystem repo for these files rather than just store them in the main directory
    file_name = sanitize_filename(f"rih_{title.replace(':','-')}.mp3")
    with open(file_name,'wb') as f:
        f.write(r.content)
    return file_name

def find_episodes_to_download(db,max_files:int = 500):
    missing_eps_sql = """
    select episodes.id, episodes.url, episodes.title
    from episodes
    left join episode_files on episodes.id = episode_files.episode_id
    where episode_files.id is null;
    """
    cur = db.cursor()
    result = cur.execute(missing_eps_sql)
    
    eps_for_dl = result.fetchall()

    for ep in eps_for_dl[:max_files]:
        print(f'Downloading Episode: {ep[2]}')
        start_time = time.perf_counter()
        dl_name = download_episode_audio_file(ep[1],ep[2])
        register_episode_download(ep[0],dl_name, db)
        end_time = time.perf_counter()
        print(f"{dl_name} took {(end_time - start_time):.4f}s")

def latest_episode(db):
    """get latest episode from db"""
    latest_sql = """
    SELECT max(pub_date)
    from episodes;
    """
    cur = db.cursor()
    result = cur.execute(latest_sql)
    last_date = result.fetchone()[0]
    if last_date:
        return datetime.fromisoformat(last_date)
    return None




def main(db):
    print("Hello from rest-is-hist!")
    eps = get_episodes_from_rss(rihc)
    # find latest episode from db, or none
    latest_pub_date = latest_episode(db)
    if latest_pub_date:
        insert_data = [e.to_sql() for e in eps if e.date > latest_pub_date]
    else:
        insert_data = [e.to_sql() for e in eps]
    
    if insert_data:
        print(f"Found {len(insert_data)} to add.")
        cur = db.cursor()
        cur.executemany("INSERT INTO episodes (title,description,pub_date,url,number) VALUES(?, ?, ?, ?, ?) ", insert_data)
        db.commit()

    find_episodes_to_download(db)

if __name__ == "__main__":
    init_db()
    db = get_db_connection()
    main(db)

