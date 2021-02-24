import requests,json,re,cloudscraper,datetime,time
from bs4 import BeautifulSoup
import psycopg2


def ptt_crawler():
    article_href = []
    titles = []
    r = requests.get("https://www.ptt.cc/bbs/Studyabroad/index.html")
    soup = BeautifulSoup(r.text,"html.parser")
    results = soup.select("div.title")[:10]
    for item in results:
        item_href = item.select_one("a").get("href")
        item_title = item.text
        article_href.append("https://www.ptt.cc"+item_href)
        titles.append(item_title[1:-1])
    return article_href,titles

def dcard_crawler():
    article_href = []
    titles = []
    url = 'https://www.dcard.tw/service/api/v2/forums/studyabroad/posts?limit=10'
    scraper = cloudscraper.create_scraper()
    req = scraper.get(url).text
    j_ls = json.loads(req)
    for i in range(len(j_ls)):
        titles.append(j_ls[i]['title'])
        article_href.append('https://www.dcard.tw/f/studyabroad/p/'+str(j_ls[i]['id']))
    return article_href,titles


def ptt_db(t,l):
    conn = psycopg2.connect(database="", user="", password="", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    create =('''CREATE TABLE PTT
       (ID  SERIAL PRIMARY KEY,
       TITLE           TEXT    NOT NULL,
       LINK            TEXT     NOT NULL,
       COUNT        INT);''')
    insert = """INSERT INTO PTT (TITLE,LINK) VALUES (%s, %s )"""
    update= """UPDATE PTT set TITLE = %s,LINK = %s where ID = %s; """
    #cur.execute("delete from PTT;")
    #cur.execute("drop table PTT;")
    for i in range(len(t)):
        res = [t[i],l[i]]
        cur.execute(update,(t[i],l[i],i+1))
    conn.commit()
    cur.execute("SELECT id, title, link,count from PTT;")
    rows = cur.fetchall()
    for row in rows:
        print ("ID = ", row[0])
        print("title = ", row[1])
        print ("link = ", row[2])
        print ("count = ", row[3])
        print(' ')
    print ("Table updated successfully")
    conn.close()

def dcard_db(t,l):
    conn = psycopg2.connect(database="", user="", password="", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    create = ('''CREATE TABLE DCARD
       (ID  SERIAL PRIMARY KEY,
       TITLE           TEXT    NOT NULL,
       LINK            TEXT     NOT NULL,
       COUNT        INT);''')
    insert = """INSERT INTO DCARD (TITLE,LINK) VALUES (%s, %s )"""
    update= """UPDATE DCARD set TITLE = %s,LINK = %s where ID = %s; """
    #cur.execute("delete from DCARD;")
    #cur.execute("drop table DCARD;")
    for i in range(len(t)):
        res = [t[i],l[i],i+1]
        cur.execute(update,(t[i],l[i],i+1))
    conn.commit()
    cur.execute("SELECT id, title, link,count from DCARD;")
    rows = cur.fetchall()
    for row in rows:
        print ("ID = ", row[0])
        print("title = ", row[1])
        print ("link = ", row[2])
        print ("count = ", row[3])
        print(' ')
    print ("Table updated successfully")
    conn.close()


# 每天凌晨一點啟動
def activate(h=1,m=0):
    while True:
        now = datetime.datetime.now()
        print(now.hour, now.minute)
        if now.hour == h and now.minute == m:
            ptt_link,ptt_title = ptt_crawler()
            dcard_link,dcard_title = dcard_crawler()
            ptt_db(ptt_title,ptt_link)
            dcard_db(dcard_title,dcard_link)
        time.sleep(600)
    
activate()
