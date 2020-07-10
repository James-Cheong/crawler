import re
import requests
import json
import pymysql
import datetime
import time

records_arrival = []
records_departure = []


def fetch():
    global records_arrival, records_departure
    header = {
        'Accept-Language': 'en-US;q=0.8,en;q=0.7',
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': 'https://m.marine.gov.mo/seawayScheduled/?lang=ENG',
        'Content-Type': 'application/json; charset=UTF-8',
        'Cookie': 'ASP.NET_SessionId=y5fbe1cotiqsuq2rx34igcuv; _ga=GA1.3.1715372419.1592990866; _gid=GA1.3.32788424.1592990866; _gat=1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}

    arrival = {"local": "TP", "type": "dataArrivalXML"}
    departure = {"local": "TP", "type": "dataDepartureXML"}

    url = 'https://m.marine.gov.mo/seawayScheduled/RealTimeSailing.aspx/GetData'

    # fetch arrival data
    res = requests.post(url, headers=header, data=json.dumps(arrival)).text
    # print(res)
    a = json.loads(res)
    s = str(a)
    # print(s)
    t = re.findall("(\d+:\d+)</td>", s)
    From = re.findall(">(\w\w|\w\w \w+)</td>", s)
    status = re.findall("color:\S+\">(.*?)</span></td>", s)  # fetched all the data
    name = re.findall("berthNo\S+>\d?</td><td class=\S+>(.*?)<br />", s)

    now = datetime.datetime.now()
    tmp = now.strftime('%Y-%m-%d 11:30:00')
    eleven = datetime.datetime.strptime(tmp, '%Y-%m-%d %H:%M:%S')  # create a 11:30am datetime

    for i in range(len(t)):  # format the time list
        if now > eleven:  # if now > 10:30
            t[i] = datetime.datetime.now().strftime(f'%Y-%m-%d {t[i]}:00')
        else:
            if t[i][0] != '0' and status[i] != 'Pending':  # if it is not pending
                dt = datetime.datetime.now() - datetime.timedelta(days=1)
                t[i] = dt.strftime(f'%Y-%m-%d {t[i]}:00')
            else:
                t[i] = datetime.datetime.now().strftime(f'%Y-%m-%d {t[i]}:00')

    records_arrival = list(zip(t, From, status, name))  # format the data
    print(records_arrival)

    # fetch departure data
    res = requests.post(url, headers=header, data=json.dumps(departure)).text
    # print(res)
    a = json.loads(res)
    s = str(a)
    # print(s)
    t = re.findall("(\d+:\d+)</td>", s)
    From = re.findall(">(\w\w|\w\w \w+)</td>", s)
    status = re.findall("color:\S+\">(.*?)</span></td>", s)  # fetched all the data
    name = re.findall("berthNo\S+>\d?</td><td class=\S+>(.*?)<br />", s)

    for i in range(len(t)):  # format the time list
        if now > eleven:  # if now > 10:30
            t[i] = datetime.datetime.now().strftime(f'%Y-%m-%d {t[i]}:00')
        else:
            if t[i][0] != '0' and status[i] != 'Pending':  # if it is not pending
                dt = datetime.datetime.now() - datetime.timedelta(days=1)
                t[i] = dt.strftime(f'%Y-%m-%d {t[i]}:00')
            else:
                t[i] = datetime.datetime.now().strftime(f'%Y-%m-%d {t[i]}:00')

    records_departure = list(zip(t, From, status, name))  # format the data
    print(records_departure)


# connect to mysql
test = pymysql.connect("127.0.0.1", "root", "root", "test")
cursor = test.cursor()


def establish():
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS test")
        test.select_db("test")
        # create a table
        cursor.execute('''CREATE TABLE IF NOT EXISTS `arrivals`(`id` INT NOT NULL AUTO_INCREMENT,
                       `time` datetime DEFAULT NULL,
                       `from` varchar(15) DEFAULT NULL,
                       `status` varchar(20) DEFAULT NULL,
                       `name` varchar(20) DEFAULT NULL,
                       `insert time` datetime DEFAULT NULL,
                       PRIMARY KEY (`id`));''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS `departure`(`id` INT NOT NULL AUTO_INCREMENT,
                       `time` datetime DEFAULT NULL,
                       `to` varchar(15) DEFAULT NULL,
                       `status` varchar(20) DEFAULT NULL,
                       `name` varchar(20) DEFAULT NULL,
                       `insert time` datetime DEFAULT NULL,
                       PRIMARY KEY (`id`));''')
        print("table created")
    except:
        import traceback

        traceback.print_exc()
        test.rollback()


def insert():
    # insert data to db
    for i in range(len(records_arrival)):
        sql_arrivals = '''INSERT INTO arrivals (`time`, `from`, `status`,`name`,`insert time`) 
        SELECT %s,%s,%s,%s,now() FROM DUAL WHERE NOT EXISTS 
        (SELECT * FROM arrivals WHERE `time`=%s AND `status`=%s);'''
        cursor.execute(sql_arrivals, (str(records_arrival[i][0]), str(records_arrival[i][1]),
                                      str(records_arrival[i][2]), str(records_arrival[i][3]),
                                      str(records_arrival[i][0]),
                                      str(records_arrival[i][2])))
    for i in range(len(records_departure)):
        sql_departure = '''INSERT INTO departure (`time`, `to`, `status`,`name`,`insert time`) 
        SELECT %s,%s,%s,%s,now() FROM DUAL WHERE NOT EXISTS 
        (SELECT * FROM departure WHERE `time`=%s AND `status`=%s);'''
        cursor.execute(sql_departure, (str(records_departure[i][0]), str(records_departure[i][1]),
                                       str(records_departure[i][2]), str(records_departure[i][3]),
                                       str(records_departure[i][0]),
                                       str(records_departure[i][2])))

    test.commit()
    print("data inserted")


def main():
    establish()
    fetch()
    insert()


while True:
    main()
    time.sleep(300)
