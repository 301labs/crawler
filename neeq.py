#!/usr/bin/python
# -*- coding: UTF-8 -*-

import time
import datetime
import requests
import MySQLdb, MySQLdb.cursors
from random import randint
import json

'''
-- 建表 SQL
CREATE TABLE `neeq_companies` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `address` varchar(512) DEFAULT '',
  `area` varchar(512) DEFAULT '',
  `broker` varchar(512) DEFAULT '',
  `code` varchar(128) DEFAULT '',
  `email` varchar(128) DEFAULT '',
  `englishName` varchar(256) DEFAULT '',
  `fax` varchar(128) DEFAULT '',
  `industry` varchar(256) DEFAULT '',
  `legalRepresentative` varchar(256) DEFAULT '',
  `listingDate` varchar(50) DEFAULT '',
  `name` varchar(512) DEFAULT '',
  `phone` varchar(256) DEFAULT NULL,
  `postcode` varchar(128) DEFAULT '',
  `secretaries` varchar(128) DEFAULT '',
  `shortname` varchar(256) DEFAULT '',
  `totalStockEquity` int(11) DEFAULT '0',
  `transferMode` varchar(128) DEFAULT '',
  `website` varchar(256) DEFAULT '',
  `created` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `neeq_executives` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) DEFAULT '',
  `company_id` int(11) DEFAULT NULL,
  `age` int(11) DEFAULT '0',
  `education` varchar(256) DEFAULT '',
  `gender` varchar(16) DEFAULT '',
  `job` varchar(128) DEFAULT '',
  `salary` varchar(128) DEFAULT '',
  `term` varchar(128) DEFAULT '',
  `created` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `neeq_finaces` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `company_id` int(11) DEFAULT NULL,
  `earningsPerShare` varchar(128) DEFAULT NULL,
  `income` varchar(128) DEFAULT NULL,
  `netAssets` varchar(128) DEFAULT NULL,
  `netAssetsPerShare` varchar(128) DEFAULT NULL,
  `netAssetsYield` varchar(128) DEFAULT NULL,
  `netProfit` varchar(128) DEFAULT NULL,
  `nonDistributeProfit` varchar(128) DEFAULT NULL,
  `profit` varchar(128) DEFAULT NULL,
  `totalAssets` varchar(128) DEFAULT NULL,
  `totalLiability` varchar(128) DEFAULT NULL,
  `created` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `neeq_holders` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) DEFAULT '',
  `company_id` int(11) DEFAULT NULL,
  `changeQty` varchar(128) DEFAULT '',
  `date` date DEFAULT NULL,
  `last_quantity` varchar(128) DEFAULT '',
  `limitedQuantity` varchar(128) DEFAULT '',
  `num` bigint(20) DEFAULT '0',
  `quantity` bigint(20) DEFAULT '0',
  `ratio` varchar(128) DEFAULT '',
  `unlimitedQuantity` bigint(20) DEFAULT '0',
  `created` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
'''

def proccess_list(page):
    url = "http://www.neeq.com.cn/nqxxController/nqxx.do?callback=hello&page=%d&typejb=T&xxzqdm=&xxzrlx=&xxhyzl=&xxssdq=&sortfield=xxzqdm&sorttype=asc&dicXxzbqs=&xxfcbj=&_=1471771899223" % page
    r = safe_fetch(url)
    list_json = json.loads(remove_jsoup(r.text))
    company_list = list_json[0]['content']
    for x in company_list:
        code = x.get('xxzqdm')
        name = x.get('xxzbqs')
        print "process " + name + ' ' + code
        proccess_a_company(code)

def proccess_a_company(code):
    url = "http://www.neeq.com.cn/nqhqController/detailCompany.do?callback=hello&zqdm=%s&_=1471772037178" % code
    r = safe_fetch(url)
    info_json = json.loads(remove_jsoup(r.text))

    return save_company_to_db(info_json)

def save_company_to_db(info_json):
    db, cursor = get_db()
    company = info_json.get('baseinfo')
    executives = info_json.get('executives', [])
    finance = info_json.get('finance', {})
    holders = info_json.get('topTenHolders', [])
    created = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

    # 保存公司基本信息
    print 'save company info...'
    sql = u'''
        INSERT INTO neeq_companies (`address`, `area`, `broker`, `code`, `email`,
                                 `englishName`, `fax`, `industry`, `legalRepresentative`, `listingDate`,
                                 `name`, `phone`, `postcode`, `secretaries`, `shortname`,
                                 `totalStockEquity`, `transferMode`, `website`, `created`)
        VALUES (%s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
    '''
    cursor.execute(sql, (company.get('address'), company.get('area'), company.get('broker'), company.get('code'), company.get('email'),
                         company.get('englishName'), company.get('fax'), company.get('industry'), company.get('legalRepresentative'), company.get('listingDate'),
                         company.get('name'), company.get('phone'), company.get('postcode'), company.get('secretaries'), company.get('shortname'),
                         company.get('totalStockEquity'), company.get('transferMode'), company.get('postcode'), created))
    company_id = int(cursor.lastrowid)

    # 保存高管人员数据
    print 'save executives...'
    for exe in executives:
        sql = u'''
            INSERT INTO neeq_executives (`name`, `company_id`, `age`, `education`, `gender`,
                                     `job`, `salary`, `term`, `created`)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s)
        '''
        cursor.execute(sql, (exe.get('name'), company_id, exe.get('age'), exe.get('education'), exe.get('gender'),
                             exe.get('job'), exe.get('salary'), exe.get('term'), created))

    # 保存公司财务信息
    print 'save finace...'
    sql = u'''
        INSERT INTO neeq_finaces (`company_id`, `earningsPerShare`, `income`, `netAssets`, `netAssetsPerShare`,
                                 `netAssetsYield`, `netProfit`, `nonDistributeProfit`, `profit`, `totalAssets`,
                                 `totalLiability`, `created`)
        VALUES (%s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s)
    '''
    cursor.execute(sql, (company_id, finance.get('earningsPerShare'), finance.get('income'), finance.get('netAssets'), finance.get('netAssetsPerShare'),
                         finance.get('netAssetsYield'), finance.get('netProfit'), finance.get('nonDistributeProfit'), finance.get('profit'), finance.get('totalAssets'),
                         finance.get('totalLiability'), created))

    # 保存前十股东信息
    print 'save holders...'
    for holder in holders:
        sql = u'''
            INSERT INTO neeq_holders (`name`, `company_id`, `changeQty`, `date`, `last_quantity`,
                                     `limitedQuantity`, `num`, `quantity`, `ratio`, `unlimitedQuantity`, `created`)
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s)
        '''
        cursor.execute(sql, (holder.get('name'), company_id, holder.get('changeQty'), holder.get('date'), holder.get('last_quantity'),
                             holder.get('limitedQuantity'), holder.get('num'), holder.get('quantity'), holder.get('ratio'), holder.get('unlimitedQuantity'), created))

    db.commit()
    db.close()

def remove_jsoup(str):
    return str.lstrip("hello(").rstrip(")")

def safe_fetch(url):
    max_try = 10000

    try_times = 1
    while try_times<max_try:
        try:
            print 'fetch ' + url
            req = requests.get(url)
        except Exception, e:
            sleep_time = randint(3*try_times, 10*try_times)
            print 'has some error, will sleep ' + str(sleep_time) + ' seconds'
            time.sleep(sleep_time)
            try_times = try_times + 1
        else:
            return req

def get_db():
    host = 'localhost'
    user = 'ele'
    password = '654321'
    dbname = 'wm'
    db = MySQLdb.connect(host=host, user=user, passwd=password, db=dbname, charset="utf8", cursorclass=MySQLdb.cursors.DictCursor)
    cursor = db.cursor()

    sql = 'set names utf8'
    cursor.execute(sql)

    return db, cursor

def main():
    start_page = 1
    end_page = 441

    for page in xrange(start_page, end_page):
        proccess_list(page)

if __name__ == '__main__':
    main()
