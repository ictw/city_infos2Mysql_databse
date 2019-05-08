# -*-coding:utf-8-*-
import json
import pymysql
from Mysql_config import *

"""
=======================================
    Version：1.0
    Revise：1
    Date：2019年05月08日
    Author：suofeiya 
    
    ★★★Shortcomings:
        1.单线程，速度较慢
        2.部分代码冗杂
        3.插入方式耗费资源大
    
    代码云备份于:https://github.com/ictw/city_infos2Mysql_databse
=======================================
"""


class Json2Mysql():
    def __init__(self):
        self.host_name = HOST
        self.port = PORT
        self.user_name = USER
        self.passwd = PASSWD
        self.db = DB
        self.charset = CHARSET

    def run(self):
        """

        :return:
        """
        conn = pymysql.connect(host=self.host_name, port=self.port, user=self.user_name, passwd=self.passwd, db=self.db,
                               charset=self.charset)
        cursor = conn.cursor()
        sql_createTable = """
        CREATE TABLE city_infos (
        id  VARCHAR(32),
        cityEn  VARCHAR(32),
        cityZh VARCHAR(32),
        provinceEn VARCHAR(32),
        provinceZh VARCHAR(32),
        leaderEn VARCHAR(32),
        leaderZh VARCHAR(32),
        lat VARCHAR(32), 
        lon VARCHAR(32)) ENGINE=InnoDB DEFAULT CHARSET=gbk ROW_FORMAT=COMPACT;
        """
        # 首次运行需要先创建表
        # cursor.execute(sql_createTable)
        # 如果表已经存在，则删除重新创建
        # cursor.execute("DROP TABLE IF EXISTS review")
        cursor.execute("SELECT VERSION()")
        data = cursor.fetchone()
        print("MySQL数据库已成功连接!!!\nDatabase version : %s " % data)

        with open('./city.json', 'r', encoding='utf-8') as fb:
            city_data = json.load(fb)
            line_num = 0
            result = []
            for data in city_data:
                try:
                    line_num += 1
                    print('\n==================正在读取第%s行の数据==================' % line_num)
                    result.append(
                        (data['id'], data['cityEn'], data['cityZh'], data['provinceEn'], data['provinceZh'],
                         data['leaderEn'], data['leaderZh'], data['lat'], data['lon'])
                    )
                    print(result)
                    sql_insert = 'INSERT INTO city_infos(id,cityEn,cityZh,provinceEn,provinceZh,leaderEn,leaderZh,lat,lon) VALUES (%s, %s, %s, %s,%s, %s, %s, %s, %s)'
                    cursor = conn.cursor()
                    cursor.executemany(sql_insert, result)
                    conn.commit()
                    result.clear()
                    print('insert to Mysql SUCCESSFUL!!!')
                except Exception as e:
                    conn.rollback()
                    print('ERROR：数据插入失败！\n原因如下:' + str(e))
                    break
            print('\n>>>总计 %s 条数据<<<' % line_num)


if __name__ == '__main__':
    json2mysql = Json2Mysql()
    json2mysql.run()
