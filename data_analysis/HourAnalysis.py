# encoding: utf-8
#!/usr/bin/python

import MySQLdb
import MySQLdb.cursors

def formatResult(ResultTup):
    ResultStr=""
    for TupElem in ResultTup:
        ResultStr=ResultStr+str(TupElem[0])+' '
    return ResultStr

Database=MySQLdb.connect(
    "202.120.37.201",
    "admin",
    "tianchi",
    "tianchi")
Cursor=Database.cursor()

File=open("HourResult.txt",'wb')

for BehavType in range(1,5):
    for Day in range(1,32):
        SQL='''
        select count(*) from user where event_date=%d
        and behavior_type=%d group by event_time
        '''%(Day,BehavType)
        Cursor.execute(SQL)
        Temp=Cursor.fetchall()
        Temp=formatResult(Temp)
        print Temp
        File.write(str(BehavType)+' '+str(Day)+' '+Temp+'\n')
File.close()
Database.close()