# encoding: utf-8
#!/usr/bin/python

import MySQLdb

Database=MySQLdb.connect(
    "202.120.37.201",
    "admin",
    "tianchi",
    "tianchi")
Cursor=Database.cursor()

def formatTuple(Tup):
    Result=""
    for TupLine in Tup:
        for TupElem in TupLine:
            if TupElem==None:
                TupElem=0
            Result=Result+str(TupElem)+' '
        Result=Result+'\n'
    return Result

def getStatisticInfo():
    File=open("result.txt",'wb')
    SQL='''select * from last_10_info
    '''
    Cursor.execute(SQL)
    Temp=Cursor.fetchall()
    TempStr=formatTuple(Temp)
    print TempStr
    File.write(TempStr)
    File.close()
    return

getStatisticInfo()