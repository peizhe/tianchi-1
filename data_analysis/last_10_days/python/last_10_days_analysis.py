# encoding: utf-8
#!/usr/bin/python

import MySQLdb

Database=MySQLdb.connect(
    "202.120.37.201",
    "admin",
    "tianchi",
    "tianchi")
Cursor=Database.cursor()

def formatTuple(Tup,Ind):
    Result=[]
    for TupElem in Tup:
        Result.append(TupElem[Ind])
    return Result

def getUserId():
    #File=open("userid.txt",'wb')
    SQL='select distinct user_id from user'
    Cursor.execute(SQL)
    Temp=Cursor.fetchall()
    ResultList=formatTuple(Temp,0)
    #File.write(str(ResultList))
    #File.close()
    return ResultList

def getStatisticInfo(UserIdList):
    File=open("result.txt",'wb')
    SQL='''select count(*) from user 
    where user_id=%s and behavior_type=%s and event_date between 22 and 31
    '''
    for User in UserIdList:
        TempStr=""
        for BehaviorType in range(1,5):
            Cursor.execute(SQL,(User,BehaviorType))
            Temp=Cursor.fetchone()
            TempStr=TempStr+str(Temp[0])+' '
        print TempStr
        File.write(str(BehaviorType)+' '+str(User)+' '+TempStr+'\n')
    File.close()
    return

UserIdList=getUserId()
getStatisticInfo(UserIdList)