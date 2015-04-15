#!/usr/bin/env python
# -*- coding: utf-8 -*-  

from sklearn.externals import joblib
import MySQLdb

def converFormat(Array):
    Result=[]
    for Elem in Array:
        Result.append(int(Elem))
    return Result

# Database=MySQLdb.connect(
#     "202.120.37.201",
#     "admin",
#     "tianchi",
#     "tianchi")
# Cursor=Database.cursor()

Result=joblib.load('lr_class.model')
print Result
ResultList=converFormat(Result)
# print ResultList[:10000]

# SQL="drop table if exists result.prediction;"
# Cursor.execute(SQL)
# SQL="create table result.prediction(ifbuy int);"
# Cursor.execute(SQL)
# SQL="update result.prediction set ifbuy=%s;"
# Cursor.executemany(SQL,ResultList)
# Database.commit()

csv=file('result.csv','w+')
Temp=''
for Elem in ResultList:
    Temp+=str(Elem)+'\n'
csv.write(Temp)
csv.close()

# Database.close()