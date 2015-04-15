#!/usr/bin/env python
# -*- coding: utf-8 -*-  

from sklearn.externals import joblib
import MySQLdb as mysql

def connect_database(ip='202.120.37.201',user='admin',
    pwd='tianchi',database='result'):

    try:
        conn=mysql.connect(ip,user,pwd,database)
        cur=conn.cursor()
        print "MySQL Connection: Success"
        return [conn,cur]
    except mysql.Error,e:
        print "MySQL Error %d:%s"%(e.args[0],e.args[1])
        return [None,None]

def fetch_data(cur,feature_table_name,table_schema='result'):
    '''
        fetch_data: Get the np.array type from database
        param:
            cur: cursor
            feature_table_name: table name of user-item feature
            table_schema: database name, default is 'tianchi'
            except_fields: Since we only need the feature values except user-id,item-id 
                            record-id, we have to eliminate them from feature fields,
                            default is [('user_id',),('item_id',)]
    '''
    try:
        sql_select_data='''
            select user_id,item_id from %s
        '''%(feature_table_name)
        data_count=cur.execute(sql_select_data)
        data_tuples=cur.fetchall()
        return data_tuples
    except mysql.Error,e:
        print "MySQL Error %d:%s"%(e.args[0],e.args[1])


def main(args):
    pass
def disconnect_database(conn,cur):
    try:
        cur.close()
        conn.close()
        print "MySQL Disconnection: Success"
    except mysql.Error,e:
        print "MySQL Error %d:%s"%(e.args[0],e.args[1])
    
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

Result=joblib.load('gbrt.model_new_trainval')
# print Result
[conn,cur]=connect_database()
testing_data=fetch_data(cur,'pre_ui_user_item_category')
total=0
csv=file('result.csv','w')
Temp='user_id,item_id\n'
# csv.close()# csv=file('result.csv','w+')
# Temp=''
# for Elem in ResultList:
#     Temp+=str(Elem)+'\n'
# csv.write(Temp)
# csv.close()
for i,item in enumerate(Result):
    if item[1]>0.8:
        total+=1
        Temp=Temp+str(testing_data[i][0])+','+str(testing_data[i][1])+'\n'

# SQL="drop table if exists result.prediction;"
# cur.execute(SQL)
# SQL="create table result.prediction(user_id int,item_id int);"
# cur.execute(SQL)
# for i,item in enumerate(Result):
# 	# print item
# 	if item[1]>0.8:
# 		total=total+1
# 		cur.execute('insert into result.prediction values (%s,%s)',testing_data[i])
# cur.close();
# conn.commit()
# conn.close
print total
print Temp
csv.write(Temp)
csv.close()
# ResultList=converFormat(Result)
# print ResultList[:10000]

# SQL="drop table if exists result.prediction;"
# Cursor.execute(SQL)
# SQL="create table result.prediction(ifbuy int);"
# Cursor.execute(SQL)
# SQL="update result.prediction set ifbuy=%s;"
# Cursor.executemany(SQL,ResultList)
# Database.commit()

# csv=file('result.csv','w+')
# Temp=''
# for Elem in ResultList:
#     Temp+=str(Elem)+'\n'
# csv.write(Temp)
# csv.close()

# Database.close()