#!/usr/bin/env python
# -*- coding: utf-8 -*-  

from sklearn.externals import joblib
import MySQLdb as mysql

def connect_database(ip='202.120.37.201',user='admin',
    pwd='tianchi',database='newresult'):

    try:
        conn=mysql.connect(ip,user,pwd,database)
        cur=conn.cursor()
        print "MySQL Connection: Success"
        return [conn,cur]
    except mysql.Error,e:
        print "MySQL Error %d:%s"%(e.args[0],e.args[1])
        return [None,None]

def fetch_data(cur,feature_table_name,table_schema='newresult'):
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

# Result=joblib.load('rf.model_new_test')
Result=joblib.load('gbrt.model_new_test')
# print Result
[conn,cur]=connect_database()
###########################################
testing_data=fetch_data(cur,'vala_ui_user')       #_item_category
###########################################
total=0
csv=file('tianchi_mobile_recommendation_predict.csv','w')
Temp='user_id,item_id\r\n'
# csv.close()# csv=file('result.csv','w+')
# Temp=''
# for Elem in ResultList:
#     Temp+=str(Elem)+'\n'
# csv.write(Temp)
# csv.close()

for i,item in enumerate(Result):
   if item[1]>0.85:
        total+=1
        Temp=Temp+str(testing_data[i][0])+','+str(testing_data[i][1])+'\r\n'
        


SQL="drop table if exists validate.prediction;"
cur.execute(SQL)
SQL="create table validate.prediction(user_id int,item_id int);"
cur.execute(SQL)

total=0;
for i,item in enumerate(Result):
 	# print item
 	if item[1]>0.85:
 		total=total+1
 		cur.execute('insert into validate.prediction values (%s,%s)',testing_data[i])



cur.close()
conn.commit()
conn.close()


print total
# print Temp
csv.write(Temp)
csv.close()
# ResultList=converFormat(Result)
# print ResultList[:10000]

# SQL="drop table if exists validate.prediction;"
# Cursor.execute(SQL)
# SQL="create table validate.prediction(ifbuy int);"
# Cursor.execute(SQL)
# SQL="update validate.prediction set ifbuy=%s;"
# Cursor.executemany(SQL,ResultList)
# Database.commit()

# csv=file('result.csv','w+')
# Temp=''
# for Elem in ResultList:
#     Temp+=str(Elem)+'\n'
# csv.write(Temp)
# csv.close()

# Database.close()