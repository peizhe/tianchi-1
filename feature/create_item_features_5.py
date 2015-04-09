import MySQLdb

Database=MySQLdb.connect(
    "202.120.37.201",
    "admin",
    "tianchi",
    "tianchi")
Cursor=Database.cursor()

OperList=['buy','clk']
TimeList=['day','eve','nt']
TimeHour={'day':tuple(range(8,19)),'eve':(19,20,21,22,23,24,0,1),'nt':(2,3,4,5,6,7)}
OperDict={'buy':4,'clk':1,'act':(1,2,3,4)}
# for Oper in OperList:
#     for Date in range(1,32):
#         for Time in TimeList:
#             print Oper,Date,Time
#             SQL='''drop table if exists feature.item_%d_%s_%s;
#             '''%(Date,Time,Oper)
#             Cursor.execute(SQL)
#             Cursor.close()
#             Cursor=Database.cursor()
#             SQL='''create table feature.item_%d_%s_%s as select item_id,ifnull(%s,0) as %s from 
# (select item_id from washed.item_subset) as a
# left outer join
# (select item_id as item_id_1, (count(*)/count(distinct(user_id))) as %s 
# from washed.tianchi_p
# where behavior_type = %d and event_time in %r group by item_id_1) as b
# on a.item_id=b.item_id_1;'''%(Date,Time,Oper,Oper,Oper,Oper,OperDict[Oper],TimeHour[Time])
#             Cursor.execute(SQL)
#             Cursor.close()
#             Cursor=Database.cursor()

for Date in range(1,32):
    for Time in TimeList:
        print Date,Time
        SQL='''drop table if exists feature.item_%d_%s_act;
        '''%(Date,Time)
        Cursor.execute(SQL)
        Cursor.close()
        Cursor=Database.cursor()
        SQL='''create table feature.item_%d_%s_act as select item_id,ifnull(act,0) as act from 
(select item_id from washed.item_subset) as a
left outer join
(select item_id as item_id_1, (count(*)/count(distinct(user_id))) as act 
from washed.tianchi_p
where event_time in %r group by item_id_1) as b
on a.item_id=b.item_id_1;'''%(Date,Time,TimeHour[Time])
        Cursor.execute(SQL)
        Cursor.close()
        Cursor=Database.cursor()

Cursor.close()
Database.close()