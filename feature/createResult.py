import MySQLdb

Database=MySQLdb.connect(
    "202.120.37.201",
    "admin",
    "tianchi",
    "tianchi")
Cursor=Database.cursor()

for Date in range(1,32):
    SQL="drop table if exists result.label_%d_pos;"%Date
    Cursor.execute(SQL)
    SQL="""create table result.label_%d_pos as
    (select user_id, item_id, if(behavior_type=4,1,0) as label
        from washed.tianchi_p
        where behavior_type=4 and event_date=%d)   
    """%(Date,Date)
    Cursor.execute(SQL)
    SQL="drop table if exists result.label_%d_neg;"%Date
    Cursor.execute(SQL)
    SQL="""create table result.label_%d_neg as
    (select user_id , item_id, if(behavior_type=4,1,0) as label
        from washed.tianchi_p
        where behavior_type!=4 and event_date=%d
        order by rand() limit 2000)   
    """%(Date,Date)
    Cursor.execute(SQL)
    
    print Date
Database.close()