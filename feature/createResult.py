import MySQLdb

def convertTup2List(Tup):
    List=[]
    for TupElem in Tup:
        List.append(TupElem[0])
    if List!=[]:
        return List
    else:
        return None

Database=MySQLdb.connect(
    "202.120.37.201",
    "admin",
    "tianchi",
    "tianchi")
Cursor=Database.cursor()

SQL="select * from washed.item_subset;"
Cursor.execute(SQL)
ItemTup=Cursor.fetchall()
SQL="select distinct user_id from washed.tianchi_p;"
Cursor.execute(SQL)
UserTup=Cursor.fetchall()

ItemList=convertTup2List(ItemTup)
UserList=convertTup2List(UserTup)

SQL="drop table if exists result.label;"
Cursor.execute(SQL)
SQL="create table result.label(user_id int,item_id int);"
Cursor.execute(SQL)

for i in ItemList:
    SQL="insert into result.label(user_id,item_id) values(%s,%s);"
    Cursor.executemany(SQL,[(u,i) for u in UserList])
    Database.commit()
    print i

Database.close()