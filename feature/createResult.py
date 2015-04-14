import MySQLdb

def convertTup2List(Tup):
    List=[]
    for TupElem in Tup:
        List.append(TupElem[0])
    if List==[]:
        return None
    else:
        return List

Database=MySQLdb.connect(
    "202.120.37.201",
    "admin",
    "tianchi",
    "tianchi")
Cursor=Database.cursor()

SQL="drop table if exists result.train;"
Cursor.execute(SQL)
SQL="create table result.train(user_id int,item_id int);"
Cursor.execute(SQL)

SQL="select distinct user_id from washed.tianchi_p_ten_1_30;"
Cursor.execute(SQL)
UserTup=Cursor.fetchall()
UserList=convertTup2List(UserTup)

for Counter,User in enumerate(UserList):
    SQL="select distinct item_id from washed.tianchi_p_ten_1_30 where user_id=%d;"%User
    Cursor.execute(SQL)
    ItemTup=Cursor.fetchall()
    ItemList=convertTup2List(ItemTup)
    if ItemList!=None:
        LineList=[(User,Item) for Item in ItemList]
        SQL="insert into result.train(user_id,item_id) values(%s,%s);"
        Cursor.executemany(SQL,LineList)
        Database.commit()
    print Counter,User

Database.close()