抽取操作记录的SQL语句

/*创建用户操作行为总次数的统计表
create table washed.user_buy as
	select user_id,count(behavior_type) as buytime from washed.tianchi_p where behavior_type=4 group by user_id;

create table washed.user_click as
	select user_id,count(behavior_type) as clicktime from washed.tianchi_p where behavior_type=1 group by user_id;

create table washed.user_favor as
	select user_id,count(behavior_type) as favortime from washed.tianchi_p where behavior_type=2 group by user_id;

create table washed.user_cart as
	select user_id,count(behavior_type) as carttime from washed.tianchi_p where behavior_type=3 group by user_id;

create table washed.user_action as
 	select user_id,buytime,favortime,carttime,clicktime from
 	(
 	select user_id,buytime,favortime,clicktime from
 	(
    select user_id,buytime,clicktime from
	(select user_id, clicktime from washed.user_click) a
	left outer join
    (select user_id as user_id_1, buytime from washed.user_buy) b
    on (a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2, favortime from washed.user_favor)d
    on (c.user_id=d.user_id_2)
    )e
    left outer join
    (select user_id as user_id_3, carttime from washed.user_cart)f
    on (e.user_id=f.user_id_3) order by user_id;

从tianchi_p过滤掉没有点击却有其它操作的用户，仍命名为tianchi_p
选完发现没有点击却有操作的用户个数为0
故此步骤省略*/


1.从表 tianchi_p中选出11月18日到12月17日对商品子集的操作记录作为表 tianchi_p_1_30

create table washed.tianchi_p_1_30 as
	select * from washed.tianchi_p where event_date<31;

2.从表 tianchi_p 中选出12月18日当天的所有behavior_type为4的用户商品对（user_id,item_id）作为表 tianchi_p_31_buy

选出第31日有购买行为的用户-商品对：

create table washed.tianchi_p_31_buy as
	select distinct user_id,item_id from  
	(
     select user_id,item_id from washed.tianchi_p where (event_date=31) and (behavior_type=4) order by user_id,item_id 
    )a;

3.创建用户在Day1-Day30日之间后10天操作行为的统计表

create table washed.user_buy_21_30 as
	select user_id,count(behavior_type) as buytime from washed.tianchi_p 
	where (behavior_type=4) and (event_date>=21) and  (event_date<=30)
	group by user_id;

create table washed.user_click_21_30 as
	select user_id,count(behavior_type) as clicktime from washed.tianchi_p 
	where (behavior_type=1) and (event_date>=21) and  (event_date<=30)
	group by user_id;

create table washed.user_favor_21_30 as
	select user_id,count(behavior_type) as favortime from washed.tianchi_p 
	where (behavior_type=2) and (event_date>=21) and  (event_date<=30)
	group by user_id;

create table washed.user_cart_21_30 as
	select user_id,count(behavior_type) as carttime from washed.tianchi_p 
	where (behavior_type=3) and (event_date>=21) and  (event_date<=30)
	group by user_id;

create table washed.user_action_21_30 as
 	select user_id,buytime,favortime,carttime,clicktime from
 	(
 	select user_id,buytime,favortime,clicktime from
 	(
    select user_id,buytime,clicktime from
	(select user_id, clicktime from washed.user_click_21_30) a
	left outer join
    (select user_id as user_id_1, buytime from washed.user_buy_21_30) b
    on (a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2, favortime from washed.user_favor_21_30)d
    on (c.user_id=d.user_id_2)
    )e
    left outer join
    (select user_id as user_id_3, carttime from washed.user_cart_21_30)f
    on (e.user_id=f.user_id_3) order by user_id;

4. 从表 tianchi_p_1_30 中选出在11月18日到12月17日间最后10天有操作的用户对商品子集的操作记录作为表 tianchi_p_ten_1_30

(1).先选出Day21-Day30之间有操作的用户

create table washed.action_users_21_30 as
	select user_id from washed.user_action_21_30
	where (buytime is not null) or (carttime is not null) or (favortime is not null) or (clicktime is not null)  
	order by user_id;

(2).选出最后10天有操作用户的记录作为表 tianchi_p_ten_1_30

create table washed.tianchi_p_ten_1_30 as
	select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time from 
	(select * from washed.tianchi_p_1_30)a
	right outer join
	(select user_id as user_id_1 from washed.action_users_21_30)b
	on(a.user_id=b.user_id_1) order by user_id,item_id;

5.从表 tianchi_p_ten_1_30 中选出在11月18日到12月17日间购买过的用户的操作记录作为表 tianchi_p_ten_buy_1_30

(1).首先统计Day1-Day30之间每个用户的购买次数

create table washed.user_buy_1_30 as
	select user_id,count(behavior_type) as buytime from washed.tianchi_p_ten_1_30 where behavior_type=4 group by user_id;

(2).选出购买过用户的操作记录作为表 tianchi_p_ten_buy_1_30

create table washed.tianchi_p_ten_buy_1_30 as
    select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time from
	(
	select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time,buytime from
	(select * from washed.tianchi_p_ten_1_30)a
	left outer join
	(select user_id as user_id_1,buytime from washed.user_buy_1_30)b
	on(a.user_id=b.user_id_1)
	)c
	where buytime is not null;



同样，可以根据上述方法分别创建预测用表tianchi_p_2_31、tianchi_p_ten_2_31、tianchi_p_ten_buy_2_31

语句如下：


create table washed.tianchi_p_2_31 as
	select * from washed.tianchi_p where event_date>1;





create table washed.user_buy_22_31 as
	select user_id,count(behavior_type) as buytime from washed.tianchi_p 
	where (behavior_type=4) and (event_date>=22) and  (event_date<=31)
	group by user_id;

create table washed.user_click_22_31 as
	select user_id,count(behavior_type) as clicktime from washed.tianchi_p 
	where (behavior_type=1) and (event_date>=22) and  (event_date<=31)
	group by user_id;

create table washed.user_favor_22_31 as
	select user_id,count(behavior_type) as favortime from washed.tianchi_p 
	where (behavior_type=2) and (event_date>=22) and  (event_date<=31)
	group by user_id;

create table washed.user_cart_22_31 as
	select user_id,count(behavior_type) as carttime from washed.tianchi_p 
	where (behavior_type=3) and (event_date>=22) and  (event_date<=31)
	group by user_id;

create table washed.user_action_22_31 as
 	select user_id,buytime,favortime,carttime,clicktime from
 	(
 	select user_id,buytime,favortime,clicktime from
 	(
    select user_id,buytime,clicktime from
	(select user_id, clicktime from washed.user_click_22_31) a
	left outer join
    (select user_id as user_id_1, buytime from washed.user_buy_22_31) b
    on (a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2, favortime from washed.user_favor_22_31)d
    on (c.user_id=d.user_id_2)
    )e
    left outer join
    (select user_id as user_id_3, carttime from washed.user_cart_22_31)f
    on (e.user_id=f.user_id_3) order by user_id;






create table washed.action_users_22_31 as
	select user_id from washed.user_action_22_31
	where (buytime is not null) or (carttime is not null) or (favortime is not null) or (clicktime is not null)  
	order by user_id;


create table washed.tianchi_p_ten_2_31 as
	select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time from 
	(select * from washed.tianchi_p_2_31)a
	right outer join
	(select user_id as user_id_1 from washed.action_users_22_31)b
	on(a.user_id=b.user_id_1) order by user_id,item_id;




create table washed.user_buy_2_31 as
	select user_id,count(behavior_type) as buytime from washed.tianchi_p_ten_2_31 where behavior_type=4 group by user_id;




create table washed.tianchi_p_ten_buy_2_31 as
    select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time from
	(
	select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time,buytime from
	(select * from washed.tianchi_p_ten_2_31)a
	left outer join
	(select user_id as user_id_1,buytime from washed.user_buy_2_31)b
	on(a.user_id=b.user_id_1)
	)c
	where buytime is not null;



