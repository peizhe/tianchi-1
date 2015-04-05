/*Database:washed 清洗数据库*/

/*一.选出商品子集P*/   /*已执行 ！*/
drop table washed.item_subset;
create table washed.item_subset as 
select distinct item_id from tianchi.item order by item_id ;


/*二.过滤掉对非商品子集内商品的操作记录：第一次过滤*/  /*已执行 ！*/

create table washed.tianchi_p as
  select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time from
  (select * from tianchi.user)a
  right outer join
  (select item_id as item_id_1 from washed.item_subset)b
  on (a.item_id=b.item_id_1);



/*三.选出后十天有操作的人*/

  /* 1.用户在最后十天的点击数、收藏数、加购物车数以及购买数*/    /*已执行 ！*/
create table washed.user_click_10 as
	select user_id,count(behavior_type) as clicktime from washed.tianchi_p where (behavior_type=1) and (event_date>21)  group by user_id;

create table washed.user_favor_10 as
	select user_id,count(behavior_type) as favortime from washed.tianchi_p where (behavior_type=2) and (event_date>21) group by user_id;

create table washed.user_cart_10 as
	select user_id,count(behavior_type) as carttime from washed.tianchi_p where (behavior_type=3) and (event_date>21) group by user_id;

create table washed.user_buy_10 as
	select user_id,count(behavior_type) as buytime from washed.tianchi_p where (behavior_type=4) and (event_date>21) group by user_id;

   /* 2.用户在最后十天的操作行为总表*/   /*已执行 ！*/
create table washed.user_action_10 as
 	select user_id,buytime,favortime,carttime,clicktime from
 	(
 	select user_id,buytime,favortime,clicktime from
 	(
    select user_id,buytime,clicktime from
	(select user_id, clicktime from washed.user_click_10) a
	left outer join
    (select user_id as user_id_1, buytime from washed.user_buy_10) b
    on (a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2, favortime from washed.user_favor_10)d
    on (c.user_id=d.user_id_2)
    )e
    left outer join
    (select user_id as user_id_3, carttime from washed.user_cart_10)f
    on (e.user_id=f.user_id_3) order by user_id;

   /* 3.选出在最后十天有操作的用户*/    /*已执行 ！*/
create table washed.ten_action_users as
	select user_id from washed.user_action_10 
	where (buytime is not null) or (carttime is not null) or (favortime is not null) or (clicktime is not null)  
	order by user_id;


/*四.过滤掉后十天没有任何操作的人*/     /*已执行 ！*/

create table washed.tianchi_p_10 as
	select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time from 
	(select * from washed.tianchi_p)a
	right outer join
	(select user_id as user_id_1 from washed.ten_action_users)b
	on(a.user_id=b.user_id_1);


/*五.过滤掉部分从未买过的用户*/
   /*一类是爬虫用户，只点不买，点超过1000次以上*/
   /*二类是从未买过的用户*/
   /*有待进一步探讨*/

   /*1.选出用户在所有天的购买次数、点击次数*/   /*已执行 ！*/
create table washed.user_buy as
	select user_id,count(behavior_type) as buytime from washed.tianchi_p_10 where behavior_type=4 group by user_id;


create table washed.user_click as
	select user_id,count(behavior_type) as clicktime from washed.tianchi_p_10 where behavior_type=1 group by user_id;

   /*2.过滤掉从未买过用户的记录,剩下的是有购买记录的人*/   /*已执行 ！*/

create table washed.tianchi_p_10_buy as
    select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time from
	(
	select user_id,item_id,item_category,behavior_type,user_geohash,event_date,event_time,buytime from
	(select * from washed.tianchi_p_10)a
	left outer join
	(select user_id as user_id_1,buytime from washed.user_buy)b
	on(a.user_id=b.user_id_1)
	)c
	where buytime is not null;

/*总结：*/  
/* tianchi_p          中包含所有用户对商品子集中商品的操作记录*/
/* tianchi_p_10       中包含所有在最后十天有操作的用户对商品子集中商品的操作记录*/
/* tianchi_p_10_buy 中包含所有在最后十天有操作且有购买记录的用户对商品子集中商品的操作记录*/

/*另外值得注意的是：要对从未购买过商品子集中商品，但是最后几天对这些商品有频繁交互行为的用户单独进行挖掘*/

