用户特征抽取的SQL语句

注：

粒度为30天，源表：tianchi_p_ten_buy_1_30和tianchi_p_ten_buy_2_31

对抽取的每一个特征先建一个表，之后再并成一个大表train_Nf_buy或pre_Nf_buy

以下都以tianchi_p_ten_buy_1_30为例，每一类特征写几条典型的SQL语句

用户特征（train后加user以标示）：

1.用户在最近的 1、3、7、10、14、20、28 天 (不包含双12的两天) 内的购买、点击、交互、留待所有商品的总次数
  用户在双12（Day 24、25）期间购买、点击、交互所有商品的总次数
  用户在每一周（对于训练集为：Day 1-7、Day 8-14、Day 15-21、Day 22-23 & 26-30）购买、点击、交互、留待所
  有商品的总次数 （可能和最近N天特征有重复）

用户在最近3天的购买总次数：

drop table if exists feature.train_user_l3_buy;
create table feature.train_user_l3_buy as
	select user_id,ifnull(user_l3_buy,0) as user_l3_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(behavior_type) as user_l3_buy from washed.tianchi_p_ten_buy_1_30 
	where (behavior_type=4) and (event_date>=28) and (event_date<=30) group by user_id)b
	on (a.user_id=b.user_id_1);


用户在最近7天的点击总次数：

drop table if exists feature.train_user_l7_clk;
create table feature.train_user_l7_clk as
	select user_id,ifnull(user_l7_clk,0) as user_l7_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(behavior_type) as user_l7_clk from washed.tianchi_p_ten_buy_1_30 
	where behavior_type=1 and ((event_date>=26 and event_date<=30) or (event_date>=22 and event_date<=23)) group by user_id)b
	on (a.user_id=b.user_id_1);


用户在最近10天的交互总次数（需要先建立点击、购买、收藏、购物车计数表，然后再建立交互计数表）：


create table feature.train_user_l10_clk as
	select user_id,ifnull(user_l10_clk,0) as user_l10_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(behavior_type) as user_l10_clk from washed.tianchi_p_ten_buy_1_30 
	where behavior_type=1 and ((event_date>=26 and event_date<=30) or (event_date>=19 and event_date<=23)) group by user_id)b
	on (a.user_id=b.user_id_1);


create table feature.train_user_l10_buy as
	select user_id,ifnull(user_l10_buy,0) as user_l10_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(behavior_type) as user_l10_buy from washed.tianchi_p_ten_buy_1_30 
	where behavior_type=4 and ((event_date>=26 and event_date<=30) or (event_date>=19 and event_date<=23)) group by user_id)b
	on (a.user_id=b.user_id_1);


create table feature.train_user_l10_favor as
	select user_id,ifnull(user_l10_favor,0) as user_l10_favor from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(behavior_type) as user_l10_favor from washed.tianchi_p_ten_buy_1_30 
	where behavior_type=2 and ((event_date>=26 and event_date<=30) or (event_date>=19 and event_date<=23)) group by user_id)b
	on (a.user_id=b.user_id_1);



create table feature.train_user_l10_cart as
	select user_id,ifnull(user_l10_cart,0) as user_l10_cart from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(behavior_type) as user_l10_cart from washed.tianchi_p_ten_buy_1_30 
	where behavior_type=3 and ((event_date>=26 and event_date<=30) or (event_date>=19 and event_date<=23)) group by user_id)b
	on (a.user_id=b.user_id_1);


create table feature.train_user_l10_act as
	select user_id,(user_l10_clk+user_l10_buy+user_l10_favor+user_l10_cart) as user_l10_act from
	(
 	select user_id,user_l10_buy,user_l10_favor,ifnull(user_l10_cart,0) as user_l10_cart,user_l10_clk from
 	(
 	select user_id,user_l10_buy,ifnull(user_l10_favor,0) as user_l10_favor,user_l10_clk from
 	(
    select user_id,ifnull(user_l10_buy,0) as user_l10_buy,ifnull(user_l10_clk,0) as user_l10_clk from
	(select user_id, user_l10_clk from feature.train_user_l10_clk) a
	left outer join
    (select user_id as user_id_1, user_l10_buy from feature.train_user_l10_buy) b
    on (a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2, user_l10_favor from feature.train_user_l10_favor)d
    on (c.user_id=d.user_id_2)
    )e
    left outer join
    (select user_id as user_id_3, user_l10_cart from feature.train_user_l10_cart)f
    on (e.user_id=f.user_id_3) order by user_id
    )g;


用户在最近10天的留待总次数（根据建立的收藏、购物车计数表，建立留待计数表）：

create table feature.train_user_l10_later as
	select user_id,(user_l10_favor+user_l10_cart) as user_l10_later from
	(
 	select user_id,user_l10_buy,user_l10_favor,ifnull(user_l10_cart,0) as user_l10_cart,user_l10_clk from
 	(
 	select user_id,user_l10_buy,ifnull(user_l10_favor,0) as user_l10_favor,user_l10_clk from
 	(
    select user_id,ifnull(user_l10_buy,0) as user_l10_buy,ifnull(user_l10_clk,0) as user_l10_clk from
	(select user_id, user_l10_clk from feature.train_user_l10_clk) a
	left outer join
    (select user_id as user_id_1, user_l10_buy from feature.train_user_l10_buy) b
    on (a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2, user_l10_favor from feature.train_user_l10_favor)d
    on (c.user_id=d.user_id_2)
    )e
    left outer join
    (select user_id as user_id_3, user_l10_cart from feature.train_user_l10_cart)f
    on (e.user_id=f.user_id_3) order by user_id
    )g;


2.用户在最近的 1、3、7、10、14、20、28 天内购买、点击、交互、留待了多少个不同的商品
  用户在每一周内购买、点击、交互、留待了多少个不同的商品


用户在最近3天购买了多少个不同的商品：


create table feature.train_user_l3_distinct_buy as
	select user_id,ifnull(user_l3_distinct_buy,0) as user_l3_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(item_id) as user_l3_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date>=28) and (event_date<=30) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);


用户在最近7天点击了多少个不同的商品：


create table feature.train_user_l7_distinct_clk as
	select user_id,ifnull(user_l7_distinct_clk,0) as user_l7_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(item_id) as user_l7_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and ((event_date>=26 and event_date<=30) or (event_date>=22 and event_date<=23)) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);


用户在最近10天交互了多少个不同的商品（不用再加behavior_type的约束）：

create table feature.train_user_l10_distinct_act as
	select user_id,ifnull(user_l10_distinct_act,0) as user_l10_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(item_id) as user_l10_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (event_date>=26 and event_date<=30) or (event_date>=19 and event_date<=23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

用户在最近10天留待了多少个不同的商品（behavior_type=2 or 3）：

create table feature.train_user_l10_distinct_later as
	select user_id,ifnull(user_l10_distinct_later,0) as user_l10_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,count(item_id) as user_l10_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=2 or behavior_type=3) and ((event_date>=26 and event_date<=30) or (event_date>=19 and event_date<=23)) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);


3. 用户第一次、最后一次购买、交互任意商品的日期（包含双12）

用户第一次购买任意商品的日期

drop table if exists feature.train_user_f_buy_date;
create table feature.train_user_f_buy_date as 
select user_id,min(event_date) as user_f_buy_date from washed.tianchi_p_ten_buy_1_30 
   where behavior_type=4  group by user_id; 

用户最后一次购买任意商品的日期

drop table if exists feature.train_user_l_buy_date;
create table feature.train_user_l_buy_date as 
select user_id,max(event_date) as user_l_buy_date from washed.tianchi_p_ten_buy_1_30 
   where behavior_type=4  group by user_id; 

用户第一次交互任意商品的日期

drop table if exists feature.train_user_f_act_date;
create table feature.train_user_f_act_date as 
select user_id,min(event_date) as user_f_act_date from washed.tianchi_p_ten_buy_1_30 
   group by user_id; 

用户最后一次交互任意商品的日期

drop table if exists feature.train_user_l_act_date;
create table feature.train_user_l_act_date as 
select user_id,max(event_date) as user_l_act_date from washed.tianchi_p_ten_buy_1_30 
   group by user_id; 


4. 用户在最近7天、14天、28天有多少单天购买过、交互过任意商品

用户在最近7天有多少单天购买过任意商品

drop table if exists feature.train_user_l7_h1_buy;
create table feature.train_user_l7_h1_buy as
    select user_id,ifnull(user_l7_h1_buy,0) as user_l7_h1_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (
        select user_id as user_id_1,count(event_date) as user_l7_h1_buy from
        (select distinct user_id,event_date from washed.tianchi_p_ten_buy_1_30 where 
            (event_date between 26 and 30 or event_date between 22 and 23) and behavior_type=4)b
        group by user_id
    )c
    on (a.user_id=c.user_id_1);

用户在最近14天有多少单天交互过任意商品


drop table if exists feature.train_user_l14_h1_act;
create table feature.train_user_l14_h1_act as
    select user_id,ifnull(user_l14_h1_act,0) as user_l14_h1_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (
        select user_id as user_id_1,count(event_date) as user_l14_h1_act from
        (select distinct user_id,event_date from washed.tianchi_p_ten_buy_1_30 where 
            event_date between 26 and 30 or event_date between 15 and 23)b
        group by user_id
    )c
    on (a.user_id=c.user_id_1);


5. 用户在最近 1、3、5、7 天的白天、晚上、深夜购买、点击、交互所有商品的总次数


用户在最近3天的白天交互所有商品的总次数

drop table if exists feature.train_user_l3_day_act;
create table feature.train_user_l3_day_act as
    select user_id,ifnull(user_l3_day_act,0) as user_l3_day_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_day_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 28 and 30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);


用户在最近7天的晚上点击所有商品的总次数

drop table if exists feature.train_user_l7_eve_clk;
create table feature.train_user_l7_eve_clk as
    select user_id,ifnull(user_l7_eve_clk,0) as user_l7_eve_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_eve_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);


6. 用户对所有商品在所有30天、28天内（不含双12）、双12期间、最后7天、14天 点击to购买转化率、点击to留待转化率、留待to购买转化率


用户在30天内对所有商品的 点击to购买转化率 (点击总次数/购买总次数)

drop table if exists feature.train_user_1_30_buy;
create table feature.train_user_1_30_buy as
    select user_id,count(behavior_type) as user_1_30_buy from washed.tianchi_p_ten_buy_1_30 where behavior_type=4 group by user_id;

drop table if exists feature.train_user_1_30_clk;
create table feature.train_user_1_30_clk as
    select user_id,count(behavior_type) as user_1_30_clk from washed.tianchi_p_ten_buy_1_30 where behavior_type=1 group by user_id;

drop table if exists feature.train_user_1_30_clk2buy;
create table feature.train_user_1_30_clk2buy as
    select user_id,ifnull(user_1_30_clk/user_1_30_buy,0) as user_1_30_clk2buy from
    (select user_id,user_1_30_clk from feature.train_user_1_30_clk)a
    left outer join
    (select user_id as user_id_1,user_1_30_buy from feature.train_user_1_30_buy)b
    on(a.user_id=b.user_id_1);


用户在双12期间的点击to留待转化率 （点击总次数/留待总次数)

drop table if exists feature.train_user_d12_clk;
create table feature.train_user_d12_clk as
    select user_id,user_d12_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_clk from washed.tianchi_p_ten_buy_1_30 where behavior_type=1 and event_date between 24 and 25 group by user_id)b
    on(a.user_id=b.user_id_1);

drop table if exists feature.train_user_d12_later;
create table feature.train_user_d12_later as
    select user_id,user_d12_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_later from washed.tianchi_p_ten_buy_1_30 where behavior_type between 2 and 3 and event_date between 24 and 25 group by user_id)b
    on(a.user_id=b.user_id_1);

drop table if exists feature.train_user_d12_clk2later;
create table feature.train_user_d12_clk2later as
    select user_id,ifnull(user_d12_clk/user_d12_later,0) as user_d12_clk2later from
    (
    select user_id,user_d12_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_d12_clk from feature.train_user_d12_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_d12_later from feature.train_user_d12_later)d
    on(c.user_id=d.user_id_2);