用户-商品特征抽取的SQL语句

注：

粒度为30天，源表：tianchi_p_ten_buy_1_30和tianchi_p_ten_buy_2_31

对抽取的每一个特征先建一个表，之后再并成一个大表train_Nf_buy或pre_Nf_buy

以下都以tianchi_p_ten_buy_1_30为例，每一类特征写几条典型的SQL语句

用户-商品特征（train后加ui以标示）：

1.用户在最近的 1、2、3、5、7、10、12、15、20、28天内的购买、点击、交互、留待此商品的总次数
  用户在双12（Day 24、25）期间购买、点击、交互此商品的次数
  用户在每一周（对于训练集为：Day 1-7、Day 8-14、Day 15-21、Day 22-23&26-30）购买、点击、交互、留待此商品的次数 （可能和最近N天特征有重复）
  用户在最近15天的每3天购买、点击、交互此商品的次数 （可能和最近N天特征有重复）
  

  用户在最近3天购买每个商品的次数：

drop table if exists feature.train_ui_l3_buy;
create table feature.train_ui_l3_buy as
	select user_id,item_id,ifnull(ui_l3_buy,0) as ui_l3_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l3_buy from washed.tianchi_p_ten_buy_1_30 
	where (behavior_type=4) and (event_date>=28) and (event_date<=30) group by user_id,item_id)b
	on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


用户在最近7天点击每个商品的次数：

drop table if exists feature.train_ui_l7_clk;
create table feature.train_ui_l7_clk as
	select user_id,item_id,ifnull(ui_l7_clk,0) as ui_l7_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,item_id as item_id_1,count(behavior_type) as ui_l7_clk from washed.tianchi_p_ten_buy_1_30 
	where behavior_type=1 and (event_date between 26 and 30 or event_date between 22 and 23) group by user_id,item_id)b
	on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


用户在最近10天交互每个商品的次数：


drop table if exists feature.train_ui_l10_act;
create table feature.train_ui_l10_act as
	select user_id,item_id,ifnull(ui_l10_act,0) as ui_l10_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
	(select user_id as user_id_1,item_id as item_id_1,count(behavior_type) as ui_l10_act from washed.tianchi_p_ten_buy_1_30 
	where event_date between 26 and 30 or event_date between 19 and 23 group by user_id,item_id)b
	on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);



用户在最近10天留待每个商品的次数：

drop table if exists feature.train_ui_l10_later;
create table feature.train_ui_l10_later as
    select user_id,item_id,ifnull(ui_l10_later,0) as ui_l10_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,count(behavior_type) as ui_l10_later from washed.tianchi_p_ten_buy_1_30 
    where behavior_type between 2 and 3 and (event_date between 26 and 30 or event_date between 19 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);



2.用户第一次、最后一次购买、交互此商品的日期（包含双12）
用户第一次和最后一次购买、交互此商品的日期相隔多少天
用户对此商品的第一个交互天的点击数、收藏数、购买数
用户对此商品的最后一个交互天的点击数、收藏数、购买数


用户第一次购买每个商品的日期

drop table if exists feature.train_ui_f_buy_date;
create table feature.train_ui_f_buy_date as 
    select user_id,item_id,ifnull(ui_f_buy_date,0) as ui_f_buy_date from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,min(event_date) as ui_f_buy_date from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 


用户最后一次购买每个商品的日期(如果没买过，就设为一个大数如100)

drop table if exists feature.train_ui_l_buy_date;
create table feature.train_ui_l_buy_date as 
    select user_id,item_id,ifnull(ui_l_buy_date,100) as ui_l_buy_date from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,max(event_date) as ui_l_buy_date from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 


用户第一次和最后一次购买此商品的日期相隔多少天

drop table if exists feature.train_ui_fl_buy_distance;
create table feature.train_ui_fl_buy_distance as 
    select user_id,item_id,(ui_l_buy_date-ui_f_buy_date) as ui_fl_buy_distance from
    (select user_id,item_id,ui_l_buy_date from feature.train_ui_l_buy_date)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_buy_date from feature.train_ui_f_buy_date)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 


用户最后一次交互每个商品的日期

drop table if exists feature.train_ui_l_act_date;
create table feature.train_ui_l_act_date as 
    select user_id,item_id,ifnull(ui_l_act_date,100) as ui_l_act_date from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,max(event_date) as ui_l_act_date from washed.tianchi_p_ten_buy_1_30 
    group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 

用户第一次交互每个商品的日期

drop table if exists feature.train_ui_f_act_date;
create table feature.train_ui_f_act_date as 
    select user_id,item_id,ifnull(ui_f_act_date,0) as ui_f_act_date from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,min(event_date) as ui_f_act_date from washed.tianchi_p_ten_buy_1_30 
    group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 


用户第一次和最后一次交互此商品的日期相隔多少天

drop table if exists feature.train_ui_fl_act_distance;
create table feature.train_ui_fl_act_distance as 
    select user_id,item_id,(ui_l_act_date-ui_f_act_date) as ui_fl_act_distance from
    (select user_id,item_id,ui_l_act_date from feature.train_ui_l_act_date)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_act_date from feature.train_ui_f_act_date)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 

3. 用户对此商品的第一个交互天的点击数、收藏数、购买数
   用户对此商品的最后一个交互天的点击数、收藏数、购买数

用户对此商品的第一个交互天的点击数


drop table if exists feature.train_ui_f_actday_clk;
create table feature.train_ui_f_actday_clk as
    select user_id,item_id,ifnull(ui_f_actday_clk,0) as ui_f_actday_clk from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_f_actday_clk from
    (
    select user_id,item_id,ui_f_act_date,behavior_type,event_date from
    (select user_id,item_id,behavior_type,event_date from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_act_date 
        from feature.train_ui_f_act_date)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=1 and event_date=ui_f_act_date 
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);



用户对此商品的最后一个交互天的点击数

drop table if exists feature.train_ui_l_actday_clk;
create table feature.train_ui_l_actday_clk as
    select user_id,item_id,ifnull(ui_l_actday_clk,0) as ui_l_actday_clk from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_l_actday_clk from
    (
    select user_id,item_id,ui_l_act_date,behavior_type,event_date from
    (select user_id,item_id,behavior_type,event_date from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=1 and event_date=ui_l_act_date 
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);


4. 用户在最近3天、5天、7天、14天，最初14天有多少单天购买过、交互过此商品

用户在最近7天有多少单天购买过此商品

drop table if exists feature.train_ui_l7_h1_buy;
create table feature.train_ui_l7_h1_buy as
    select user_id,item_id,ifnull(ui_l7_h1_buy,0) as ui_l7_h1_buy from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (
        select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l7_h1_buy from
        (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30 where 
            (event_date between 26 and 30 or event_date between 22 and 23) and behavior_type=4)b
        group by user_id,item_id
    )c
    on (a.user_id=c.user_id_1 and a.item_id=c.item_id_1);

用户在最近14天有多少单天交互过此商品


drop table if exists feature.train_ui_l14_h1_act;
create table feature.train_ui_l14_h1_act as
    select user_id,item_id,ifnull(ui_l14_h1_act,0) as ui_l14_h1_act from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (
        select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l14_h1_act from
        (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30 where 
            event_date between 26 and 30 or event_date between 15 and 23)b
        group by user_id,item_id
    )c
    on (a.user_id=c.user_id_1 and a.item_id=c.item_id_1);



5. 用户在最近 1、3、5、7 天的白天、晚上、深夜购买、点击、交互此商品的次数


用户在最近3天的白天交互此商品的次数

drop table if exists feature.train_ui_l3_day_act;
create table feature.train_ui_l3_day_act as
    select user_id,item_id,ifnull(ui_l3_day_act,0) as ui_l3_day_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1, count(behavior_type) as ui_l3_day_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 28 and 30) and (event_time between 8 and 18) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

 
用户在最近7天的晚上点击此商品的次数

drop table if exists feature.train_ui_l7_eve_clk;
create table feature.train_ui_l7_eve_clk as
    select user_id,item_id,ifnull(ui_l7_eve_clk,0) as ui_l7_eve_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1, count(behavior_type) as ui_l7_eve_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 19 and 23 or event_time between 0 and 1) 
    group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


6. 用户对此商品在所有30天、28天内（不含双12）、双12期间、最后7天、14天 点击to购买转化率、点击to留待转化率、留待to购买转化率


用户在30天内对此商品的 点击to购买转化率 (点击总次数/购买总次数)

drop table if exists feature.train_ui_1_30_buy;
create table feature.train_ui_1_30_buy as
    select user_id,item_id,ifnull(ui_1_30_buy,0) as ui_1_30_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,count(behavior_type) as ui_1_30_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 group by user_id,item_id)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists feature.train_ui_1_30_clk;
create table feature.train_ui_1_30_clk as
    select user_id,item_id,ifnull(ui_1_30_clk,0) as ui_1_30_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,count(behavior_type) as ui_1_30_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 group by user_id,item_id)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists feature.train_ui_1_30_clk2buy;
create table feature.train_ui_1_30_clk2buy as
    select user_id,item_id,ifnull(ui_1_30_clk/ui_1_30_buy,0) as ui_1_30_clk2buy from
    (select user_id,item_id,ui_1_30_clk from feature.train_ui_1_30_clk)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_1_30_buy from feature.train_ui_1_30_buy)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


用户在双12期间的点击to留待转化率 （点击总次数/留待总次数)

drop table if exists feature.train_ui_d12_clk;
create table feature.train_ui_d12_clk as
    select user_id,item_id,ifnull(ui_d12_clk,0) as ui_d12_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,count(behavior_type) as ui_d12_clk from washed.tianchi_p_ten_buy_1_30 
        where behavior_type=1 and event_date between 24 and 25 group by user_id,item_id)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists feature.train_ui_d12_later;
create table feature.train_ui_d12_later as
    select user_id,item_id,ifnull(ui_d12_later,0) as ui_d12_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,count(behavior_type) as ui_d12_later from washed.tianchi_p_ten_buy_1_30 
        where behavior_type between 2 and 3 and event_date between 24 and 25 group by user_id,item_id)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists feature.train_ui_d12_clk2later;
create table feature.train_ui_d12_clk2later as
    select user_id,item_id,ifnull(ui_d12_clk/ui_d12_later,0) as ui_d12_clk2later from
    (
    select user_id,item_id,ui_d12_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_d12_clk from feature.train_ui_d12_clk)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    left outer join
    (select user_id as user_id_2,item_id as item_id_2,ui_d12_later from feature.train_ui_d12_later)d
    on(c.user_id=d.user_id_2 and c.item_id=d.item_id_2);


7.用户在购买此商品之前的点击次数、收藏次数、加购物车次数 （抽取时，若在同一天，则由小时定义是否在之前！！！）

用户在购买此商品之前的点击次数

drop table if exists test.train_ui_f_buy_date_time;
create table test.train_ui_f_buy_date_time as 
    select user_id,item_id,ifnull(ui_f_buy_date_time,0) as ui_f_buy_date_time from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,min(event_date*100+event_time) as ui_f_buy_date_time from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 

drop table if exists feature.train_ui_f_buybefore_clk;
create table feature.train_ui_f_buybefore_clk as
    select user_id,item_id,ifnull(ui_f_buybefore_clk,0) as ui_f_buybefore_clk from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_f_buybefore_clk from
    (
    select user_id,item_id,ui_f_buy_date_time,behavior_type,(event_date*100+event_time) as event_date_time from
    (select user_id,item_id,behavior_type,event_date,event_time from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_buy_date_time 
        from test.train_ui_f_buy_date_time)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=1 and event_date_time<=ui_f_buy_date_time
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);

用户在购买此商品之前的收藏次数

drop table if exists feature.train_ui_f_buybefore_favor;
create table feature.train_ui_f_buybefore_favor as
    select user_id,item_id,ifnull(ui_f_buybefore_favor,0) as ui_f_buybefore_favor from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_f_buybefore_favor from
    (
    select user_id,item_id,ui_f_buy_date_time,behavior_type,(event_date*100+event_time) as event_date_time from
    (select user_id,item_id,behavior_type,event_date,event_time from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_buy_date_time 
        from test.train_ui_f_buy_date_time)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=2 and event_date_time<=ui_f_buy_date_time
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);

用户在购买此商品之前的加购物车次数

drop table if exists feature.train_ui_f_buybefore_cart;
create table feature.train_ui_f_buybefore_cart as
    select user_id,item_id,ifnull(ui_f_buybefore_cart,0) as ui_f_buybefore_cart from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_f_buybefore_cart from
    (
    select user_id,item_id,ui_f_buy_date_time,behavior_type,(event_date*100+event_time) as event_date_time from
    (select user_id,item_id,behavior_type,event_date,event_time from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_buy_date_time 
        from test.train_ui_f_buy_date_time)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=3 and event_date_time<=ui_f_buy_date_time
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);

8. *用户最后一次交互此商品与最后一次交互任意商品相隔多少天*


用户最后一次交互每个商品的日期 feature.train_ui_l_act_date;
用户最后一次交互任意商品的日期 feature.train_user_l_act_date;

用户最后一次交互此商品与最后一次交互任意商品相隔多少天:

drop table if exists feature.train_ui_l_act_distance;
create table feature.train_ui_l_act_distance as 
    select user_id,item_id,(user_l_act_date-ui_l_act_date) as ui_l_act_distance from
    (select user_id,item_id,ui_l_act_date from feature.train_ui_l_act_date)a
    left outer join
    (select user_id as user_id_1,user_l_act_date from feature.train_user_l_act_date)b
    on (a.user_id=b.user_id_1); 


*用户最后一次交互此商品的前一天、当天、后一天分别交互了多少个其它商品*

用户最后一次交互此商品的当天交互了多少个其它商品
drop table if exists feature.train_ui_l_actday_other_distinct_act;
create table feature.train_ui_l_actday_other_distinct_act as
select user_id_1 as user_id,item_id_1 as item_id,(user_ui_l_actday_distinct_act-1) as ui_l_actday_other_distinct_act from
(select user_id_1,item_id_1,count(distinct item_id) as user_ui_l_actday_distinct_act from
(select user_id_1,item_id_1,item_id,ui_l_act_date,event_date from
    (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date)a
    right outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date)b
    on(a.user_id=b.user_id_1 and a.event_date=b.ui_l_act_date)
    order by user_id_1,ui_l_act_date,item_id_1)c
    group by user_id_1,item_id_1)d;


用户最后一次交互此商品的后一天交互了多少个其它商品

drop table if exists feature.train_ui_l_actnextday_other_distinct_act;
create table feature.train_ui_l_actnextday_other_distinct_act as
select user_id,item_id,ifnull(user_ui_l_actnextday_distinct_act,0) as ui_l_actnextday_other_distinct_act from
    (
    select user_id_1,item_id_1,count(distinct item_id) as user_ui_l_actnextday_distinct_act from
    (
    select user_id_1,item_id_1,item_id,ui_l_act_date,event_date from
    (
        select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date
    )a
    right outer join
    (
        select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date
    )b
    on(a.user_id=b.user_id_1 and a.event_date=b.ui_l_act_date+1)
    order by user_id_1,ui_l_act_date,item_id_1
    )c 
    where item_id is not null 
    group by user_id_1,item_id_1
    )d 
    right outer join
    (
        select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    )e
    on(d.user_id_1=e.user_id and d.item_id_1=e.item_id);


用户最后一次交互此商品的前一天交互了多少个其它商品


drop table if exists feature.train_ui_l_actprevday_other_distinct_act;
create table feature.train_ui_l_actprevday_other_distinct_act as

select user_id,item_id,(user_ui_l_actprevday_distinct_act-ui_acted_in_l_actprevday)as ui_l_actprevday_other_distinct_act from

(select user_id,item_id,ifnull(user_ui_l_actprevday_distinct_act,0) as user_ui_l_actprevday_distinct_act from
    (
    select user_id_1,item_id_1,count(distinct item_id) as user_ui_l_actprevday_distinct_act from
    (
    select user_id_1,item_id_1,item_id,ui_l_act_date,event_date from
    (
        select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date
    )a
    right outer join
    (
        select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date
    )b
    on(a.user_id=b.user_id_1 and a.event_date=b.ui_l_act_date-1)
    order by user_id_1,ui_l_act_date,item_id_1
    )c 
    where item_id is not null 
    group by user_id_1,item_id_1
    )d 
    right outer join
    (
        select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    )e
    on(d.user_id_1=e.user_id and d.item_id_1=e.item_id)
    )f

    left outer join

    (
    select user_id as user_id_1,item_id as item_id_1,ifnull(ui_acted_in_l_actprevday,0) as ui_acted_in_l_actprevday from
    (
    select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    )g
    left outer join 
    (
    select user_id_1,item_id_1,1 as ui_acted_in_l_actprevday from
    (
    select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date
    )h
    right outer join
    (
        select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date
    )i
    on(h.user_id=i.user_id_1 and h.item_id=i.item_id_1 and h.event_date=i.ui_l_act_date-1)
    where event_date is not null
    )j
    on(g.user_id=j.user_id_1 and g.item_id=j.item_id_1)
    )k

    on (f.user_id=k.user_id_1 and f.item_id=k.item_id_1);


*用户最后一次交互此商品的当天、后一天分别点击、购买、交互了多少次其它商品*

用户最后一次交互此商品的当天交互了多少次其它商品

drop table if exists feature.train_ui_l_actday_other_act;
create table feature.train_ui_l_actday_other_act as
select user_id,item_id,(user_ui_l_actday_act-ui_l_actday_act) as ui_l_actday_other_act from

(select user_id,item_id,count(behavior_type) as ui_l_actday_act from
(select user_id,item_id,event_date,behavior_type from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date)a
    right outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1 and a.event_date=b.ui_l_act_date)
group by user_id,item_id)c

left outer join

(select user_id_1,item_id_1,count(behavior_type) as user_ui_l_actday_act from
(select user_id,item_id,event_date,behavior_type from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date
)d
right outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date
)e
on(d.user_id=e.user_id_1 and d.event_date=e.ui_l_act_date)
group by user_id_1,item_id_1)f

on(c.user_id=f.user_id_1 and c.item_id=f.item_id_1);

用户最后一次交互此商品的当天点击了多少次其它商品

drop table if exists feature.train_ui_l_actday_other_clk;
create table feature.train_ui_l_actday_other_clk as
select user_id,item_id,(user_ui_l_actday_clk-ui_l_actday_clk) as ui_l_actday_other_clk from
(
select user_id,item_id,ui_l_actday_clk,ifnull(user_ui_l_actday_clk,0) as user_ui_l_actday_clk from
(select user_id,item_id,ifnull(ui_l_actday_clk,0) as ui_l_actday_clk from
(select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)aa
left outer join
(select user_id as user_id_1,item_id as item_id_1,count(behavior_type) as ui_l_actday_clk from
(select user_id,item_id,event_date,behavior_type from washed.tianchi_p_ten_buy_1_30  
        order by user_id,event_date)a
    right outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1 and a.event_date=b.ui_l_act_date)
    where behavior_type=1
group by user_id,item_id)c
on(aa.user_id=c.user_id_1 and aa.item_id=c.item_id_1)
)bb

left outer join

(select user_id_1,item_id_1,count(behavior_type) as user_ui_l_actday_clk from
(select user_id,item_id,event_date,behavior_type from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date
)d
right outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date
)e
on(d.user_id=e.user_id_1 and d.event_date=e.ui_l_act_date)
where behavior_type=1
group by user_id_1,item_id_1)f

on(bb.user_id=f.user_id_1 and bb.item_id=f.item_id_1)
)cc;


用户最后一次交互此商品的当天购买了多少次其它商品

drop table if exists feature.train_ui_l_actday_other_buy;
create table feature.train_ui_l_actday_other_buy as
select user_id,item_id,(user_ui_l_actday_buy-ui_l_actday_buy) as ui_l_actday_other_buy from
(
select user_id,item_id,ui_l_actday_buy,ifnull(user_ui_l_actday_buy,0) as user_ui_l_actday_buy from
(select user_id,item_id,ifnull(ui_l_actday_buy,0) as ui_l_actday_buy from
(select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)aa
left outer join
(select user_id as user_id_1,item_id as item_id_1,count(behavior_type) as ui_l_actday_buy from
(select user_id,item_id,event_date,behavior_type from washed.tianchi_p_ten_buy_1_30  
        order by user_id,event_date)a
    right outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1 and a.event_date=b.ui_l_act_date)
    where behavior_type=4
group by user_id,item_id)c
on(aa.user_id=c.user_id_1 and aa.item_id=c.item_id_1)
)bb

left outer join

(select user_id_1,item_id_1,count(behavior_type) as user_ui_l_actday_buy from
(select user_id,item_id,event_date,behavior_type from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date
)d
right outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date
)e
on(d.user_id=e.user_id_1 and d.event_date=e.ui_l_act_date)
where behavior_type=4
group by user_id_1,item_id_1)f

on(bb.user_id=f.user_id_1 and bb.item_id=f.item_id_1)
)cc;


用户最后一次交互此商品的下一天交互了多少次其它商品

drop table if exists feature.train_ui_l_actnextday_other_act;
create table feature.train_ui_l_actnextday_other_act as
select user_id,item_id,ifnull(user_ui_l_actnextday_act,0) as ui_l_actnextday_other_act from
    (
    select user_id_1,item_id_1,count(behavior_type) as user_ui_l_actnextday_act from
    (
    select user_id_1,item_id_1,item_id,behavior_type from
    (
        select user_id,item_id,event_date,behavior_type from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date
    )a
    right outer join
    (
        select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date
    )b
    on(a.user_id=b.user_id_1 and a.event_date=b.ui_l_act_date+1)
    /*where*/
    order by user_id_1,ui_l_act_date,item_id_1
    )c 
    where item_id is not null 
    group by user_id_1,item_id_1
    )d 
    right outer join
    (
        select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    )e
    on(d.user_id_1=e.user_id and d.item_id_1=e.item_id);

用户最后一次交互此商品的下一天点击了多少次其它商品

drop table if exists feature.train_ui_l_actnextday_other_clk;
create table feature.train_ui_l_actnextday_other_clk as
select user_id,item_id,ifnull(user_ui_l_actnextday_clk,0) as ui_l_actnextday_other_clk from
    (
    select user_id_1,item_id_1,count(behavior_type) as user_ui_l_actnextday_clk from
    (
    select user_id_1,item_id_1,item_id,behavior_type from
    (
        select user_id,item_id,event_date,behavior_type from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date
    )a
    right outer join
    (
        select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date
    )b
    on(a.user_id=b.user_id_1 and a.event_date=b.ui_l_act_date+1)
    where behavior_type=1
    order by user_id_1,ui_l_act_date,item_id_1
    )c 
    where item_id is not null 
    group by user_id_1,item_id_1
    )d 
    right outer join
    (
        select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    )e
    on(d.user_id_1=e.user_id and d.item_id_1=e.item_id);

用户最后一次交互此商品的下一天购买了多少次其它商品

drop table if exists feature.train_ui_l_actnextday_other_buy;
create table feature.train_ui_l_actnextday_other_buy as
select user_id,item_id,ifnull(user_ui_l_actnextday_buy,0) as ui_l_actnextday_other_buy from
    (
    select user_id_1,item_id_1,count(behavior_type) as user_ui_l_actnextday_buy from
    (
    select user_id_1,item_id_1,item_id,behavior_type from
    (
        select user_id,item_id,event_date,behavior_type from washed.tianchi_p_ten_buy_1_30 
        order by user_id,event_date
    )a
    right outer join
    (
        select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from feature.train_ui_l_act_date
    )b
    on(a.user_id=b.user_id_1 and a.event_date=b.ui_l_act_date+1)
    where behavior_type=4
    order by user_id_1,ui_l_act_date,item_id_1
    )c 
    where item_id is not null 
    group by user_id_1,item_id_1
    )d 
    right outer join
    (
        select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    )e
    on(d.user_id_1=e.user_id and d.item_id_1=e.item_id);

*用户在最近 1、3、5、7、10、14、28 天内有多少个单天只交互了此商品*


用户在最近1天有多少单天只交互了此商品


drop table if exists feature.train_ui_l1_h1_only_act;
create table feature.train_ui_l1_h1_only_act as

    select user_id,item_id,ifnull(ui_l1_h1_only_act,0) as  ui_l1_h1_only_act from

    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)aaa

    left outer join

    (select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l1_h1_only_act from

    (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30
    where event_date=30 
    order by user_id,event_date)aa
     
    join
    
    (select user_id as user_id_1,event_date as event_date_1,user_distinct_act from
    (
    select user_id,event_date,count(distinct item_id) as user_distinct_act from
    (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30
        where event_date=30)a
    group by user_id,event_date
    )b
    where user_distinct_act=1 
    order by user_id,event_date
    )c

    on(aa.user_id=c.user_id_1 and aa.event_date=c.event_date_1)
    group by user_id,item_id
    )d

    on(aaa.user_id=d.user_id_1 and aaa.item_id=d.item_id_1);

用户在最近7天有多少单天只交互了此商品

drop table if exists feature.train_ui_l7_h1_only_act;
create table feature.train_ui_l7_h1_only_act as

    select user_id,item_id,ifnull(ui_l7_h1_only_act,0) as  ui_l7_h1_only_act from

    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)aaa

    left outer join

    (select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l7_h1_only_act from

    (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30
    where event_date between 26 and 30 or event_date between 22 and 23 
    order by user_id,event_date)aa
     
    join
    
    (select user_id as user_id_1,event_date as event_date_1,user_distinct_act from
    (
    select user_id,event_date,count(distinct item_id) as user_distinct_act from
    (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_buy_1_30
        where event_date between 26 and 30 or event_date between 22 and 23)a
    group by user_id,event_date
    )b
    where user_distinct_act=1 
    order by user_id,event_date
    )c

    on(aa.user_id=c.user_id_1 and aa.event_date=c.event_date_1)
    group by user_id,item_id
    )d

    on(aaa.user_id=d.user_id_1 and aaa.item_id=d.item_id_1);
    


*用户在四周（Week 1-4）中有几周只交互了此商品*


给操作记录加上 Week No.：

drop table if exists test.ui_week_act;
create table test.ui_week_act as
select user_id,item_id,week_number from
(
select user_id,item_id,case when event_date>=1 AND event_date <= 7 THEN '1'
WHEN event_date >=8 AND event_date <= 14  THEN '2'
WHEN event_date >=15 AND event_date <= 21  THEN '3'
WHEN event_date between 22 and 23 or event_date between 26 and 30 THEN '4'
ELSE NULL END week_number,event_date
 from washed.tianchi_p_ten_buy_1_30 
 )a
where week_number is not null;


用户在四周中有几周只交互了此商品:

drop table if exists feature.train_ui_l28_h7_only_act;
create table feature.train_ui_l28_h7_only_act as

    select user_id,item_id,ifnull(ui_l28_h7_only_act,0) as ui_l28_h7_only_act from

    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)aaa

    left outer join

    (select user_id as user_id_1,item_id as item_id_1,count(week_number) as ui_l28_h7_only_act from

    (select distinct user_id,item_id,week_number from test.ui_week_act
    order by user_id,week_number)aa
     
    join
    
    (select user_id as user_id_1,week_number as week_number_1,user_distinct_act from
    (
    select user_id,week_number,count(distinct item_id) as user_distinct_act from
    (select distinct user_id,item_id,week_number from test.ui_week_act)a
    group by user_id,week_number
    )b
    where user_distinct_act=1 
    order by user_id,week_number
    )c

    on(aa.user_id=c.user_id_1 and aa.week_number=c.week_number_1)
    group by user_id,item_id
    )d

    on(aaa.user_id=d.user_id_1 and aaa.item_id=d.item_id_1);





9.用户最后一次交互每个类别的日期
  用户最后一次交互此类别与最后一次交互行为相隔多少天


用户最后一次交互每个类别的日期 feature.train_uc_l_act_date;

drop table if exists feature.train_uc_l_act_date;
create table feature.train_uc_l_act_date as 
    select user_id,item_category,ifnull(uc_l_act_date,100) as uc_l_act_date from
    (select distinct user_id,item_category from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,item_category as item_category_1,max(event_date) as uc_l_act_date from washed.tianchi_p_ten_buy_1_30 
    group by user_id,item_category)b
    on (a.user_id=b.user_id_1 and a.item_category=b.item_category_1); 

用户最后一次交互行为的日期 feature.train_user_l_act_date;

用户最后一次交互此类别与最后一次交互行为相隔多少天:

drop table if exists feature.train_uc_l_act_distance;
create table feature.train_uc_l_act_distance as 
    select user_id,item_category,(user_l_act_date-uc_l_act_date) as uc_l_act_distance from
    (select user_id,item_category,uc_l_act_date from feature.train_uc_l_act_date)a
    left outer join
    (select user_id as user_id_1,user_l_act_date from feature.train_user_l_act_date)b
    on (a.user_id=b.user_id_1); 



10. 用户和商品之间的位置信息

两种方案：1.用户和商品之间位置远近（dummy特征）

nodata  1common  2common 3common 4common 5common 6common 7common  

2.用户和商品之间距离的统计特征：min_distance,avg_distance,max_distance

***先采用第2种***

用户地理位置信息
create table test.train_user_geo as 
SELECT user_id,item_id,user_geohash FROM washed.tianchi_p_ten_buy_1_30
order by user_id,item_id;

商品地理位置信息
create table test.item_geo as  
SELECT item_id,item_geohash FROM tianchi.item order by item_id;

用户-商品地理位置信息（两个都有位置信息的）
 create table test.user_item_geo as 
 select * from(
 select user_id,item_id,user_geohash,item_geohash from
    (select user_id,item_id,user_geohash from test.train_user_geo)a
    left outer join
    (select item_id as item_id_1,item_geohash from test.item_geo)b
    on(a.item_id=b.item_id_1)
    )c where user_geohash is not null and item_geohash is not null;


用户-商品之间距离（两个都有地理位置信息的）
select user_id,item_id,case when user_geohash=item_geohash THEN '7'
WHEN left(user_geohash,6)=left(item_geohash,6)  THEN '6'
WHEN left(user_geohash,5)=left(item_geohash,5)  THEN '5'
WHEN left(user_geohash,4)=left(item_geohash,4)  THEN '4'
WHEN left(user_geohash,3)=left(item_geohash,3)  THEN '3'
WHEN left(user_geohash,2)=left(item_geohash,2)  THEN '2'
WHEN left(user_geohash,1)=left(item_geohash,1)  THEN '1'
ELSE '0' END distance,user_geohash,item_geohash
 from test.user_item_geo; 


用户-商品之间距离（没有地理位置信息的记为0）
create table test.user_item_distance as 
select user_id,item_id,ifnull(distance,0) as distance from
(select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,case when user_geohash=item_geohash THEN '7'
WHEN left(user_geohash,6)=left(item_geohash,6)  THEN '6'
WHEN left(user_geohash,5)=left(item_geohash,5)  THEN '5'
WHEN left(user_geohash,4)=left(item_geohash,4)  THEN '4'
WHEN left(user_geohash,3)=left(item_geohash,3)  THEN '3'
WHEN left(user_geohash,2)=left(item_geohash,2)  THEN '2'
WHEN left(user_geohash,1)=left(item_geohash,1)  THEN '1'
ELSE '0' END distance from test.user_item_geo)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)


用户-商品之间距离统计

drop table if exists feature.train_ui_distance;
create table feature.train_ui_distance as 
select user_id,item_id,max(distance) as ui_min_distance,avg(distance) as ui_avg_distance,min(distance) as ui_max_distance from
test.user_item_distance group by user_id,item_id;
