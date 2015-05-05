1.用户在最近的 1、3、7、10、14、20、28 天 (不包含双12的两天) 内的购买、点击、交互、留待所有商品的总次数
  用户在双12（Day 24、25）期间购买、点击、交互所有商品的总次数
  用户在每一周（对于训练集为：Day 1-7、Day 8-14、Day 15-21、Day 22-23 & 26-30）购买、点击、交互、留待所
  有商品的总次数 （可能和最近N天特征有重复）

/*用户在最近 1、3、7、10、14、20、28 天的购买总次数：*/

drop table if exists feature.train_user_l1_buy;
create table feature.train_user_l1_buy as
    select user_id,ifnull(user_l1_buy,0) as user_l1_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date=30) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_buy;
create table feature.train_user_l3_buy as
    select user_id,ifnull(user_l3_buy,0) as user_l3_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 28 and 30) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l7_buy;
create table feature.train_user_l7_buy as
    select user_id,ifnull(user_l7_buy,0) as user_l7_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 26 and 30  or event_date between 22 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l10_buy;
create table feature.train_user_l10_buy as
    select user_id,ifnull(user_l10_buy,0) as user_l10_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l10_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 26 and 30 or event_date between 19 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l14_buy;
create table feature.train_user_l14_buy as
    select user_id,ifnull(user_l14_buy,0) as user_l14_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l14_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 26 and 30 or event_date between 15 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l20_buy;
create table feature.train_user_l20_buy as
    select user_id,ifnull(user_l20_buy,0) as user_l20_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l20_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 26 and 30 or event_date between 9 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l28_buy;
create table feature.train_user_l28_buy as
    select user_id,ifnull(user_l28_buy,0) as user_l28_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l28_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 26 and 30 or event_date between 1 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

/*用户在最近 1、3、7、10、14、20、28 天的点击总次数：*/

drop table if exists feature.train_user_l1_clk;
create table feature.train_user_l1_clk as
    select user_id,ifnull(user_l1_clk,0) as user_l1_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date=30) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_clk;
create table feature.train_user_l3_clk as
    select user_id,ifnull(user_l3_clk,0) as user_l3_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 28 and 30) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l7_clk;
create table feature.train_user_l7_clk as
    select user_id,ifnull(user_l7_clk,0) as user_l7_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 26 and 30  or event_date between 22 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l10_clk;
create table feature.train_user_l10_clk as
    select user_id,ifnull(user_l10_clk,0) as user_l10_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l10_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 26 and 30 or event_date between 19 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l14_clk;
create table feature.train_user_l14_clk as
    select user_id,ifnull(user_l14_clk,0) as user_l14_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l14_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 26 and 30 or event_date between 15 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l20_clk;
create table feature.train_user_l20_clk as
    select user_id,ifnull(user_l20_clk,0) as user_l20_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l20_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 26 and 30 or event_date between 9 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l28_clk;
create table feature.train_user_l28_clk as
    select user_id,ifnull(user_l28_clk,0) as user_l28_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l28_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 26 and 30 or event_date between 1 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

用户在最近 1、3、7、10、14、20、28 天的交互总次数：

drop table if exists feature.train_user_l1_act;
create table feature.train_user_l1_act as
    select user_id,ifnull(user_l1_act,0) as user_l1_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_act from washed.tianchi_p_ten_buy_1_30 
    where event_date=30 group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_act;
create table feature.train_user_l3_act as
    select user_id,ifnull(user_l3_act,0) as user_l3_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 28 and 30 group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l7_act;
create table feature.train_user_l7_act as
    select user_id,ifnull(user_l7_act,0) as user_l7_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 26 and 30  or event_date between 22 and 23 group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l10_act;
create table feature.train_user_l10_act as
    select user_id,ifnull(user_l10_act,0) as user_l10_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l10_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 26 and 30 or event_date between 19 and 23 group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l14_act;
create table feature.train_user_l14_act as
    select user_id,ifnull(user_l14_act,0) as user_l14_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l14_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 26 and 30 or event_date between 15 and 23 group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l20_act;
create table feature.train_user_l20_act as
    select user_id,ifnull(user_l20_act,0) as user_l20_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l20_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 26 and 30 or event_date between 9 and 23 group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l28_act;
create table feature.train_user_l28_act as
    select user_id,ifnull(user_l28_act,0) as user_l28_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l28_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 26 and 30 or event_date between 1 and 23 group by user_id)b
    on (a.user_id=b.user_id_1);


用户在最近 1、3、7、10、14、20、28 天的留待总次数：


drop table if exists feature.train_user_l1_later;
create table feature.train_user_l1_later as
    select user_id,ifnull(user_l1_later,0) as user_l1_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date=30) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_later;
create table feature.train_user_l3_later as
    select user_id,ifnull(user_l3_later,0) as user_l3_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date between 28 and 30) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l7_later;
create table feature.train_user_l7_later as
    select user_id,ifnull(user_l7_later,0) as user_l7_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date between 26 and 30  or event_date between 22 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l10_later;
create table feature.train_user_l10_later as
    select user_id,ifnull(user_l10_later,0) as user_l10_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l10_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date between 26 and 30 or event_date between 19 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l14_later;
create table feature.train_user_l14_later as
    select user_id,ifnull(user_l14_later,0) as user_l14_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l14_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date between 26 and 30 or event_date between 15 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l20_later;
create table feature.train_user_l20_later as
    select user_id,ifnull(user_l20_later,0) as user_l20_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l20_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date between 26 and 30 or event_date between 9 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l28_later;
create table feature.train_user_l28_later as
    select user_id,ifnull(user_l28_later,0) as user_l28_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l28_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date between 26 and 30 or event_date between 1 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


用户在双12（Day 24、25）期间购买、点击、交互所有商品的总次数
  用户在每一周（对于训练集为：Day 1-7、Day 8-14、Day 15-21、Day 22-23 & 26-30）购买、点击、交互、留待所
  有商品的总次数 （可能和最近N天特征有重复）

drop table if exists feature.train_user_d12_buy;
create table feature.train_user_d12_buy as
    select user_id,ifnull(user_d12_buy,0) as user_d12_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 24 and 25) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week1_buy;
create table feature.train_user_week1_buy as
    select user_id,ifnull(user_week1_buy,0) as user_week1_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week1_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 1 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week2_buy;
create table feature.train_user_week2_buy as
    select user_id,ifnull(user_week2_buy,0) as user_week2_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week2_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 8 and 14) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week3_buy;
create table feature.train_user_week3_buy as
    select user_id,ifnull(user_week3_buy,0) as user_week3_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week3_buy from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=4) and (event_date between 15 and 21) group by user_id)b
    on (a.user_id=b.user_id_1);




drop table if exists feature.train_user_d12_clk;
create table feature.train_user_d12_clk as
    select user_id,ifnull(user_d12_clk,0) as user_d12_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 24 and 25) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week1_clk;
create table feature.train_user_week1_clk as
    select user_id,ifnull(user_week1_clk,0) as user_week1_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week1_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 1 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week2_clk;
create table feature.train_user_week2_clk as
    select user_id,ifnull(user_week2_clk,0) as user_week2_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week2_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 8 and 14) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week3_clk;
create table feature.train_user_week3_clk as
    select user_id,ifnull(user_week3_clk,0) as user_week3_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week3_clk from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type=1) and (event_date between 15 and 21) group by user_id)b
    on (a.user_id=b.user_id_1);




drop table if exists feature.train_user_d12_act;
create table feature.train_user_d12_act as
    select user_id,ifnull(user_d12_act,0) as user_d12_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 24 and 25 group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week1_act;
create table feature.train_user_week1_act as
    select user_id,ifnull(user_week1_act,0) as user_week1_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week1_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 1 and 7 group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week2_act;
create table feature.train_user_week2_act as
    select user_id,ifnull(user_week2_act,0) as user_week2_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week2_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 8 and 14 group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week3_act;
create table feature.train_user_week3_act as
    select user_id,ifnull(user_week3_act,0) as user_week3_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week3_act from washed.tianchi_p_ten_buy_1_30 
    where event_date between 15 and 21 group by user_id)b
    on (a.user_id=b.user_id_1);



drop table if exists feature.train_user_week1_later;
create table feature.train_user_week1_later as
    select user_id,ifnull(user_week1_later,0) as user_week1_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week1_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date between 1 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week2_later;
create table feature.train_user_week2_later as
    select user_id,ifnull(user_week2_later,0) as user_week2_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week2_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date between 8 and 14) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_week3_later;
create table feature.train_user_week3_later as
    select user_id,ifnull(user_week3_later,0) as user_week3_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_week3_later from washed.tianchi_p_ten_buy_1_30 
    where (behavior_type between 2 and 3) and (event_date between 15 and 21) group by user_id)b
    on (a.user_id=b.user_id_1);



2.用户在最近的 1、3、7、10、14、20、28 天内购买、点击、交互、留待了多少个不同的商品
  用户在每一周内购买、点击、交互、留待了多少个不同的商品



用户在最近的 1、3、7、10、14、20、28 天内点击了多少个不同的商品
drop table if exists feature.train_user_l1_distinct_clk;
create table feature.train_user_l1_distinct_clk as
    select user_id,ifnull(user_l1_distinct_clk,0) as user_l1_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l1_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date=30) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l3_distinct_clk;
create table feature.train_user_l3_distinct_clk as
    select user_id,ifnull(user_l3_distinct_clk,0) as user_l3_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l3_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date between 28 and 30) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);


drop table if exists feature.train_user_l7_distinct_clk;
create table feature.train_user_l7_distinct_clk as
    select user_id,ifnull(user_l7_distinct_clk,0) as user_l7_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l7_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date between 26 and 30  or event_date between 22 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l10_distinct_clk;
create table feature.train_user_l10_distinct_clk as
    select user_id,ifnull(user_l10_distinct_clk,0) as user_l10_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l10_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date between 26 and 30  or event_date between 19 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l14_distinct_clk;
create table feature.train_user_l14_distinct_clk as
    select user_id,ifnull(user_l14_distinct_clk,0) as user_l14_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l14_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date between 26 and 30  or event_date between 15 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l20_distinct_clk;
create table feature.train_user_l20_distinct_clk as
    select user_id,ifnull(user_l20_distinct_clk,0) as user_l20_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l20_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date between 26 and 30  or event_date between 9 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l28_distinct_clk;
create table feature.train_user_l28_distinct_clk as
    select user_id,ifnull(user_l28_distinct_clk,0) as user_l28_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l28_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date between 26 and 30  or event_date between 1 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);



用户在最近的 1、3、7、10、14、20、28 天内购买了多少个不同的商品
drop table if exists feature.train_user_l1_distinct_buy;
create table feature.train_user_l1_distinct_buy as
    select user_id,ifnull(user_l1_distinct_buy,0) as user_l1_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l1_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date=30) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l3_distinct_buy;
create table feature.train_user_l3_distinct_buy as
    select user_id,ifnull(user_l3_distinct_buy,0) as user_l3_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l3_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date between 28 and 30) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l7_distinct_buy;
create table feature.train_user_l7_distinct_buy as
    select user_id,ifnull(user_l7_distinct_buy,0) as user_l7_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l7_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date between 26 and 30  or event_date between 22 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l10_distinct_buy;
create table feature.train_user_l10_distinct_buy as
    select user_id,ifnull(user_l10_distinct_buy,0) as user_l10_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l10_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date between 26 and 30  or event_date between 19 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l14_distinct_buy;
create table feature.train_user_l14_distinct_buy as
    select user_id,ifnull(user_l14_distinct_buy,0) as user_l14_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l14_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date between 26 and 30  or event_date between 15 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l20_distinct_buy;
create table feature.train_user_l20_distinct_buy as
    select user_id,ifnull(user_l20_distinct_buy,0) as user_l20_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l20_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date between 26 and 30  or event_date between 9 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l28_distinct_buy;
create table feature.train_user_l28_distinct_buy as
    select user_id,ifnull(user_l28_distinct_buy,0) as user_l28_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l28_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date between 26 and 30  or event_date between 1 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

用户在最近的 1、3、7、10、14、20、28 天内留待了多少个不同的商品

drop table if exists feature.train_user_l1_distinct_later;
create table feature.train_user_l1_distinct_later as
    select user_id,ifnull(user_l1_distinct_later,0) as user_l1_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l1_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date=30) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l3_distinct_later;
create table feature.train_user_l3_distinct_later as
    select user_id,ifnull(user_l3_distinct_later,0) as user_l3_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l3_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date between 28 and 30) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l7_distinct_later;
create table feature.train_user_l7_distinct_later as
    select user_id,ifnull(user_l7_distinct_later,0) as user_l7_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l7_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date between 26 and 30  or event_date between 22 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l10_distinct_later;
create table feature.train_user_l10_distinct_later as
    select user_id,ifnull(user_l10_distinct_later,0) as user_l10_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l10_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date between 26 and 30  or event_date between 19 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l14_distinct_later;
create table feature.train_user_l14_distinct_later as
    select user_id,ifnull(user_l14_distinct_later,0) as user_l14_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l14_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date between 26 and 30  or event_date between 15 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l20_distinct_later;
create table feature.train_user_l20_distinct_later as
    select user_id,ifnull(user_l20_distinct_later,0) as user_l20_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l20_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date between 26 and 30  or event_date between 9 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l28_distinct_later;
create table feature.train_user_l28_distinct_later as
    select user_id,ifnull(user_l28_distinct_later,0) as user_l28_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l28_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date between 26 and 30  or event_date between 1 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);


用户在最近的 1、3、7、10、14、20、28 天内交互了多少个不同的商品

drop table if exists feature.train_user_l1_distinct_act;
create table feature.train_user_l1_distinct_act as
    select user_id,ifnull(user_l1_distinct_act,0) as user_l1_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l1_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date=30 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l3_distinct_act;
create table feature.train_user_l3_distinct_act as
    select user_id,ifnull(user_l3_distinct_act,0) as user_l3_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l3_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date between 28 and 30 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l7_distinct_act;
create table feature.train_user_l7_distinct_act as
    select user_id,ifnull(user_l7_distinct_act,0) as user_l7_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l7_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date between 26 and 30  or event_date between 22 and 23 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l10_distinct_act;
create table feature.train_user_l10_distinct_act as
    select user_id,ifnull(user_l10_distinct_act,0) as user_l10_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l10_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date between 26 and 30  or event_date between 19 and 23 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l14_distinct_act;
create table feature.train_user_l14_distinct_act as
    select user_id,ifnull(user_l14_distinct_act,0) as user_l14_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l14_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date between 26 and 30  or event_date between 15 and 23 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l20_distinct_act;
create table feature.train_user_l20_distinct_act as
    select user_id,ifnull(user_l20_distinct_act,0) as user_l20_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l20_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date between 26 and 30  or event_date between 9 and 23 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l28_distinct_act;
create table feature.train_user_l28_distinct_act as
    select user_id,ifnull(user_l28_distinct_act,0) as user_l28_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l28_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date between 26 and 30  or event_date between 1 and 23 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

用户在每一周内购买、点击、交互、留待了多少个不同的商品

drop table if exists feature.train_user_week1_distinct_buy;
create table feature.train_user_week1_distinct_buy as
    select user_id,ifnull(user_week1_distinct_buy,0) as user_week1_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week1_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date between 1 and 7) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_week1_distinct_clk;
create table feature.train_user_week1_distinct_clk as
    select user_id,ifnull(user_week1_distinct_clk,0) as user_week1_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week1_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date between 1 and 7) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_week1_distinct_act;
create table feature.train_user_week1_distinct_act as
    select user_id,ifnull(user_week1_distinct_act,0) as user_week1_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week1_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date between 1 and 7 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_week1_distinct_later;
create table feature.train_user_week1_distinct_later as
    select user_id,ifnull(user_week1_distinct_later,0) as user_week1_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week1_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date between 1 and 7) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);



drop table if exists feature.train_user_week2_distinct_buy;
create table feature.train_user_week2_distinct_buy as
    select user_id,ifnull(user_week2_distinct_buy,0) as user_week2_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week2_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date between 8 and 14) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_week2_distinct_clk;
create table feature.train_user_week2_distinct_clk as
    select user_id,ifnull(user_week2_distinct_clk,0) as user_week2_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week2_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date between 8 and 14) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_week2_distinct_act;
create table feature.train_user_week2_distinct_act as
    select user_id,ifnull(user_week2_distinct_act,0) as user_week2_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week2_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date between 8 and 14 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_week2_distinct_later;
create table feature.train_user_week2_distinct_later as
    select user_id,ifnull(user_week2_distinct_later,0) as user_week2_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week2_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date between 8 and 14) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);


drop table if exists feature.train_user_week3_distinct_buy;
create table feature.train_user_week3_distinct_buy as
    select user_id,ifnull(user_week3_distinct_buy,0) as user_week3_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week3_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=4) and (event_date between 15 and 21) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_week3_distinct_clk;
create table feature.train_user_week3_distinct_clk as
    select user_id,ifnull(user_week3_distinct_clk,0) as user_week3_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week3_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type=1) and (event_date between 15 and 21) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_week3_distinct_act;
create table feature.train_user_week3_distinct_act as
    select user_id,ifnull(user_week3_distinct_act,0) as user_week3_distinct_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week3_distinct_act from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where event_date between 15 and 21 order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_week3_distinct_later;
create table feature.train_user_week3_distinct_later as
    select user_id,ifnull(user_week3_distinct_later,0) as user_week3_distinct_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_week3_distinct_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_buy_1_30
    where (behavior_type between 2 and 3) and (event_date between 15 and 21) order by user_id,item_id)b
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

用户在最近 7、14、28 天有多少单天购买过任意商品

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

drop table if exists feature.train_user_l14_h1_buy;
create table feature.train_user_l14_h1_buy as
    select user_id,ifnull(user_l14_h1_buy,0) as user_l14_h1_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (
        select user_id as user_id_1,count(event_date) as user_l14_h1_buy from
        (select distinct user_id,event_date from washed.tianchi_p_ten_buy_1_30 where 
            (event_date between 26 and 30 or event_date between 15 and 23) and behavior_type=4)b
        group by user_id
    )c
    on (a.user_id=c.user_id_1);

drop table if exists feature.train_user_l28_h1_buy;
create table feature.train_user_l28_h1_buy as
    select user_id,ifnull(user_l28_h1_buy,0) as user_l28_h1_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (
        select user_id as user_id_1,count(event_date) as user_l28_h1_buy from
        (select distinct user_id,event_date from washed.tianchi_p_ten_buy_1_30 where 
            (event_date between 26 and 30 or event_date between 1 and 23) and behavior_type=4)b
        group by user_id
    )c
    on (a.user_id=c.user_id_1);

用户在最近 7、14、28 天有多少单天交互过任意商品


drop table if exists feature.train_user_l7_h1_act;
create table feature.train_user_l7_h1_act as
    select user_id,ifnull(user_l7_h1_act,0) as user_l7_h1_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (
        select user_id as user_id_1,count(event_date) as user_l7_h1_act from
        (select distinct user_id,event_date from washed.tianchi_p_ten_buy_1_30 where 
            event_date between 26 and 30 or event_date between 22 and 23)b
        group by user_id
    )c
    on (a.user_id=c.user_id_1);

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


drop table if exists feature.train_user_l28_h1_act;
create table feature.train_user_l28_h1_act as
    select user_id,ifnull(user_l28_h1_act,0) as user_l28_h1_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (
        select user_id as user_id_1,count(event_date) as user_l28_h1_act from
        (select distinct user_id,event_date from washed.tianchi_p_ten_buy_1_30 where 
            event_date between 26 and 30 or event_date between 1 and 23)b
        group by user_id
    )c
    on (a.user_id=c.user_id_1);


5. 用户在最近 1、3、5、7 天的白天、晚上、深夜购买、点击、交互所有商品的总次数


用户在最近 1、3、5、7 天的白天交互、点击、购买所有商品的总次数

drop table if exists feature.train_user_l1_day_act;
create table feature.train_user_l1_day_act as
    select user_id,ifnull(user_l1_day_act,0) as user_l1_day_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_day_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date=30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l3_day_act;
create table feature.train_user_l3_day_act as
    select user_id,ifnull(user_l3_day_act,0) as user_l3_day_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_day_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 28 and 30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l5_day_act;
create table feature.train_user_l5_day_act as
    select user_id,ifnull(user_l5_day_act,0) as user_l5_day_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l5_day_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 26 and 30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l7_day_act;
create table feature.train_user_l7_day_act as
    select user_id,ifnull(user_l7_day_act,0) as user_l7_day_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_day_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);





drop table if exists feature.train_user_l1_day_clk;
create table feature.train_user_l1_day_clk as
    select user_id,ifnull(user_l1_day_clk,0) as user_l1_day_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_day_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date=30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_day_clk;
create table feature.train_user_l3_day_clk as
    select user_id,ifnull(user_l3_day_clk,0) as user_l3_day_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_day_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 28 and 30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l5_day_clk;
create table feature.train_user_l5_day_clk as
    select user_id,ifnull(user_l5_day_clk,0) as user_l5_day_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l5_day_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 26 and 30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l7_day_clk;
create table feature.train_user_l7_day_clk as
    select user_id,ifnull(user_l7_day_clk,0) as user_l7_day_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_day_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);



drop table if exists feature.train_user_l1_day_buy;
create table feature.train_user_l1_day_buy as
    select user_id,ifnull(user_l1_day_buy,0) as user_l1_day_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_day_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date=30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_day_buy;
create table feature.train_user_l3_day_buy as
    select user_id,ifnull(user_l3_day_buy,0) as user_l3_day_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_day_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date between 28 and 30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l5_day_buy;
create table feature.train_user_l5_day_buy as
    select user_id,ifnull(user_l5_day_buy,0) as user_l5_day_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l5_day_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date between 26 and 30) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l7_day_buy;
create table feature.train_user_l7_day_buy as
    select user_id,ifnull(user_l7_day_buy,0) as user_l7_day_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_day_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 8 and 18) group by user_id)b
    on (a.user_id=b.user_id_1);


用户在最近 1、3、5、7 天的晚上交互、点击、购买所有商品的总次数 event_time between 19 and 23 or event_time between 0 and 1

drop table if exists feature.train_user_l1_eve_act;
create table feature.train_user_l1_eve_act as
    select user_id,ifnull(user_l1_eve_act,0) as user_l1_eve_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_eve_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date=30) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l3_eve_act;
create table feature.train_user_l3_eve_act as
    select user_id,ifnull(user_l3_eve_act,0) as user_l3_eve_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_eve_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 28 and 30) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l5_eve_act;
create table feature.train_user_l5_eve_act as
    select user_id,ifnull(user_l5_eve_act,0) as user_l5_eve_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l5_eve_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 26 and 30) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l7_eve_act;
create table feature.train_user_l7_eve_act as
    select user_id,ifnull(user_l7_eve_act,0) as user_l7_eve_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_eve_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);





drop table if exists feature.train_user_l1_eve_clk;
create table feature.train_user_l1_eve_clk as
    select user_id,ifnull(user_l1_eve_clk,0) as user_l1_eve_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_eve_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date=30) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_eve_clk;
create table feature.train_user_l3_eve_clk as
    select user_id,ifnull(user_l3_eve_clk,0) as user_l3_eve_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_eve_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 28 and 30) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l5_eve_clk;
create table feature.train_user_l5_eve_clk as
    select user_id,ifnull(user_l5_eve_clk,0) as user_l5_eve_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l5_eve_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 26 and 30) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l7_eve_clk;
create table feature.train_user_l7_eve_clk as
    select user_id,ifnull(user_l7_eve_clk,0) as user_l7_eve_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_eve_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);



drop table if exists feature.train_user_l1_eve_buy;
create table feature.train_user_l1_eve_buy as
    select user_id,ifnull(user_l1_eve_buy,0) as user_l1_eve_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_eve_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date=30) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_eve_buy;
create table feature.train_user_l3_eve_buy as
    select user_id,ifnull(user_l3_eve_buy,0) as user_l3_eve_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_eve_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date between 28 and 30) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l5_eve_buy;
create table feature.train_user_l5_eve_buy as
    select user_id,ifnull(user_l5_eve_buy,0) as user_l5_eve_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l5_eve_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date between 26 and 30) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l7_eve_buy;
create table feature.train_user_l7_eve_buy as
    select user_id,ifnull(user_l7_eve_buy,0) as user_l7_eve_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_eve_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 19 and 23 or event_time between 0 and 1) group by user_id)b
    on (a.user_id=b.user_id_1);


用户在最近 1、3、5、7 天的深夜交互、点击、购买所有商品的总次数 event_time between 2 and 7


drop table if exists feature.train_user_l1_nt_act;
create table feature.train_user_l1_nt_act as
    select user_id,ifnull(user_l1_nt_act,0) as user_l1_nt_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_nt_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date=30) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l3_nt_act;
create table feature.train_user_l3_nt_act as
    select user_id,ifnull(user_l3_nt_act,0) as user_l3_nt_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_nt_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 28 and 30) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l5_nt_act;
create table feature.train_user_l5_nt_act as
    select user_id,ifnull(user_l5_nt_act,0) as user_l5_nt_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l5_nt_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 26 and 30) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l7_nt_act;
create table feature.train_user_l7_nt_act as
    select user_id,ifnull(user_l7_nt_act,0) as user_l7_nt_act from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_nt_act from washed.tianchi_p_ten_buy_1_30 
    where (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);





drop table if exists feature.train_user_l1_nt_clk;
create table feature.train_user_l1_nt_clk as
    select user_id,ifnull(user_l1_nt_clk,0) as user_l1_nt_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_nt_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date=30) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_nt_clk;
create table feature.train_user_l3_nt_clk as
    select user_id,ifnull(user_l3_nt_clk,0) as user_l3_nt_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_nt_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 28 and 30) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l5_nt_clk;
create table feature.train_user_l5_nt_clk as
    select user_id,ifnull(user_l5_nt_clk,0) as user_l5_nt_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l5_nt_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 26 and 30) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l7_nt_clk;
create table feature.train_user_l7_nt_clk as
    select user_id,ifnull(user_l7_nt_clk,0) as user_l7_nt_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_nt_clk from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=1 and (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);



drop table if exists feature.train_user_l1_nt_buy;
create table feature.train_user_l1_nt_buy as
    select user_id,ifnull(user_l1_nt_buy,0) as user_l1_nt_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_nt_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date=30) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists feature.train_user_l3_nt_buy;
create table feature.train_user_l3_nt_buy as
    select user_id,ifnull(user_l3_nt_buy,0) as user_l3_nt_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_nt_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date between 28 and 30) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l5_nt_buy;
create table feature.train_user_l5_nt_buy as
    select user_id,ifnull(user_l5_nt_buy,0) as user_l5_nt_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l5_nt_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date between 26 and 30) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists feature.train_user_l7_nt_buy;
create table feature.train_user_l7_nt_buy as
    select user_id,ifnull(user_l7_nt_buy,0) as user_l7_nt_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_nt_buy from washed.tianchi_p_ten_buy_1_30 
    where behavior_type=4 and (event_date between 26 and 30 or event_date between 22 and 23) and (event_time between 2 and 7) group by user_id)b
    on (a.user_id=b.user_id_1);



6. 用户对所有商品在所有30天、28天内（不含双12）、双12期间、最后7天、14天 点击to购买转化率、点击to留待转化率、留待to购买转化率

drop table if exists feature.train_user_1_30_buy;
create table feature.train_user_1_30_buy as
    select user_id,count(behavior_type) as user_1_30_buy from washed.tianchi_p_ten_buy_1_30 where behavior_type=4 group by user_id;

drop table if exists feature.train_user_1_30_clk;
create table feature.train_user_1_30_clk as
    select user_id,count(behavior_type) as user_1_30_clk from washed.tianchi_p_ten_buy_1_30 where behavior_type=1 group by user_id;

drop table if exists feature.train_user_1_30_later;
create table feature.train_user_1_30_later as
    select user_id,count(behavior_type) as user_1_30_later from washed.tianchi_p_ten_buy_1_30 where behavior_type between 2 and 3 group by user_id;



drop table if exists feature.train_user_d12_clk;
create table feature.train_user_d12_clk as
    select user_id,user_d12_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_clk from washed.tianchi_p_ten_buy_1_30 where behavior_type=1 and event_date between 24 and 25 group by user_id)b
    on(a.user_id=b.user_id_1);

drop table if exists feature.train_user_d12_buy;
create table feature.train_user_d12_buy as
    select user_id,user_d12_buy from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_buy from washed.tianchi_p_ten_buy_1_30 where behavior_type=4 and event_date between 24 and 25 group by user_id)b
    on(a.user_id=b.user_id_1);

drop table if exists feature.train_user_d12_later;
create table feature.train_user_d12_later as
    select user_id,user_d12_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_later from washed.tianchi_p_ten_buy_1_30 where behavior_type between 2 and 3 and event_date between 24 and 25 group by user_id)b
    on(a.user_id=b.user_id_1);


用户在30天内对所有商品的 点击to购买转化率(点击总次数/购买总次数) 点击to留待转化率、留待to购买转化率


drop table if exists feature.train_user_1_30_clk2buy;
create table feature.train_user_1_30_clk2buy as
    select user_id,ifnull(user_1_30_clk/user_1_30_buy,0) as user_1_30_clk2buy from
    (
    select user_id,user_1_30_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_1_30_clk from feature.train_user_1_30_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_1_30_buy from feature.train_user_1_30_buy)d
    on(c.user_id=d.user_id_2);

drop table if exists feature.train_user_1_30_clk2later;
create table feature.train_user_1_30_clk2later as
    select user_id,ifnull(user_1_30_clk/user_1_30_later,0) as user_1_30_clk2later from
    (
    select user_id,user_1_30_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_1_30_clk from feature.train_user_1_30_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_1_30_later from feature.train_user_1_30_later)d
    on(c.user_id=d.user_id_2);


drop table if exists feature.train_user_1_30_later2buy;
create table feature.train_user_1_30_later2buy as
    select user_id,ifnull(user_1_30_later/user_1_30_buy,0) as user_1_30_later2buy from
    (
    select user_id,user_1_30_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_1_30_later from feature.train_user_1_30_later)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_1_30_buy from feature.train_user_1_30_buy)d
    on(c.user_id=d.user_id_2);

用户在双12期间的点击to购买转化率(点击总次数/购买总次数) 、点击to留待转化率 （点击总次数/留待总次数)、留待to购买转化率

drop table if exists feature.train_user_d12_clk2buy;
create table feature.train_user_d12_clk2buy as
    select user_id,ifnull(user_d12_clk/user_d12_buy,0) as user_d12_clk2buy from
    (
    select user_id,user_d12_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_d12_clk from feature.train_user_d12_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_d12_buy from feature.train_user_d12_buy)d
    on(c.user_id=d.user_id_2);


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

drop table if exists feature.train_user_d12_later2buy;
create table feature.train_user_d12_later2buy as
    select user_id,ifnull(user_d12_later/user_d12_buy,0) as user_d12_later2buy from
    (
    select user_id,user_d12_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_d12_later from feature.train_user_d12_later)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_d12_buy from feature.train_user_d12_buy)d
    on(c.user_id=d.user_id_2);

用户在最近28天、14天、7天间的点击to购买转化率、点击to留待转化率、留待to购买转化率

drop table if exists feature.train_user_l28_clk2buy;
create table feature.train_user_l28_clk2buy as
    select user_id,ifnull(user_l28_clk/user_l28_buy,0) as user_l28_clk2buy from
    (
    select user_id,user_l28_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_l28_clk from feature.train_user_l28_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l28_buy from feature.train_user_l28_buy)d
    on(c.user_id=d.user_id_2);


drop table if exists feature.train_user_l28_clk2later;
create table feature.train_user_l28_clk2later as
    select user_id,ifnull(user_l28_clk/user_l28_later,0) as user_l28_clk2later from
    (
    select user_id,user_l28_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_l28_clk from feature.train_user_l28_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l28_later from feature.train_user_l28_later)d
    on(c.user_id=d.user_id_2);

drop table if exists feature.train_user_l28_later2buy;
create table feature.train_user_l28_later2buy as
    select user_id,ifnull(user_l28_later/user_l28_buy,0) as user_l28_later2buy from
    (
    select user_id,user_l28_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_l28_later from feature.train_user_l28_later)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l28_buy from feature.train_user_l28_buy)d
    on(c.user_id=d.user_id_2);



drop table if exists feature.train_user_l14_clk2buy;
create table feature.train_user_l14_clk2buy as
    select user_id,ifnull(user_l14_clk/user_l14_buy,0) as user_l14_clk2buy from
    (
    select user_id,user_l14_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_l14_clk from feature.train_user_l14_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l14_buy from feature.train_user_l14_buy)d
    on(c.user_id=d.user_id_2);


drop table if exists feature.train_user_l14_clk2later;
create table feature.train_user_l14_clk2later as
    select user_id,ifnull(user_l14_clk/user_l14_later,0) as user_l14_clk2later from
    (
    select user_id,user_l14_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_l14_clk from feature.train_user_l14_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l14_later from feature.train_user_l14_later)d
    on(c.user_id=d.user_id_2);

drop table if exists feature.train_user_l14_later2buy;
create table feature.train_user_l14_later2buy as
    select user_id,ifnull(user_l14_later/user_l14_buy,0) as user_l14_later2buy from
    (
    select user_id,user_l14_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_l14_later from feature.train_user_l14_later)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l14_buy from feature.train_user_l14_buy)d
    on(c.user_id=d.user_id_2);

drop table if exists feature.train_user_l7_clk2buy;
create table feature.train_user_l7_clk2buy as
    select user_id,ifnull(user_l7_clk/user_l7_buy,0) as user_l7_clk2buy from
    (
    select user_id,user_l7_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_l7_clk from feature.train_user_l7_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l7_buy from feature.train_user_l7_buy)d
    on(c.user_id=d.user_id_2);


drop table if exists feature.train_user_l7_clk2later;
create table feature.train_user_l7_clk2later as
    select user_id,ifnull(user_l7_clk/user_l7_later,0) as user_l7_clk2later from
    (
    select user_id,user_l7_clk from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_l7_clk from feature.train_user_l7_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l7_later from feature.train_user_l7_later)d
    on(c.user_id=d.user_id_2);

drop table if exists feature.train_user_l7_later2buy;
create table feature.train_user_l7_later2buy as
    select user_id,ifnull(user_l7_later/user_l7_buy,0) as user_l7_later2buy from
    (
    select user_id,user_l7_later from
    (select distinct user_id from washed.tianchi_p_ten_buy_1_30)a
    left outer join
    (select user_id as user_id_1,user_l7_later from feature.train_user_l7_later)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l7_buy from feature.train_user_l7_buy)d
    on(c.user_id=d.user_id_2);