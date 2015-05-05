/* day_ave_buy*/
drop table if exists new.train_item_l1_buy;
create table new.train_item_l1_buy as
    select item_id,ifnull(item_l1_buy,0) as item_l1_buy from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(behavior_type) as item_l1_buy from washed.tianchi_p_ten_11_30 
    where (behavior_type=4) and (event_date=30) group by item_id)b
    on (a.item_id=b.item_id_1);


drop table if exists new.train_item_l3_buy;
create table new.train_item_l3_buy as
    select item_id,ifnull(item_l3_buy,0)/3 as item_l3_buy from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(behavior_type) as item_l3_buy from washed.tianchi_p_ten_11_30 
    where (behavior_type=4) and (event_date between 28 and 30) group by item_id)b
    on (a.item_id=b.item_id_1);


drop table if exists new.train_item_l7_buy;
create table new.train_item_l7_buy as
    select item_id,ifnull(item_l7_buy,0)/7 as item_l7_buy from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(behavior_type) as item_l7_buy from washed.tianchi_p_ten_11_30 
    where (behavior_type=4) and (event_date between 26 and 30 or event_date between 22 and 23) group by item_id)b
    on (a.item_id=b.item_id_1);


drop table if exists new.train_item_l14_buy;
create table new.train_item_l14_buy as
    select item_id,ifnull(item_l14_buy,0)/14 as item_l14_buy from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(behavior_type) as item_l14_buy from washed.tianchi_p_ten_11_30 
    where (behavior_type=4) and (event_date between 17 and 30) group by item_id)b
    on (a.item_id=b.item_id_1);


/*day_ave_clk*/
drop table if exists new.train_item_l1_clk;
create table new.train_item_l1_clk as
    select item_id,ifnull(item_l1_clk,0) as item_l1_clk from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(behavior_type) as item_l1_clk from washed.tianchi_p_ten_11_30 
    where (behavior_type=1) and (event_date=30) group by item_id)b
    on (a.item_id=b.item_id_1);


drop table if exists new.train_item_l3_clk;
create table new.train_item_l3_clk as
    select item_id,ifnull(item_l3_clk,0)/3 as item_l3_clk from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(behavior_type) as item_l3_clk from washed.tianchi_p_ten_11_30 
    where (behavior_type=1) and (event_date between 28 and 30) group by item_id)b
    on (a.item_id=b.item_id_1);


drop table if exists new.train_item_l7_clk;
create table new.train_item_l7_clk as
    select item_id,ifnull(item_l7_clk,0)/7 as item_l7_clk from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(behavior_type) as item_l7_clk from washed.tianchi_p_ten_11_30 
    where (behavior_type=1) and (event_date between 26 and 30 or event_date between 22 and 23) group by item_id)b
    on (a.item_id=b.item_id_1);


drop table if exists new.train_item_l14_clk;
create table new.train_item_l14_clk as
    select item_id,ifnull(item_l14_clk,0)/14 as item_l14_clk from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(behavior_type) as item_l14_clk from washed.tianchi_p_ten_11_30 
    where (behavior_type=1) and (event_date between 17 and 30) group by item_id)b
    on (a.item_id=b.item_id_1);



/*day_distinct_clk*/


drop table if exists new.train_item_l1_distinct_clk;
create table new.train_item_l1_distinct_clk as
    select item_id,ifnull(item_l1_distinct_clk,0) as item_l1_distinct_clk from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(user_id) as item_l1_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30
    where (behavior_type=1) and (event_date=30) order by user_id,item_id)b
    group by item_id)c
    on (a.item_id=c.item_id_1);


drop table if exists new.train_item_l3_distinct_clk;
create table new.train_item_l3_distinct_clk as
    select item_id,ifnull(item_l3_distinct_clk,0) as item_l3_distinct_clk from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(user_id) as item_l3_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30
    where (behavior_type=1) and (event_date between 28 and 30) order by user_id,item_id)b
    group by item_id)c
    on (a.item_id=c.item_id_1);


drop table if exists new.train_item_l7_distinct_clk;
create table new.train_item_l7_distinct_clk as
    select item_id,ifnull(item_l7_distinct_clk,0) as item_l7_distinct_clk from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(user_id) as item_l7_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30
    where (behavior_type=1) and (event_date between 26 and 30 or event_date between 22 and 23) order by user_id,item_id)b
    group by item_id)c
    on (a.item_id=c.item_id_1);


drop table if exists new.train_item_l14_distinct_clk;
create table new.train_item_l14_distinct_clk as
    select item_id,ifnull(item_l14_distinct_clk,0) as item_l14_distinct_clk from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(user_id) as item_l14_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30
    where (behavior_type=1) and (event_date between 17 and 30) order by user_id,item_id)b
    group by item_id)c
    on (a.item_id=c.item_id_1);


/*day_distinct_buy*/


drop table if exists new.train_item_l1_distinct_buy;
create table new.train_item_l1_distinct_buy as
    select item_id,ifnull(item_l1_distinct_buy,0) as item_l1_distinct_buy from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(user_id) as item_l1_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30
    where (behavior_type=4) and (event_date=30) order by user_id,item_id)b
    group by item_id)c
    on (a.item_id=c.item_id_1);


drop table if exists new.train_item_l3_distinct_buy;
create table new.train_item_l3_distinct_buy as
    select item_id,ifnull(item_l3_distinct_buy,0) as item_l3_distinct_buy from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(user_id) as item_l3_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30
    where (behavior_type=4) and (event_date between 28 and 30) order by user_id,item_id)b
    group by item_id)c
    on (a.item_id=c.item_id_1);


drop table if exists new.train_item_l7_distinct_buy;
create table new.train_item_l7_distinct_buy as
    select item_id,ifnull(item_l7_distinct_buy,0) as item_l7_distinct_buy from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(user_id) as item_l7_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30
    where (behavior_type=4) and (event_date between 26 and 30 or event_date between 22 and 23) order by user_id,item_id)b
    group by item_id)c
    on (a.item_id=c.item_id_1);


drop table if exists new.train_item_l14_distinct_buy;
create table new.train_item_l14_distinct_buy as
    select item_id,ifnull(item_l14_distinct_buy,0) as item_l14_distinct_buy from
    (select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select item_id as item_id_1,count(user_id) as item_l14_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30
    where (behavior_type=4) and (event_date between 17 and 30) order by user_id,item_id)b
    group by item_id)c
    on (a.item_id=c.item_id_1);



/*last_clk_buy*/
drop table if exists new.train_item_l_buy_date;
create table new.train_item_l_buy_date as
select item_id,ifnull(item_l_buy_date,-50) as item_l_buy_date from
(select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join
(select item_id as item_id_1,31-max(event_date) as item_l_buy_date from washed.tianchi_p_ten_11_30 
   where behavior_type=4  group by item_id)b
   on (a.item_id=b.item_id_1);



drop table if exists new.train_item_l_act_date;
create table new.train_item_l_act_date as 
select item_id,ifnull(item_l_act_date,-50) as item_l_act_date from
(select distinct item_id from washed.tianchi_p_ten_11_30)a
    left outer join 
(select item_id as item_id_1,31-max(event_date) as item_l_act_date from washed.tianchi_p_ten_11_30 
   group by item_id)b
   on (a.item_id=b.item_id_1);


/*rebuy_features*/

drop table if exists new.train_item_ifrebuy;
create table new.train_item_ifrebuy as
select item_id,ifnull(item_ifrebuy,0) as item_ifrebuy from
(select distinct item_id from washed.tianchi_p_ten_11_30)e
    left outer join
(select item_id as item_id_1, 1 as item_ifrebuy from
(select distinct item_id from
(select item_id,user_id,count(event_date) as days from
(select distinct item_id,user_id,event_date from washed.tianchi_p_ten_11_30 
   where behavior_type=4)a group by item_id,user_id)b where days>1)c)d
on(e.item_id=d.item_id_1);


drop table if exists new.train_item_distinct_rebuy;
create table new.train_item_distinct_rebuy as
select item_id,ifnull(item_distinct_rebuy,0) as item_distinct_rebuy from
(select distinct item_id from washed.tianchi_p_ten_11_30)e
    left outer join
(select item_id as item_id_1,count(user_id) as item_distinct_rebuy from
(select item_id,user_id from
(select item_id,user_id,count(event_date) as days from
(select distinct item_id,user_id,event_date from washed.tianchi_p_ten_11_30 
   where behavior_type=4)a group by item_id,user_id)b where days>1)c group by item_id)d
on(e.item_id=d.item_id_1);