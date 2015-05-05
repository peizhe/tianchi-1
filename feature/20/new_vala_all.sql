/*user_1_1   6*/ 
drop table if exists new.vala_user_l1_clk;
create table new.vala_user_l1_clk as
    select user_id,ifnull(user_l1_clk,0) as user_l1_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_clk from washed.tianchi_p_ten_10_29 
    where (behavior_type=1) and (event_date=29) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l3_clk;
create table new.vala_user_l3_clk as
    select user_id,ifnull(user_l3_clk,0) as user_l3_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_clk from washed.tianchi_p_ten_10_29 
    where (behavior_type=1) and (event_date between 27 and 29) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l7_clk;
create table new.vala_user_l7_clk as
    select user_id,ifnull(user_l7_clk,0) as user_l7_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_clk from washed.tianchi_p_ten_10_29 
    where (behavior_type=1) and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l10_clk;
create table new.vala_user_l10_clk as
    select user_id,ifnull(user_l10_clk,0) as user_l10_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l10_clk from washed.tianchi_p_ten_10_29 
    where (behavior_type=1) and (event_date between 26 and 29 or event_date between 18 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists new.vala_user_l14_clk;
create table new.vala_user_l14_clk as
    select user_id,ifnull(user_l14_clk,0) as user_l14_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l14_clk from washed.tianchi_p_ten_10_29 
    where (behavior_type=1) and (event_date between 26 and 29 or event_date between 14 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists new.vala_user_l18_clk;
create table new.vala_user_l18_clk as
    select user_id,ifnull(user_l18_clk,0) as user_l18_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l18_clk from washed.tianchi_p_ten_10_29 
    where (behavior_type=1) and (event_date between 26 and 29 or event_date between 10 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


/*user_1_2   6*/
drop table if exists new.vala_user_l1_buy;
create table new.vala_user_l1_buy as
    select user_id,ifnull(user_l1_buy,0) as user_l1_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_buy from washed.tianchi_p_ten_10_29 
    where (behavior_type=4) and (event_date=29) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l3_buy;
create table new.vala_user_l3_buy as
    select user_id,ifnull(user_l3_buy,0) as user_l3_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_buy from washed.tianchi_p_ten_10_29 
    where (behavior_type=4) and (event_date between 27 and 29) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l7_buy;
create table new.vala_user_l7_buy as
    select user_id,ifnull(user_l7_buy,0) as user_l7_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_buy from washed.tianchi_p_ten_10_29 
    where (behavior_type=4) and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l10_buy;
create table new.vala_user_l10_buy as
    select user_id,ifnull(user_l10_buy,0) as user_l10_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l10_buy from washed.tianchi_p_ten_10_29 
    where (behavior_type=4) and (event_date between 26 and 29 or event_date between 18 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists new.vala_user_l14_buy;
create table new.vala_user_l14_buy as
    select user_id,ifnull(user_l14_buy,0) as user_l14_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l14_buy from washed.tianchi_p_ten_10_29 
    where (behavior_type=4) and (event_date between 26 and 29 or event_date between 14 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists new.vala_user_l18_buy;
create table new.vala_user_l18_buy as
    select user_id,ifnull(user_l18_buy,0) as user_l18_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l18_buy from washed.tianchi_p_ten_10_29 
    where (behavior_type=4) and (event_date between 26 and 29 or event_date between 10 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

/*user_1_3   6*/

drop table if exists new.vala_user_l1_cart;
create table new.vala_user_l1_cart as
    select user_id,ifnull(user_l1_cart,0) as user_l1_cart from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l1_cart from washed.tianchi_p_ten_10_29 
    where (behavior_type=3) and (event_date=29) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l3_cart;
create table new.vala_user_l3_cart as
    select user_id,ifnull(user_l3_cart,0) as user_l3_cart from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_cart from washed.tianchi_p_ten_10_29 
    where (behavior_type=3) and (event_date between 27 and 29) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l7_cart;
create table new.vala_user_l7_cart as
    select user_id,ifnull(user_l7_cart,0) as user_l7_cart from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_cart from washed.tianchi_p_ten_10_29 
    where (behavior_type=3) and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l7_favor;
create table new.vala_user_l7_favor as
    select user_id,ifnull(user_l7_favor,0) as user_l7_favor from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_favor from washed.tianchi_p_ten_10_29 
    where (behavior_type=2) and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l10_favor;
create table new.vala_user_l10_favor as
    select user_id,ifnull(user_l10_favor,0) as user_l10_favor from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l10_favor from washed.tianchi_p_ten_10_29 
    where (behavior_type=2) and (event_date between 26 and 29 or event_date between 18 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);

drop table if exists new.vala_user_l14_favor;
create table new.vala_user_l14_favor as
    select user_id,ifnull(user_l14_favor,0) as user_l14_favor from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l14_favor from washed.tianchi_p_ten_10_29 
    where (behavior_type=2) and (event_date between 26 and 29 or event_date between 14 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


/*user_1_4    6*/


drop table if exists new.vala_user_l1_distinct_clk;
create table new.vala_user_l1_distinct_clk as
    select user_id,ifnull(user_l1_distinct_clk,0) as user_l1_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l1_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29
    where (behavior_type=1) and (event_date=29) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);


drop table if exists new.vala_user_l3_distinct_clk;
create table new.vala_user_l3_distinct_clk as
    select user_id,ifnull(user_l3_distinct_clk,0) as user_l3_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l3_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29
    where (behavior_type=1) and (event_date between 27 and 29) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);


drop table if exists new.vala_user_l7_distinct_clk;
create table new.vala_user_l7_distinct_clk as
    select user_id,ifnull(user_l7_distinct_clk,0) as user_l7_distinct_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l7_distinct_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29
    where (behavior_type=1) and (event_date between 26 and 29 or event_date between 21 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);


drop table if exists new.vala_user_l1_distinct_buy;
create table new.vala_user_l1_distinct_buy as
    select user_id,ifnull(user_l1_distinct_buy,0) as user_l1_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l1_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29
    where (behavior_type=4) and (event_date=29) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists new.vala_user_l3_distinct_buy;
create table new.vala_user_l3_distinct_buy as
    select user_id,ifnull(user_l3_distinct_buy,0) as user_l3_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l3_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29
    where (behavior_type=4) and (event_date between 27 and 29) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);

drop table if exists new.vala_user_l7_distinct_buy;
create table new.vala_user_l7_distinct_buy as
    select user_id,ifnull(user_l7_distinct_buy,0) as user_l7_distinct_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(item_id) as user_l7_distinct_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29
    where (behavior_type=4) and (event_date between 26 and 29 or event_date between 21 and 23) order by user_id,item_id)b
    group by user_id)c
    on (a.user_id=c.user_id_1);



/*user_2   4*/

drop table if exists new.vala_user_d12_buy;
create table new.vala_user_d12_buy as
    select user_id,ifnull(user_d12_buy,0) as user_d12_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_buy from washed.tianchi_p_ten_10_29 
    where (behavior_type=4) and (event_date between 24 and 25) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_d12_clk;
create table new.vala_user_d12_clk as
    select user_id,ifnull(user_d12_clk,0) as user_d12_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_d12_clk from washed.tianchi_p_ten_10_29 
    where (behavior_type=1) and (event_date between 24 and 25) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l_buy_date;
create table new.vala_user_l_buy_date as
select user_id,ifnull(user_l_buy_date,-50) as user_l_buy_date from
(select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
(select user_id as user_id_1,30-max(event_date) as user_l_buy_date from washed.tianchi_p_ten_10_29 
   where behavior_type=4  group by user_id)b
   on (a.user_id=b.user_id_1);



drop table if exists new.vala_user_l_act_date;
create table new.vala_user_l_act_date as 
select user_id,ifnull(user_l_act_date,-50) as user_l_act_date from
(select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join 
(select user_id as user_id_1,30-max(event_date) as user_l_act_date from washed.tianchi_p_ten_10_29 
   group by user_id)b
   on (a.user_id=b.user_id_1);



/*user_3_1    6*/

/* preparation for revert-rate */

drop table if exists new.vala_user_l3_later;
create table new.vala_user_l3_later as
    select user_id,ifnull(user_l3_later,0) as user_l3_later from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l3_later from washed.tianchi_p_ten_10_29 
    where (behavior_type between 2 and 3) and (event_date between 27 and 29) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l7_later;
create table new.vala_user_l7_later as
    select user_id,ifnull(user_l7_later,0) as user_l7_later from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l7_later from washed.tianchi_p_ten_10_29 
    where (behavior_type between 2 and 3) and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


drop table if exists new.vala_user_l14_later;
create table new.vala_user_l14_later as
    select user_id,ifnull(user_l14_later,0) as user_l14_later from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,count(behavior_type) as user_l14_later from washed.tianchi_p_ten_10_29 
    where (behavior_type between 2 and 3) and (event_date between 26 and 29 or event_date between 14 and 23) group by user_id)b
    on (a.user_id=b.user_id_1);


/* calculation of revert-rate */

drop table if exists new.vala_user_l3_clk2buy;
create table new.vala_user_l3_clk2buy as
    select user_id,ifnull(user_l3_clk/user_l3_buy,0) as user_l3_clk2buy from
    (
    select user_id,user_l3_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,user_l3_clk from new.vala_user_l3_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l3_buy from new.vala_user_l3_buy)d
    on(c.user_id=d.user_id_2);



drop table if exists new.vala_user_l3_later2buy;
create table new.vala_user_l3_later2buy as
    select user_id,ifnull(user_l3_later/user_l3_buy,0) as user_l3_later2buy from
    (
    select user_id,user_l3_later from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,user_l3_later from new.vala_user_l3_later)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l3_buy from new.vala_user_l3_buy)d
    on(c.user_id=d.user_id_2);




drop table if exists new.vala_user_l7_clk2buy;
create table new.vala_user_l7_clk2buy as
    select user_id,ifnull(user_l7_clk/user_l7_buy,0) as user_l7_clk2buy from
    (
    select user_id,user_l7_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,user_l7_clk from new.vala_user_l7_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l7_buy from new.vala_user_l7_buy)d
    on(c.user_id=d.user_id_2);


drop table if exists new.vala_user_l7_later2buy;
create table new.vala_user_l7_later2buy as
    select user_id,ifnull(user_l7_later/user_l7_buy,0) as user_l7_later2buy from
    (
    select user_id,user_l7_later from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,user_l7_later from new.vala_user_l7_later)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l7_buy from new.vala_user_l7_buy)d
    on(c.user_id=d.user_id_2);



drop table if exists new.vala_user_l14_clk2buy;
create table new.vala_user_l14_clk2buy as
    select user_id,ifnull(user_l14_clk/user_l14_buy,0) as user_l14_clk2buy from
    (
    select user_id,user_l14_clk from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,user_l14_clk from new.vala_user_l14_clk)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l14_buy from new.vala_user_l14_buy)d
    on(c.user_id=d.user_id_2);


drop table if exists new.vala_user_l14_later2buy;
create table new.vala_user_l14_later2buy as
    select user_id,ifnull(user_l14_later/user_l14_buy,0) as user_l14_later2buy from
    (
    select user_id,user_l14_later from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,user_l14_later from new.vala_user_l14_later)b
    on(a.user_id=b.user_id_1)
    )c
    left outer join
    (select user_id as user_id_2,user_l14_buy from new.vala_user_l14_buy)d
    on(c.user_id=d.user_id_2);





/*user_3_2     3*/


drop table if exists new.vala_user_l3_h1_buy;
create table new.vala_user_l3_h1_buy as
    select user_id,ifnull(user_l3_h1_buy,0) as user_l3_h1_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (
        select user_id as user_id_1,count(event_date) as user_l3_h1_buy from
        (select distinct user_id,event_date from washed.tianchi_p_ten_10_29 where 
            (event_date between 27 and 29) and behavior_type=4)b
        group by user_id
    )c
    on (a.user_id=c.user_id_1);


drop table if exists new.vala_user_l7_h1_buy;
create table new.vala_user_l7_h1_buy as
    select user_id,ifnull(user_l7_h1_buy,0) as user_l7_h1_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (
        select user_id as user_id_1,count(event_date) as user_l7_h1_buy from
        (select distinct user_id,event_date from washed.tianchi_p_ten_10_29 where 
            (event_date between 26 and 29 or event_date between 21 and 23) and behavior_type=4)b
        group by user_id
    )c
    on (a.user_id=c.user_id_1);

drop table if exists new.vala_user_l14_h1_buy;
create table new.vala_user_l14_h1_buy as
    select user_id,ifnull(user_l14_h1_buy,0) as user_l14_h1_buy from
    (select distinct user_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (
        select user_id as user_id_1,count(event_date) as user_l14_h1_buy from
        (select distinct user_id,event_date from washed.tianchi_p_ten_10_29 where 
            (event_date between 26 and 29 or event_date between 14 and 23) and behavior_type=4)b
        group by user_id
    )c
    on (a.user_id=c.user_id_1);




/*ui_1    16*/


drop table if exists new.vala_ui_l1_buy;
create table new.vala_ui_l1_buy as
    select user_id,item_id,ifnull(ui_l1_buy,0) as ui_l1_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l1_buy from washed.tianchi_p_ten_10_29 
    where behavior_type=4 and (event_date=29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l2_buy;
create table new.vala_ui_l2_buy as
    select user_id,item_id,ifnull(ui_l2_buy,0) as ui_l2_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l2_buy from washed.tianchi_p_ten_10_29 
    where behavior_type=4 and (event_date between 28 and 29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l3_buy;
create table new.vala_ui_l3_buy as
    select user_id,item_id,ifnull(ui_l3_buy,0) as ui_l3_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l3_buy from washed.tianchi_p_ten_10_29 
    where behavior_type=4 and (event_date between 27 and 29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l5_buy;
create table new.vala_ui_l5_buy as
    select user_id,item_id,ifnull(ui_l5_buy,0) as ui_l5_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l5_buy from washed.tianchi_p_ten_10_29 
    where behavior_type=4 and (event_date between 26 and 29 or event_date=23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists new.vala_ui_l7_buy;
create table new.vala_ui_l7_buy as
    select user_id,item_id,ifnull(ui_l7_buy,0) as ui_l7_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l7_buy from washed.tianchi_p_ten_10_29 
    where behavior_type=4 and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists new.vala_ui_l10_buy;
create table new.vala_ui_l10_buy as
    select user_id,item_id,ifnull(ui_l10_buy,0) as ui_l10_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l10_buy from washed.tianchi_p_ten_10_29 
    where behavior_type=4 and (event_date between 26 and 29 or event_date between 18 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists new.vala_ui_l14_buy;
create table new.vala_ui_l14_buy as
    select user_id,item_id,ifnull(ui_l14_buy,0) as ui_l14_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l14_buy from washed.tianchi_p_ten_10_29 
    where behavior_type=4 and (event_date between 26 and 29 or event_date between 14 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists new.vala_ui_l18_buy;
create table new.vala_ui_l18_buy as
    select user_id,item_id,ifnull(ui_l18_buy,0) as ui_l18_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l18_buy from washed.tianchi_p_ten_10_29 
    where behavior_type=4 and (event_date between 26 and 29 or event_date between 10 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);




drop table if exists new.vala_ui_l1_clk;
create table new.vala_ui_l1_clk as
    select user_id,item_id,ifnull(ui_l1_clk,0) as ui_l1_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l1_clk from washed.tianchi_p_ten_10_29 
    where behavior_type=1 and (event_date=29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l2_clk;
create table new.vala_ui_l2_clk as
    select user_id,item_id,ifnull(ui_l2_clk,0) as ui_l2_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l2_clk from washed.tianchi_p_ten_10_29 
    where behavior_type=1 and (event_date between 28 and 29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l3_clk;
create table new.vala_ui_l3_clk as
    select user_id,item_id,ifnull(ui_l3_clk,0) as ui_l3_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l3_clk from washed.tianchi_p_ten_10_29 
    where behavior_type=1 and (event_date between 27 and 29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l5_clk;
create table new.vala_ui_l5_clk as
    select user_id,item_id,ifnull(ui_l5_clk,0) as ui_l5_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l5_clk from washed.tianchi_p_ten_10_29 
    where behavior_type=1 and (event_date between 26 and 29 or event_date=23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists new.vala_ui_l7_clk;
create table new.vala_ui_l7_clk as
    select user_id,item_id,ifnull(ui_l7_clk,0) as ui_l7_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l7_clk from washed.tianchi_p_ten_10_29 
    where behavior_type=1 and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists new.vala_ui_l10_clk;
create table new.vala_ui_l10_clk as
    select user_id,item_id,ifnull(ui_l10_clk,0) as ui_l10_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l10_clk from washed.tianchi_p_ten_10_29 
    where behavior_type=1 and (event_date between 26 and 29 or event_date between 18 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists new.vala_ui_l14_clk;
create table new.vala_ui_l14_clk as
    select user_id,item_id,ifnull(ui_l14_clk,0) as ui_l14_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l14_clk from washed.tianchi_p_ten_10_29 
    where behavior_type=1 and (event_date between 26 and 29 or event_date between 14 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists new.vala_ui_l18_clk;
create table new.vala_ui_l18_clk as
    select user_id,item_id,ifnull(ui_l18_clk,0) as ui_l18_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l18_clk from washed.tianchi_p_ten_10_29 
    where behavior_type=1 and (event_date between 26 and 29 or event_date between 10 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);




/*ui_2  9*/


drop table if exists new.vala_ui_d12_clk;
create table new.vala_ui_d12_clk as
    select user_id,item_id,ifnull(ui_d12_clk,0) as ui_d12_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_d12_clk from washed.tianchi_p_ten_10_29 
    where behavior_type=1 and (event_date between 24 and 25) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_d12_buy;
create table new.vala_ui_d12_buy as
    select user_id,item_id,ifnull(ui_d12_buy,0) as ui_d12_buy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_d12_buy from washed.tianchi_p_ten_10_29 
    where behavior_type=4 and (event_date between 24 and 25) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l1_cart;
create table new.vala_ui_l1_cart as
    select user_id,item_id,ifnull(ui_l1_cart,0) as ui_l1_cart from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l1_cart from washed.tianchi_p_ten_10_29 
    where behavior_type=3 and (event_date=29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l2_cart;
create table new.vala_ui_l2_cart as
    select user_id,item_id,ifnull(ui_l2_cart,0) as ui_l2_cart from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l2_cart from washed.tianchi_p_ten_10_29 
    where behavior_type=3 and (event_date between 28 and 29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l3_cart;
create table new.vala_ui_l3_cart as
    select user_id,item_id,ifnull(ui_l3_cart,0) as ui_l3_cart from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l3_cart from washed.tianchi_p_ten_10_29 
    where behavior_type=3 and (event_date between 27 and 29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l7_cart;
create table new.vala_ui_l7_cart as
    select user_id,item_id,ifnull(ui_l7_cart,0) as ui_l7_cart from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l7_cart from washed.tianchi_p_ten_10_29 
    where behavior_type=3 and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);





drop table if exists new.vala_ui_l7_favor;
create table new.vala_ui_l7_favor as
    select user_id,item_id,ifnull(ui_l7_favor,0) as ui_l7_favor from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l7_favor from washed.tianchi_p_ten_10_29 
    where behavior_type=2 and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l14_favor;
create table new.vala_ui_l14_favor as
    select user_id,item_id,ifnull(ui_l14_favor,0) as ui_l14_favor from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l14_favor from washed.tianchi_p_ten_10_29 
    where behavior_type=2 and (event_date between 26 and 29 or event_date between 14 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);

drop table if exists new.vala_ui_l18_favor;
create table new.vala_ui_l18_favor as
    select user_id,item_id,ifnull(ui_l18_favor,0) as ui_l18_favor from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l18_favor from washed.tianchi_p_ten_10_29 
    where behavior_type=2 and (event_date between 26 and 29 or event_date between 10 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);



/*ui_3     7*/

drop table if exists new.vala_ui_l_act_date;
create table new.vala_ui_l_act_date as 
    select user_id,item_id,ifnull(ui_l_act_date,-50) as ui_l_act_date from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,30-max(event_date) as ui_l_act_date from washed.tianchi_p_ten_10_29 
    group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 


drop table if exists new.vala_ui_f_buy_date;
create table new.vala_ui_f_buy_date as 
    select user_id,item_id,ifnull(ui_f_buy_date,50) as ui_f_buy_date from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,30-min(event_date) as ui_f_buy_date from washed.tianchi_p_ten_10_29 
    where behavior_type=4 group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 


drop table if exists new.vala_ui_l_buy_date;
create table new.vala_ui_l_buy_date as 
    select user_id,item_id,ifnull(ui_l_buy_date,-50) as ui_l_buy_date from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,30-max(event_date) as ui_l_buy_date from washed.tianchi_p_ten_10_29 
    where behavior_type=4 group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 


drop table if exists new.vala_ui_fl_buy_distance;
create table new.vala_ui_fl_buy_distance as 
    select user_id,item_id,(ui_f_buy_date-ui_l_buy_date) as ui_fl_buy_distance from
    (select user_id,item_id,ui_l_buy_date from new.vala_ui_l_buy_date)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_buy_date from new.vala_ui_f_buy_date)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 



drop table if exists new.vala_ui_l_actday_clk;
create table new.vala_ui_l_actday_clk as
    select user_id,item_id,ifnull(ui_l_actday_clk,0) as ui_l_actday_clk from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_l_actday_clk from
    (
    select user_id,item_id,ui_l_act_date,behavior_type,event_date from
    (select user_id,item_id,behavior_type,event_date from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from new.vala_ui_l_act_date)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=1 and event_date=ui_l_act_date 
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);


drop table if exists new.vala_ui_l_actday_favor;
create table new.vala_ui_l_actday_favor as
    select user_id,item_id,ifnull(ui_l_actday_favor,0) as ui_l_actday_favor from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_l_actday_favor from
    (
    select user_id,item_id,ui_l_act_date,behavior_type,event_date from
    (select user_id,item_id,behavior_type,event_date from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from new.vala_ui_l_act_date)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=2 and event_date=ui_l_act_date 
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);


drop table if exists new.vala_ui_l_actday_buy;
create table new.vala_ui_l_actday_buy as
    select user_id,item_id,ifnull(ui_l_actday_buy,0) as ui_l_actday_buy from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_l_actday_buy from
    (
    select user_id,item_id,ui_l_act_date,behavior_type,event_date from
    (select user_id,item_id,behavior_type,event_date from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from new.vala_ui_l_act_date)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=4 and event_date=ui_l_act_date 
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);



/*ui_4_1      6*/

drop table if exists new.vala_ui_l3_h1_act;
create table new.vala_ui_l3_h1_act as
    select user_id,item_id,ifnull(ui_l3_h1_act,0) as ui_l3_h1_act from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (
        select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l3_h1_act from
        (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29 where 
            (event_date between 27 and 29)
            )b
        group by user_id,item_id
    )c
    on (a.user_id=c.user_id_1 and a.item_id=c.item_id_1);


drop table if exists new.vala_ui_l5_h1_act;
create table new.vala_ui_l5_h1_act as
    select user_id,item_id,ifnull(ui_l5_h1_act,0) as ui_l5_h1_act from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (
        select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l5_h1_act from
        (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29 where 
            (event_date between 26 and 29 or event_date=23)
            )b
        group by user_id,item_id
    )c
    on (a.user_id=c.user_id_1 and a.item_id=c.item_id_1);


drop table if exists new.vala_ui_l7_h1_act;
create table new.vala_ui_l7_h1_act as
    select user_id,item_id,ifnull(ui_l7_h1_act,0) as ui_l7_h1_act from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (
        select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l7_h1_act from
        (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29 where 
            (event_date between 26 and 29 or event_date between 21 and 23)
            )b
        group by user_id,item_id
    )c
    on (a.user_id=c.user_id_1 and a.item_id=c.item_id_1);


drop table if exists new.vala_ui_l3_h1_buy;
create table new.vala_ui_l3_h1_buy as
    select user_id,item_id,ifnull(ui_l3_h1_buy,0) as ui_l3_h1_buy from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (
        select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l3_h1_buy from
        (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29 where 
            (event_date between 27 and 29) and behavior_type=4
            )b
        group by user_id,item_id
    )c
    on (a.user_id=c.user_id_1 and a.item_id=c.item_id_1);


drop table if exists new.vala_ui_l5_h1_buy;
create table new.vala_ui_l5_h1_buy as
    select user_id,item_id,ifnull(ui_l5_h1_buy,0) as ui_l5_h1_buy from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (
        select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l5_h1_buy from
        (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29 where 
            (event_date between 26 and 29 or event_date=23) and behavior_type=4
            )b
        group by user_id,item_id
    )c
    on (a.user_id=c.user_id_1 and a.item_id=c.item_id_1);


drop table if exists new.vala_ui_l7_h1_buy;
create table new.vala_ui_l7_h1_buy as
    select user_id,item_id,ifnull(ui_l7_h1_buy,0) as ui_l7_h1_buy from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (
        select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l7_h1_buy from
        (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29 where 
            (event_date between 26 and 29 or event_date between 21 and 23) and behavior_type=4
            )b
        group by user_id,item_id
    )c
    on (a.user_id=c.user_id_1 and a.item_id=c.item_id_1);


/*ui_4_2      3*/

/*用户在购买此商品之前的点击次数*/

drop table if exists test.vala_ui_f_buy_date_time;
create table test.vala_ui_f_buy_date_time as 
    select user_id,item_id,ifnull(ui_f_buy_date_time,0) as ui_f_buy_date_time from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,min(event_date*100+event_time) as ui_f_buy_date_time from washed.tianchi_p_ten_10_29 
    where behavior_type=4 group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1); 

drop table if exists new.vala_ui_f_buybefore_clk;
create table new.vala_ui_f_buybefore_clk as
    select user_id,item_id,ifnull(ui_f_buybefore_clk,0) as ui_f_buybefore_clk from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_f_buybefore_clk from
    (
    select user_id,item_id,ui_f_buy_date_time,behavior_type,(event_date*100+event_time) as event_date_time from
    (select user_id,item_id,behavior_type,event_date,event_time from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_buy_date_time 
        from test.vala_ui_f_buy_date_time)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=1 and event_date_time<=ui_f_buy_date_time
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);

/*用户在购买此商品之前的收藏次数*/

drop table if exists new.vala_ui_f_buybefore_favor;
create table new.vala_ui_f_buybefore_favor as
    select user_id,item_id,ifnull(ui_f_buybefore_favor,0) as ui_f_buybefore_favor from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_f_buybefore_favor from
    (
    select user_id,item_id,ui_f_buy_date_time,behavior_type,(event_date*100+event_time) as event_date_time from
    (select user_id,item_id,behavior_type,event_date,event_time from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_buy_date_time 
        from test.vala_ui_f_buy_date_time)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=2 and event_date_time<=ui_f_buy_date_time
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);

/*用户在购买此商品之前的加购物车次数*/

drop table if exists new.vala_ui_f_buybefore_cart;
create table new.vala_ui_f_buybefore_cart as
    select user_id,item_id,ifnull(ui_f_buybefore_cart,0) as ui_f_buybefore_cart from 
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)d
    left outer join
    (
    select user_id as user_id_2,item_id as item_id_2,count(behavior_type) as ui_f_buybefore_cart from
    (
    select user_id,item_id,ui_f_buy_date_time,behavior_type,(event_date*100+event_time) as event_date_time from
    (select user_id,item_id,behavior_type,event_date,event_time from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_f_buy_date_time 
        from test.vala_ui_f_buy_date_time)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    where behavior_type=3 and event_date_time<=ui_f_buy_date_time
    group by user_id,item_id
    )e
    on(d.user_id=e.user_id_2 and d.item_id=e.item_id_2);


/*ui_5_1       6*/

/* preparation for revert-rate */
drop table if exists new.vala_ui_l3_later;
create table new.vala_ui_l3_later as
    select user_id,item_id,ifnull(ui_l3_later,0) as ui_l3_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l3_later from washed.tianchi_p_ten_10_29 
    where (behavior_type between 2 and 3) and (event_date between 27 and 29) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);



drop table if exists new.vala_ui_l7_later;
create table new.vala_ui_l7_later as
    select user_id,item_id,ifnull(ui_l7_later,0) as ui_l7_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l7_later from washed.tianchi_p_ten_10_29 
    where (behavior_type between 2 and 3) and (event_date between 26 and 29 or event_date between 21 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists new.vala_ui_l14_later;
create table new.vala_ui_l14_later as
    select user_id,item_id,ifnull(ui_l14_later,0) as ui_l14_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1, item_id as item_id_1, count(behavior_type) as ui_l14_later from washed.tianchi_p_ten_10_29 
    where (behavior_type between 2 and 3) and (event_date between 26 and 29 or event_date between 14 and 23) group by user_id,item_id)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


/* calculation of revert-rate */

drop table if exists new.vala_ui_l3_clk2buy;
create table new.vala_ui_l3_clk2buy as
    select user_id,item_id,ifnull(ui_l3_clk/ui_l3_buy,0) as ui_l3_clk2buy from
    (
    select user_id,item_id,ui_l3_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l3_clk from new.vala_ui_l3_clk)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    left outer join
    (select user_id as user_id_2,item_id as item_id_2,ui_l3_buy from new.vala_ui_l3_buy)d
    on(c.user_id=d.user_id_2 and c.item_id=d.item_id_2);



drop table if exists new.vala_ui_l3_later2buy;
create table new.vala_ui_l3_later2buy as
    select user_id,item_id,ifnull(ui_l3_later/ui_l3_buy,0) as ui_l3_later2buy from
    (
    select user_id,item_id,ui_l3_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l3_later from new.vala_ui_l3_later)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    left outer join
    (select user_id as user_id_2,item_id as item_id_2,ui_l3_buy from new.vala_ui_l3_buy)d
    on(c.user_id=d.user_id_2 and c.item_id=d.item_id_2);



drop table if exists new.vala_ui_l7_clk2buy;
create table new.vala_ui_l7_clk2buy as
    select user_id,item_id,ifnull(ui_l7_clk/ui_l7_buy,0) as ui_l7_clk2buy from
    (
    select user_id,item_id,ui_l7_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l7_clk from new.vala_ui_l7_clk)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    left outer join
    (select user_id as user_id_2,item_id as item_id_2,ui_l7_buy from new.vala_ui_l7_buy)d
    on(c.user_id=d.user_id_2 and c.item_id=d.item_id_2);


drop table if exists new.vala_ui_l7_later2buy;
create table new.vala_ui_l7_later2buy as
    select user_id,item_id,ifnull(ui_l7_later/ui_l7_buy,0) as ui_l7_later2buy from
    (
    select user_id,item_id,ui_l7_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l7_later from new.vala_ui_l7_later)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    left outer join
    (select user_id as user_id_2,item_id as item_id_2,ui_l7_buy from new.vala_ui_l7_buy)d
    on(c.user_id=d.user_id_2 and c.item_id=d.item_id_2);


drop table if exists new.vala_ui_l14_clk2buy;
create table new.vala_ui_l14_clk2buy as
    select user_id,item_id,ifnull(ui_l14_clk/ui_l14_buy,0) as ui_l14_clk2buy from
    (
    select user_id,item_id,ui_l14_clk from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l14_clk from new.vala_ui_l14_clk)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    left outer join
    (select user_id as user_id_2,item_id as item_id_2,ui_l14_buy from new.vala_ui_l14_buy)d
    on(c.user_id=d.user_id_2 and c.item_id=d.item_id_2);


drop table if exists new.vala_ui_l14_later2buy;
create table new.vala_ui_l14_later2buy as
    select user_id,item_id,ifnull(ui_l14_later/ui_l14_buy,0) as ui_l14_later2buy from
    (
    select user_id,item_id,ui_l14_later from
    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1,ui_l14_later from new.vala_ui_l14_later)b
    on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
    )c
    left outer join
    (select user_id as user_id_2,item_id as item_id_2,ui_l14_buy from new.vala_ui_l14_buy)d
    on(c.user_id=d.user_id_2 and c.item_id=d.item_id_2);


/*ui_5_2   4*/

drop table if exists new.vala_ui_l_act_distance;
create table new.vala_ui_l_act_distance as 
    select user_id,item_id,(user_l_act_date-ui_l_act_date) as ui_l_act_distance from
    (select user_id,item_id,ui_l_act_date from new.vala_ui_l_act_date)a
    left outer join
    (select user_id as user_id_1,user_l_act_date from new.vala_user_l_act_date)b
    on (a.user_id=b.user_id_1); 



/*用户地理位置信息*/
drop table if exists test.vala_user_geo;
create table test.vala_user_geo as 
SELECT user_id,item_id,user_geohash FROM washed.tianchi_p_ten_10_29
order by user_id,item_id;

/*商品地理位置信息*/
drop table if exists test.item_geo;
create table test.item_geo as  
SELECT item_id,item_geohash FROM tianchi.item order by item_id;

/*用户-商品地理位置信息（两个都有位置信息的）*/
drop table if exists test.user_item_geo;
 create table test.user_item_geo as 
 select * from(
 select user_id,item_id,user_geohash,item_geohash from
    (select user_id,item_id,user_geohash from test.vala_user_geo)a
    left outer join
    (select item_id as item_id_1,item_geohash from test.item_geo)b
    on(a.item_id=b.item_id_1)
    )c where user_geohash is not null and item_geohash is not null;

/*用户-商品之间距离（没有地理位置信息的记为0）*/
drop table if exists test.user_item_distance;
create table test.user_item_distance as 
select user_id,item_id,ifnull(distance,0) as distance from
(select distinct user_id,item_id from washed.tianchi_p_ten_10_29)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,case when user_geohash=item_geohash THEN '7'
WHEN left(user_geohash,6)=left(item_geohash,6)  THEN '6'
WHEN left(user_geohash,5)=left(item_geohash,5)  THEN '5'
WHEN left(user_geohash,4)=left(item_geohash,4)  THEN '4'
WHEN left(user_geohash,3)=left(item_geohash,3)  THEN '3'
WHEN left(user_geohash,2)=left(item_geohash,2)  THEN '2'
WHEN left(user_geohash,1)=left(item_geohash,1)  THEN '1'
ELSE '0' END distance from test.user_item_geo)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


/*用户-商品之间距离统计*/

drop table if exists new.vala_ui_distance;
create table new.vala_ui_distance as 
select user_id,item_id,max(distance) as ui_min_distance,avg(distance) as ui_avg_distance,min(distance) as ui_max_distance from
test.user_item_distance group by user_id,item_id;



/*ui_6       3*/


/*用户最后一次交互此商品的后一天交互了多少个其它商品*/

drop table if exists new.vala_ui_l_actnextday_other_distinct_act;
create table new.vala_ui_l_actnextday_other_distinct_act as
select user_id,item_id,ifnull(user_ui_l_actnextday_distinct_act,0) as ui_l_actnextday_other_distinct_act from
    (
    select user_id_1,item_id_1,count(distinct item_id) as user_ui_l_actnextday_distinct_act from
    (
    select user_id_1,item_id_1,item_id,ui_l_act_date,event_date from
    (
        select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29 
        order by user_id,event_date
    )a
    right outer join
    (
        select user_id as user_id_1,item_id as item_id_1,ui_l_act_date 
        from new.vala_ui_l_act_date
    )b
    on(a.user_id=b.user_id_1 and a.event_date=b.ui_l_act_date+1)
    order by user_id_1,ui_l_act_date,item_id_1
    )c 
    where item_id is not null 
    group by user_id_1,item_id_1
    )d 
    right outer join
    (
        select distinct user_id,item_id from washed.tianchi_p_ten_10_29
    )e
    on(d.user_id_1=e.user_id and d.item_id_1=e.item_id);



/*用户在最近1天有多少单天只交互了此商品*/


drop table if exists new.vala_ui_l1_h1_only_act;
create table new.vala_ui_l1_h1_only_act as

    select user_id,item_id,ifnull(ui_l1_h1_only_act,0) as  ui_l1_h1_only_act from

    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)aaa

    left outer join

    (select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l1_h1_only_act from

    (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29
    where event_date=29 
    order by user_id,event_date)aa
     
    join
    
    (select user_id as user_id_1,event_date as event_date_1,user_distinct_act from
    (
    select user_id,event_date,count(distinct item_id) as user_distinct_act from
    (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29
        where event_date=29)a
    group by user_id,event_date
    )b
    where user_distinct_act=1 
    order by user_id,event_date
    )c

    on(aa.user_id=c.user_id_1 and aa.event_date=c.event_date_1)
    group by user_id,item_id
    )d

    on(aaa.user_id=d.user_id_1 and aaa.item_id=d.item_id_1);


/*用户在最近3天有多少单天只交互了此商品*/

drop table if exists new.vala_ui_l3_h1_only_act;
create table new.vala_ui_l3_h1_only_act as

    select user_id,item_id,ifnull(ui_l3_h1_only_act,0) as  ui_l3_h1_only_act from

    (select distinct user_id,item_id from washed.tianchi_p_ten_10_29)aaa

    left outer join

    (select user_id as user_id_1,item_id as item_id_1,count(event_date) as ui_l3_h1_only_act from

    (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29
    where event_date between 27 and 29 
    order by user_id,event_date)aa
     
    join
    
    (select user_id as user_id_1,event_date as event_date_1,user_distinct_act from
    (
    select user_id,event_date,count(distinct item_id) as user_distinct_act from
    (select distinct user_id,item_id,event_date from washed.tianchi_p_ten_10_29
        where event_date between 27 and 29)a
    group by user_id,event_date
    )b
    where user_distinct_act=1 
    order by user_id,event_date
    )c

    on(aa.user_id=c.user_id_1 and aa.event_date=c.event_date_1)
    group by user_id,item_id
    )d

    on(aaa.user_id=d.user_id_1 and aaa.item_id=d.item_id_1);