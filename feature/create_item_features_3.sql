# 商品第一次被购买的日期
drop table if exists feature.train_item_first_buy;
create table feature.train_item_first_buy as
	select item_id,ifnull(buy_date,0) as item_f_buy_date from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, min(event_date) as buy_date from washed.tianchi_p_1_30
    where behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 商品最后一次被购买的日期
drop table if exists feature.train_item_last_buy;
create table feature.train_item_last_buy as
	select item_id,ifnull(buy_date,0) as item_l_buy_date from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, max(event_date) as buy_date from washed.tianchi_p_1_30
    where behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 商品第一次交互的日期
drop table if exists feature.train_item_first_act;
create table feature.train_item_first_act as
	select item_id,ifnull(buy_date,0) as item_f_act_date  from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, min(event_date) as buy_date from washed.tianchi_p_1_30
    group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 商品最后一次交互的日期
drop table if exists feature.train_item_last_act;
create table feature.train_item_last_act as
	select item_id,ifnull(buy_date,0) as item_l_act_date from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, max(event_date) as buy_date from washed.tianchi_p_1_30
    group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 商品第一次被购买的日期
drop table if exists feature.pre_item_first_buy;
create table feature.pre_item_first_buy as
    select item_id,ifnull(buy_date,0) as item_f_buy_date from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, min(event_date) as buy_date from washed.tianchi_p_2_31
    where behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 商品最后一次被购买的日期
drop table if exists feature.pre_item_last_buy;
create table feature.pre_item_last_buy as
    select item_id,ifnull(buy_date,0) as item_l_buy_date from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, max(event_date) as buy_date from washed.tianchi_p_2_31
    where behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 商品第一次交互的日期
drop table if exists feature.pre_item_first_act;
create table feature.pre_item_first_act as
    select item_id,ifnull(buy_date,0) as item_f_act_date from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, min(event_date) as buy_date from washed.tianchi_p_2_31
    group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 商品最后一次交互的日期
drop table if exists feature.pre_item_last_act;
create table feature.pre_item_last_act as
    select item_id,ifnull(buy_date,0) as item_l_act_date from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, max(event_date) as buy_date from washed.tianchi_p_2_31
    group by item_id_1) as b
    on a.item_id=b.item_id_1;