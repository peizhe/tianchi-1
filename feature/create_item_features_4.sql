# 选出购买过商品的条目
drop table if exists test.buy;
create table test.buy as
	select user_id,item_id,event_date from washed.tianchi_p_1_30
    where behavior_type=4;
# 去掉双十二
delete from test.buy where event_date in (24,25);

# 选出多次购买的
drop table if exists test.buy_many_times;
create table test.buy_many_times as
	select item_id,user_id,count(*) as times 
	from test.buy
	group by item_id,user_id
    having times>1;

# 选出在同一天多次购买的
drop table if exists test.buy_many_times_in_1_day;
create table test.buy_many_times_in_1_day as
	select item_id,user_id,event_date,count(*) as times
    from test.buy
    group by item_id,user_id,event_date
    having times>1;
    
# 从多次购买中去除同一天多次购买的条目
drop table if exists test.item_buy_times_diff_days;
create table test.item_buy_times_diff_days as
	select item_id,user_id, ifnull(a.times1-b.times2,0) as times
    from
    (select item_id,user_id,times as times1 from test.buy_many_times) as a
    left outer join
    (select item_id as item_id_1,user_id as user_id_1, times as times2 
        from test.buy_many_times_in_1_day) as b
    on a.item_id=b.item_id_1 and a.user_id=b.user_id_1;
delete from test.item_buy_times_diff_days where times<=1;

# 统计商品总共被多少人在不同天买过
drop table if exists feature.train_item_l28_distinct_day_buy;
create table feature.train_item_l28_distinct_day_buy as
	select item_id,ifnull(repeat_user,0) as item_distinct_day_buy from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1,count(distinct user_id) as repeat_user 
        from test.item_buy_times_diff_days group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
drop table test.buy;
drop table test.buy_many_times;
drop table test.buy_many_times_in_1_day;
drop table test.item_buy_times_diff_days;

# ============================================================================

# 选出购买过商品的条目
drop table if exists test.buy;
create table test.buy as
	select user_id,item_id,event_date from washed.tianchi_p_2_31
    where behavior_type=4;
# 去掉双十二
delete from test.buy where event_date in (24,25);

# 选出多次购买的
drop table if exists test.buy_many_times;
create table test.buy_many_times as
	select item_id,user_id,count(*) as times 
	from test.buy
	group by item_id,user_id
    having times>1;

# 选出在同一天多次购买的
drop table if exists test.buy_many_times_in_1_day;
create table test.buy_many_times_in_1_day as
	select item_id,user_id,event_date,count(*) as times
    from test.buy
    group by item_id,user_id,event_date
    having times>1;
    
# 从多次购买中去除同一天多次购买的条目
drop table if exists test.item_buy_times_diff_days;
create table test.item_buy_times_diff_days as
	select item_id,user_id, ifnull(a.times1-b.times2,0) as times
    from
    (select item_id,user_id,times as times1 from test.buy_many_times) as a
    left outer join
    (select item_id as item_id_1,user_id as user_id_1, times as times2 
        from test.buy_many_times_in_1_day) as b
    on a.item_id=b.item_id_1 and a.user_id=b.user_id_1;
delete from test.item_buy_times_diff_days where times<=1;

# 统计商品总共被多少人在不同天买过
drop table if exists feature.pre_item_l28_distinct_day_buy;
create table feature.pre_item_l28_distinct_day_buy as
	select item_id,ifnull(repeat_user,0) as item_distinct_day_buy from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1,count(distinct user_id) as repeat_user 
        from test.item_buy_times_diff_days group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
drop table test.buy;
drop table test.buy_many_times;
drop table test.buy_many_times_in_1_day;
drop table test.item_buy_times_diff_days;

# ============================================================================

drop table if exists feature.train_item_l28_rebuyrate;
create table feature.train_item_l28_rebuyrate as
	select item_id,ifnull(item_distinct_day_buy/user_sum,0) as item_rebuy_rate
    from (select item_id,item_distinct_day_buy from feature.train_item_l28_distinct_day_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct user_id) as user_sum from washed.tianchi_p_1_30
    where behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
drop table if exists feature.pre_item_l28_rebuyrate;
create table feature.pre_item_l28_rebuyrate as
    select item_id,ifnull(item_distinct_day_buy/user_sum,0) as item_rebuy_rate
    from (select item_id,item_distinct_day_buy from feature.pre_item_l28_distinct_day_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct user_id) as user_sum from washed.tianchi_p_2_31
    where behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# ============================================================================   

drop table if exists feature.item_d12_buy_user;
create table feature.item_d12_buy_user as
	select item_id, ifnull(user,0) as d12_buy_user from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_b, count(distinct user_id) as user from
    washed.tianchi_p_ten_1_30 where event_date in (24,25) and behavior_type=4
    group by item_id_b) as be
    on item_id=item_id_b;