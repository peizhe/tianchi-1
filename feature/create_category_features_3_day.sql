# 选出购买过商品的条目
drop table if exists test.buy;
create table test.buy as
	select user_id,item_category,event_date from washed.tianchi_p_1_30
    where behavior_type=4;
# 去掉双十二
delete from test.buy where event_date in (24,25);

# 选出多次购买的
drop table if exists test.buy_many_times;
create table test.buy_many_times as
	select item_category,user_id,count(*) as times 
	from test.buy
	group by item_category,user_id
    having times>1;

# 选出在同一天多次购买的
drop table if exists test.buy_many_times_in_1_day;
create table test.buy_many_times_in_1_day as
	select item_category,user_id,event_date,count(*) as times
    from test.buy
    group by item_category,user_id,event_date
    having times>1;
    
# 从多次购买中去除同一天多次购买的条目
drop table if exists test.category_buy_times_diff_days;
create table test.category_buy_times_diff_days as
	select item_category,user_id, ifnull(a.times1-b.times2,0) as times
    from
    (select item_category,user_id,times as times1 from test.buy_many_times) as a
    left outer join
    (select item_category as item_category_1,user_id as user_id_1, times as times2 
        from test.buy_many_times_in_1_day) as b
    on a.item_category=b.item_category_1 and a.user_id=b.user_id_1;
delete from test.category_buy_times_diff_days where times<=1;

# 统计商品总共被多少人在不同天买过
drop table if exists feature.train_category_l28_distinct_day_buy;
create table feature.train_category_l28_distinct_day_buy as
	select category_id,ifnull(repeat_user,0) as category_distinct_day_buy from
    (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as item_category_1,count(distinct user_id) as repeat_user 
        from test.category_buy_times_diff_days group by item_category_1) as b
    on a.category_id=b.item_category_1;
    
drop table test.buy;
drop table test.buy_many_times;
drop table test.buy_many_times_in_1_day;
drop table test.category_buy_times_diff_days;

# ============================================================================

# 选出购买过商品的条目
drop table if exists test.buy;
create table test.buy as
    select user_id,item_category,event_date from washed.tianchi_p_2_31
    where behavior_type=4;
# 去掉双十二
delete from test.buy where event_date in (24,25);

# 选出多次购买的
drop table if exists test.buy_many_times;
create table test.buy_many_times as
    select item_category,user_id,count(*) as times 
    from test.buy
    group by item_category,user_id
    having times>1;

# 选出在同一天多次购买的
drop table if exists test.buy_many_times_in_1_day;
create table test.buy_many_times_in_1_day as
    select item_category,user_id,event_date,count(*) as times
    from test.buy
    group by item_category,user_id,event_date
    having times>1;
    
# 从多次购买中去除同一天多次购买的条目
drop table if exists test.category_buy_times_diff_days;
create table test.category_buy_times_diff_days as
    select item_category,user_id, ifnull(a.times1-b.times2,0) as times
    from
    (select item_category,user_id,times as times1 from test.buy_many_times) as a
    left outer join
    (select item_category as item_category_1,user_id as user_id_1, times as times2 
        from test.buy_many_times_in_1_day) as b
    on a.item_category=b.item_category_1 and a.user_id=b.user_id_1;
delete from test.category_buy_times_diff_days where times<=1;

# 统计商品总共被多少人在不同天买过
drop table if exists feature.pre_category_l28_distinct_day_buy;
create table feature.pre_category_l28_distinct_day_buy as
    select category_id,ifnull(repeat_user,0) as category_distinct_day_buy from
    (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as item_category_1,count(distinct user_id) as repeat_user 
        from test.category_buy_times_diff_days group by item_category_1) as b
    on a.category_id=b.item_category_1;
    
drop table test.buy;
drop table test.buy_many_times;
drop table test.buy_many_times_in_1_day;
drop table test.category_buy_times_diff_days;

# ============================================================================

drop table if exists feature.train_category_l28_rebuyrate;
create table feature.train_category_l28_rebuyrate as
	select category_id,ifnull(category_distinct_day_buy/user_sum,0) as category_rebuy_rate
    from (select category_id,category_distinct_day_buy from feature.train_category_l28_distinct_day_buy) as a
    left outer join
    (select item_category as item_category_1,count(distinct user_id) as user_sum 
        from washed.tianchi_p_1_30
    where behavior_type=4 group by item_category_1) as b
    on a.category_id=b.item_category_1;
    
drop table if exists feature.pre_category_l28_rebuyrate;
create table feature.pre_category_l28_rebuyrate as
    select category_id,ifnull(category_distinct_day_buy/user_sum,0) as category_rebuy_rate
    from (select category_id,category_distinct_day_buy from feature.pre_category_l28_distinct_day_buy) as a
    left outer join
    (select item_category as item_category_1,count(distinct user_id) as user_sum 
        from washed.tianchi_p_2_31
    where behavior_type=4 group by item_category_1) as b
    on a.category_id=b.item_category_1;

# ============================================================================   