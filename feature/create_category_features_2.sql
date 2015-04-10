# 双12期间平均每天购买次数
drop table if exists feature.category_d12_buy;
create table feature.category_d12_buy as
    select item_category,(count(*)/2) as buy from washed.tianchi_p_1_30
    where event_date between 24 and 25 and behavior_type=4
    group by item_category;

# 双12期间平均每天交互次数    
drop table if exists feature.category_d12_act;
create table feature.category_d12_act as
    select item_category,(count(*)/2) as act from washed.tianchi_p_1_30
    where event_date between 24 and 25
    group by item_category;
    
# ===================================================================

# 双12期间人均购买次数
drop table if exists feature.category_d12_ave_buy;
create table feature.category_d12_ave_buy as
    select item_category, (count(*)/count(distinct(user_id))) as ave_buy from washed.tianchi_p_1_30
    where event_date between 24 and 25 and behavior_type=4
    group by item_category;
    
# 双12期间人均交互次数
drop table if exists feature.category_d12_ave_act;
create table feature.category_d12_ave_act as
    select item_category, (count(*)/count(distinct(user_id))) as ave_act from washed.tianchi_p_1_30
    where event_date between 24 and 25
    group by item_category;
    
# ===================================================================
# 第1周日均购买量
drop table if exists feature.train_category_1_7_buy;
create table feature.train_category_1_7_buy as
    select category_id,(sum/7) as buy 
    from (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as category_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 1 and 7 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;

# 第1周人均购买量
drop table if exists feature.train_category_1_7_ave_buy;
create table feature.train_category_1_7_ave_buy as
    select category_id, ifnull(buy/user,0) as ave_buy 
    from (select category_id,buy from feature.train_category_1_7_buy) as a
    left outer join
    (select item_category as category_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 1 and 7 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;
    
# 第2周日均购买量
drop table if exists feature.train_category_8_14_buy;
create table feature.train_category_8_14_buy as
    select category_id,(sum/7) as buy 
    from (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as category_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 8 and 14 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;

# 第2周人均购买量
drop table if exists feature.train_category_8_14_ave_buy;
create table feature.train_category_8_14_ave_buy as
    select category_id, ifnull(buy/user,0) as ave_buy 
    from (select category_id,buy from feature.train_category_8_14_buy) as a
    left outer join
    (select item_category as category_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 8 and 14 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;
    
# 第3周日均购买量
drop table if exists feature.train_category_15_21_buy;
create table feature.train_category_15_21_buy as
    select category_id,(sum/7) as buy 
    from (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as category_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 15 and 21 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;

# 第3周人均购买量
drop table if exists feature.train_category_15_21_ave_buy;
create table feature.train_category_15_21_ave_buy as
    select category_id, ifnull(buy/user,0) as ave_buy 
    from (select category_id,buy from feature.train_category_15_21_buy) as a
    left outer join
    (select item_category as category_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 15 and 21 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;
    
# 第4周日均购买量
drop table if exists feature.train_category_22_30_buy;
create table feature.train_category_22_30_buy as
    select category_id,(sum/7) as buy 
    from (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as category_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;

# 第4周人均购买量
drop table if exists feature.train_category_22_30_ave_buy;
create table feature.train_category_22_30_ave_buy as
    select category_id, ifnull(buy/user,0) as ave_buy 
    from (select category_id,buy from feature.train_category_22_30_buy) as a
    left outer join
    (select item_category as category_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;
    
# =======================================================================

# 第1周日均购买量
drop table if exists feature.pre_category_2_8_buy;
create table feature.pre_category_2_8_buy as
    select category_id,(sum/7) as buy 
    from (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as category_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 2 and 8 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;

# 第1周人均购买量
drop table if exists feature.pre_category_2_8_ave_buy;
create table feature.pre_category_2_8_ave_buy as
    select category_id, ifnull(buy/user,0) as ave_buy 
    from (select category_id,buy from feature.pre_category_2_8_buy) as a
    left outer join
    (select item_category as category_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 2 and 8 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;
    
# 第2周日均购买量
drop table if exists feature.pre_category_9_15_buy;
create table feature.pre_category_9_15_buy as
    select category_id,(sum/7) as buy 
    from (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as category_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 9 and 15 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;

# 第2周人均购买量
drop table if exists feature.pre_category_9_15_ave_buy;
create table feature.pre_category_9_15_ave_buy as
    select category_id, ifnull(buy/user,0) as ave_buy 
    from (select category_id,buy from feature.pre_category_9_15_buy) as a
    left outer join
    (select item_category as category_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 9 and 15 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;
    
# 第3周日均购买量
drop table if exists feature.pre_category_16_22_buy;
create table feature.pre_category_16_22_buy as
    select category_id,(sum/7) as buy 
    from (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as category_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 16 and 22 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;

# 第3周人均购买量
drop table if exists feature.pre_category_16_22_ave_buy;
create table feature.pre_category_16_22_ave_buy as
    select category_id, ifnull(buy/user,0) as ave_buy 
    from (select category_id,buy from feature.pre_category_16_22_buy) as a
    left outer join
    (select item_category as category_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 16 and 22 and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;
    
# 第4周日均购买量
drop table if exists feature.pre_category_23_31_buy;
create table feature.pre_category_23_31_buy as
    select category_id,(sum/7) as buy 
    from (select category_id from washed.category_subset) as a
    left outer join
    (select item_category as category_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;

# 第4周人均购买量
drop table if exists feature.pre_category_23_31_ave_buy;
create table feature.pre_category_23_31_ave_buy as
    select category_id, ifnull(buy/user,0) as ave_buy 
    from (select category_id,buy from feature.pre_category_23_31_buy) as a
    left outer join
    (select item_category as category_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) and behavior_type=4 group by category_id_1) as b
    on a.category_id=b.category_id_1;