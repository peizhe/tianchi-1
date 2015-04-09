# 双12期间平均每天购买次数
drop table if exists feature.item_d12_buy;
create table feature.item_d12_buy as
	select item_id,(count(*)/2) as buy from washed.tianchi_p_1_30
    where event_date between 24 and 25 and behavior_type=4
    group by item_id;

# 双12期间平均每天点击次数    
drop table if exists feature.item_d12_clk;
create table feature.item_d12_clk as
	select item_id,(count(*)/2) as clk from washed.tianchi_p_1_30
    where event_date between 24 and 25 and behavior_type=1
    group by item_id;

# 双12期间平均每天交互次数    
drop table if exists feature.item_d12_act;
create table feature.item_d12_act as
	select item_id,(count(*)/2) as act from washed.tianchi_p_1_30
    where event_date between 24 and 25
    group by item_id;
    
# ===================================================================

# 双12期间人均购买次数
drop table if exists feature.item_d12_ave_buy;
create table feature.item_d12_ave_buy as
	select item_id, (count(*)/count(distinct(user_id))) as ave_buy from washed.tianchi_p_1_30
    where event_date between 24 and 25 and behavior_type=4
    group by item_id;
    
# 双12期间人均点击次数
drop table if exists feature.item_d12_ave_clk;
create table feature.item_d12_ave_clk as
	select item_id, (count(*)/count(distinct(user_id))) as ave_clk from washed.tianchi_p_1_30
    where event_date between 24 and 25 and behavior_type=1
    group by item_id;
    
# 双12期间人均交互次数
drop table if exists feature.item_d12_ave_act;
create table feature.item_d12_ave_act as
	select item_id, (count(*)/count(distinct(user_id))) as ave_act from washed.tianchi_p_1_30
    where event_date between 24 and 25
    group by item_id;
    
# ===================================================================
# 第1周日均购买量
drop table if exists feature.train_1_7_buy;
create table feature.train_1_7_buy as
    select item_id,(sum/7) as buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 1 and 7 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周日均交互量
drop table if exists feature.train_1_7_act;
create table feature.train_1_7_act as
    select item_id,(sum/7) as act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 1 and 7 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周人均购买量
drop table if exists feature.train_1_7_ave_buy;
create table feature.train_1_7_ave_buy as
    select item_id, ifnull(buy/user,0) as ave_buy 
    from (select item_id,buy from feature.train_1_7_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 1 and 7 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周人均交互量
drop table if exists feature.train_1_7_ave_act;
create table feature.train_1_7_ave_act as
    select item_id, ifnull(act/user,0) as ave_act 
    from (select item_id,act from feature.train_1_7_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 1 and 7 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第2周日均购买量
drop table if exists feature.train_8_14_buy;
create table feature.train_8_14_buy as
    select item_id,(sum/7) as buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 8 and 14 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周日均交互量
drop table if exists feature.train_8_14_act;
create table feature.train_8_14_act as
    select item_id,(sum/7) as act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 8 and 14 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周人均购买量
drop table if exists feature.train_8_14_ave_buy;
create table feature.train_8_14_ave_buy as
    select item_id, ifnull(buy/user,0) as ave_buy 
    from (select item_id,buy from feature.train_8_14_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 8 and 14 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周人均交互量
drop table if exists feature.train_8_14_ave_act;
create table feature.train_8_14_ave_act as
    select item_id, ifnull(act/user,0) as ave_act 
    from (select item_id,act from feature.train_8_14_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 8 and 14 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第3周日均购买量
drop table if exists feature.train_15_21_buy;
create table feature.train_15_21_buy as
    select item_id,(sum/7) as buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 15 and 21 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周日均交互量
drop table if exists feature.train_15_21_act;
create table feature.train_15_21_act as
    select item_id,(sum/7) as act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 15 and 21 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周人均购买量
drop table if exists feature.train_15_21_ave_buy;
create table feature.train_15_21_ave_buy as
    select item_id, ifnull(buy/user,0) as ave_buy 
    from (select item_id,buy from feature.train_15_21_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 15 and 21 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周人均交互量
drop table if exists feature.train_15_21_ave_act;
create table feature.train_15_21_ave_act as
    select item_id, ifnull(act/user,0) as ave_act 
    from (select item_id,act from feature.train_15_21_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 15 and 21 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第4周日均购买量
drop table if exists feature.train_22_30_buy;
create table feature.train_22_30_buy as
    select item_id,(sum/7) as buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周日均交互量
drop table if exists feature.train_22_30_act;
create table feature.train_22_30_act as
    select item_id,(sum/7) as act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周人均购买量
drop table if exists feature.train_22_30_ave_buy;
create table feature.train_22_30_ave_buy as
    select item_id, ifnull(buy/user,0) as ave_buy 
    from (select item_id,buy from feature.train_22_30_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周人均交互量
drop table if exists feature.train_22_30_ave_act;
create table feature.train_22_30_ave_act as
    select item_id, ifnull(act/user,0) as ave_act 
    from (select item_id,act from feature.train_22_30_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# =======================================================================

# 第1周日均购买量
drop table if exists feature.pre_2_8_buy;
create table feature.pre_2_8_buy as
    select item_id,(sum/7) as buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 2 and 8 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周日均交互量
drop table if exists feature.pre_2_8_act;
create table feature.pre_2_8_act as
    select item_id,(sum/7) as act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 2 and 8 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周人均购买量
drop table if exists feature.pre_2_8_ave_buy;
create table feature.pre_2_8_ave_buy as
    select item_id, ifnull(buy/user,0) as ave_buy 
    from (select item_id,buy from feature.pre_2_8_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 2 and 8 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周人均交互量
drop table if exists feature.pre_2_8_ave_act;
create table feature.pre_2_8_ave_act as
    select item_id, ifnull(act/user,0) as ave_act 
    from (select item_id,act from feature.pre_2_8_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 2 and 8 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第2周日均购买量
drop table if exists feature.pre_9_15_buy;
create table feature.pre_9_15_buy as
    select item_id,(sum/7) as buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 9 and 15 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周日均交互量
drop table if exists feature.pre_9_15_act;
create table feature.pre_9_15_act as
    select item_id,(sum/7) as act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 9 and 15 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周人均购买量
drop table if exists feature.pre_9_15_ave_buy;
create table feature.pre_9_15_ave_buy as
    select item_id, ifnull(buy/user,0) as ave_buy 
    from (select item_id,buy from feature.pre_9_15_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 9 and 15 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周人均交互量
drop table if exists feature.pre_9_15_ave_act;
create table feature.pre_9_15_ave_act as
    select item_id, ifnull(act/user,0) as ave_act 
    from (select item_id,act from feature.pre_9_15_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 9 and 15 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第3周日均购买量
drop table if exists feature.pre_16_22_buy;
create table feature.pre_16_22_buy as
    select item_id,(sum/7) as buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 16 and 22 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周日均交互量
drop table if exists feature.pre_16_22_act;
create table feature.pre_16_22_act as
    select item_id,(sum/7) as act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 16 and 22 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周人均购买量
drop table if exists feature.pre_16_22_ave_buy;
create table feature.pre_16_22_ave_buy as
    select item_id, ifnull(buy/user,0) as ave_buy 
    from (select item_id,buy from feature.pre_16_22_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 16 and 22 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周人均交互量
drop table if exists feature.pre_16_22_ave_act;
create table feature.pre_16_22_ave_act as
    select item_id, ifnull(act/user,0) as ave_act 
    from (select item_id,act from feature.pre_16_22_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 16 and 22 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第4周日均购买量
drop table if exists feature.pre_23_31_buy;
create table feature.pre_23_31_buy as
    select item_id,(sum/7) as buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周日均交互量
drop table if exists feature.pre_23_31_act;
create table feature.pre_23_31_act as
    select item_id,(sum/7) as act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周人均购买量
drop table if exists feature.pre_23_31_ave_buy;
create table feature.pre_23_31_ave_buy as
    select item_id, ifnull(buy/user,0) as ave_buy 
    from (select item_id,buy from feature.pre_23_31_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周人均交互量
drop table if exists feature.pre_23_31_ave_act;
create table feature.pre_23_31_ave_act as
    select item_id, ifnull(act/user,0) as ave_act 
    from (select item_id,act from feature.pre_23_31_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) group by item_id_1) as b
    on a.item_id=b.item_id_1;