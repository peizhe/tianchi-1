# 双12期间平均每天购买次数
drop table if exists feature.item_d12_buy;
create table feature.item_d12_buy as
	select item_id, ifnull(item_d12_buy,0) as item_d12_buy from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_b,(count(*)/2) as item_d12_buy from washed.tianchi_p_ten_1_30
    where event_date between 24 and 25 and behavior_type=4
    group by item_id_b) as b
    on item_id=item_id_b;

# 双12期间平均每天点击次数    
drop table if exists feature.item_d12_clk;
create table feature.item_d12_clk as
    select item_id, ifnull(item_d12_clk,0) as item_d12_clk from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_b,(count(*)/2) as item_d12_clk from washed.tianchi_p_ten_1_30
    where event_date between 24 and 25 and behavior_type=1
    group by item_id_b) as b
    on item_id=item_id_b;

# 双12期间平均每天交互次数    
drop table if exists feature.item_d12_act;
create table feature.item_d12_act as
    select item_id, ifnull(item_d12_act,0) as item_d12_act from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_b,(count(*)/2) as item_d12_act from washed.tianchi_p_ten_1_30
    where event_date between 24 and 25
    group by item_id_b) as b
    on item_id=item_id_b;
    
# ===================================================================

# 双12期间人均购买次数
drop table if exists feature.item_d12_ave_buy;
create table feature.item_d12_ave_buy as
    select item_id, ifnull(item_d12_ave_buy,0) as item_d12_ave_buy from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_b,(count(*)/count(distinct(user_id))) as item_d12_ave_buy
    from washed.tianchi_p_1_30
    where event_date between 24 and 25 and behavior_type=4
    group by item_id_b) as b
    on item_id=item_id_b;
    
# 双12期间人均点击次数
drop table if exists feature.item_d12_ave_clk;
create table feature.item_d12_ave_clk as
    select item_id, ifnull(item_d12_ave_clk,0) as item_d12_ave_clk from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_b,(count(*)/count(distinct(user_id))) as item_d12_ave_clk
    from washed.tianchi_p_1_30
    where event_date between 24 and 25 and behavior_type=1
    group by item_id_b) as b
    on item_id=item_id_b;
    
# 双12期间人均交互次数
drop table if exists feature.item_d12_ave_act;
create table feature.item_d12_ave_act as
    select item_id, ifnull(item_d12_ave_act,0) as item_d12_ave_act from
    (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_b,(count(*)/count(distinct(user_id))) as item_d12_ave_act
    from washed.tianchi_p_1_30
    where event_date between 24 and 25
    group by item_id_b) as b
    on item_id=item_id_b;
    
# ===================================================================
# 第1周日均购买量
drop table if exists feature.train_item_1_7_buy;
create table feature.train_item_1_7_buy as
    select item_id,(sum/7) as item_1_7_buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 1 and 7 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周日均交互量
drop table if exists feature.train_item_1_7_act;
create table feature.train_item_1_7_act as
    select item_id,(sum/7) as item_1_7_act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 1 and 7 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周人均购买量
drop table if exists feature.train_item_1_7_ave_buy;
create table feature.train_item_1_7_ave_buy as
    select item_id, ifnull(item_1_7_buy/user,0) as item_1_7_ave_buy 
    from (select item_id,item_1_7_buy from feature.train_item_1_7_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 1 and 7 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周人均交互量
drop table if exists feature.train_item_1_7_ave_act;
create table feature.train_item_1_7_ave_act as
    select item_id, ifnull(item_1_7_act/user,0) as item_1_7_ave_act 
    from (select item_id,item_1_7_act from feature.train_item_1_7_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 1 and 7 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第2周日均购买量
drop table if exists feature.train_item_8_14_buy;
create table feature.train_item_8_14_buy as
    select item_id,(sum/7) as item_8_14_buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 8 and 14 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周日均交互量
drop table if exists feature.train_item_8_14_act;
create table feature.train_item_8_14_act as
    select item_id,(sum/7) as item_8_14_act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 8 and 14 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周人均购买量
drop table if exists feature.train_item_8_14_ave_buy;
create table feature.train_item_8_14_ave_buy as
    select item_id, ifnull(item_8_14_buy/user,0) as item_8_14_ave_buy 
    from (select item_id,item_8_14_buy from feature.train_item_8_14_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 8 and 14 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周人均交互量
drop table if exists feature.train_item_8_14_ave_act;
create table feature.train_item_8_14_ave_act as
    select item_id, ifnull(item_8_14_act/user,0) as item_8_14_ave_act 
    from (select item_id,item_8_14_act from feature.train_item_8_14_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 8 and 14 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第3周日均购买量
drop table if exists feature.train_item_15_21_buy;
create table feature.train_item_15_21_buy as
    select item_id,(sum/7) as item_15_21_buy 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 15 and 21 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周日均交互量
drop table if exists feature.train_item_15_21_act;
create table feature.train_item_15_21_act as
    select item_id,(sum/7) as item_15_21_act 
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date between 15 and 21 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周人均购买量
drop table if exists feature.train_item_15_21_ave_buy;
create table feature.train_item_15_21_ave_buy as
    select item_id, ifnull(item_15_21_buy/user,0) as item_15_21_ave_buy 
    from (select item_id,item_15_21_buy from feature.train_item_15_21_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 15 and 21 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周人均交互量
drop table if exists feature.train_item_15_21_ave_act;
create table feature.train_item_15_21_ave_act as
    select item_id, ifnull(item_15_21_act/user,0) as item_15_21_ave_act
    from (select item_id,item_15_21_act from feature.train_item_15_21_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date between 15 and 21 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第4周日均购买量
drop table if exists feature.train_item_22_30_buy;
create table feature.train_item_22_30_buy as
    select item_id,(sum/7) as item_22_30_buy
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周日均交互量
drop table if exists feature.train_item_22_30_act;
create table feature.train_item_22_30_act as
    select item_id,(sum/7) as item_22_30_act
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周人均购买量
drop table if exists feature.train_item_22_30_ave_buy;
create table feature.train_item_22_30_ave_buy as
    select item_id, ifnull(item_22_30_buy/user,0) as item_22_30_ave_buy
    from (select item_id,item_22_30_buy from feature.train_item_22_30_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周人均交互量
drop table if exists feature.train_item_22_30_ave_act;
create table feature.train_item_22_30_ave_act as
    select item_id, ifnull(item_22_30_act/user,0) as item_22_30_ave_act
    from (select item_id,item_22_30_act from feature.train_item_22_30_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_1_30
    where event_date in (22,23,26,27,28,29,30) group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# =======================================================================

# 第1周日均购买量
drop table if exists feature.pre_item_2_8_buy;
create table feature.pre_item_2_8_buy as
    select item_id,(sum/7) as item_2_8_buy
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 2 and 8 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周日均交互量
drop table if exists feature.pre_item_2_8_act;
create table feature.pre_item_2_8_act as
    select item_id,(sum/7) as item_2_8_act
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 2 and 8 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周人均购买量
drop table if exists feature.pre_item_2_8_ave_buy;
create table feature.pre_item_2_8_ave_buy as
    select item_id, ifnull(item_2_8_buy/user,0) as item_2_8_ave_buy
    from (select item_id,item_2_8_buy from feature.pre_item_2_8_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 2 and 8 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第1周人均交互量
drop table if exists feature.pre_item_2_8_ave_act;
create table feature.pre_item_2_8_ave_act as
    select item_id, ifnull(item_2_8_act/user,0) as item_2_8_ave_act
    from (select item_id,item_2_8_act from feature.pre_item_2_8_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 2 and 8 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第2周日均购买量
drop table if exists feature.pre_item_9_15_buy;
create table feature.pre_item_9_15_buy as
    select item_id,(sum/7) as item_9_15_buy
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 9 and 15 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周日均交互量
drop table if exists feature.pre_item_9_15_act;
create table feature.pre_item_9_15_act as
    select item_id,(sum/7) as item_9_15_act
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 9 and 15 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周人均购买量
drop table if exists feature.pre_item_9_15_ave_buy;
create table feature.pre_item_9_15_ave_buy as
    select item_id, ifnull(item_9_15_buy/user,0) as item_9_15_ave_buy
    from (select item_id,item_9_15_buy from feature.pre_item_9_15_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 9 and 15 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第2周人均交互量
drop table if exists feature.pre_item_9_15_ave_act;
create table feature.pre_item_9_15_ave_act as
    select item_id, ifnull(item_9_15_act/user,0) as item_9_15_ave_act
    from (select item_id,item_9_15_act from feature.pre_item_9_15_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 9 and 15 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第3周日均购买量
drop table if exists feature.pre_item_16_22_buy;
create table feature.pre_item_16_22_buy as
    select item_id,(sum/7) as item_16_22_buy
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 16 and 22 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周日均交互量
drop table if exists feature.pre_item_16_22_act;
create table feature.pre_item_16_22_act as
    select item_id,(sum/7) as item_16_22_act
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date between 16 and 22 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周人均购买量
drop table if exists feature.pre_item_16_22_ave_buy;
create table feature.pre_item_16_22_ave_buy as
    select item_id, ifnull(item_16_22_buy/user,0) as item_16_22_ave_buy
    from (select item_id,item_16_22_buy from feature.pre_item_16_22_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 16 and 22 and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第3周人均交互量
drop table if exists feature.pre_item_16_22_ave_act;
create table feature.pre_item_16_22_ave_act as
    select item_id, ifnull(item_16_22_act/user,0) as item_16_22_ave_act
    from (select item_id,item_16_22_act from feature.pre_item_16_22_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date between 16 and 22 group by item_id_1) as b
    on a.item_id=b.item_id_1;
    
# 第4周日均购买量
drop table if exists feature.pre_item_23_31_buy;
create table feature.pre_item_23_31_buy as
    select item_id,(sum/7) as item_23_31_buy
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周日均交互量
drop table if exists feature.pre_item_23_31_act;
create table feature.pre_item_23_31_act as
    select item_id,(sum/7) as item_23_31_act
    from (select item_id from washed.item_subset) as a
    left outer join
    (select item_id as item_id_1, count(*) as sum from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周人均购买量
drop table if exists feature.pre_item_23_31_ave_buy;
create table feature.pre_item_23_31_ave_buy as
    select item_id, ifnull(item_23_31_buy/user,0) as item_23_31_ave_buy
    from (select item_id,item_23_31_buy from feature.pre_item_23_31_buy) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) and behavior_type=4 group by item_id_1) as b
    on a.item_id=b.item_id_1;

# 第4周人均交互量
drop table if exists feature.pre_item_23_31_ave_act;
create table feature.pre_item_23_31_ave_act as
    select item_id, ifnull(item_23_31_act/user,0) as item_23_31_ave_act
    from (select item_id,item_23_31_act from feature.pre_item_23_31_act) as a
    left outer join
    (select item_id as item_id_1,count(distinct(user_id)) as user from washed.tianchi_p_2_31
    where event_date in (23,26,27,28,29,30,31) group by item_id_1) as b
    on a.item_id=b.item_id_1;