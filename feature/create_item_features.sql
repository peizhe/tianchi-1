# 最后一天点击量
drop table if exists feature.train_item_l1_click;
CREATE TABLE feature.train_item_l1_click AS SELECT item_id, IFNULL(l1_click, 0) AS item_l1_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, COUNT(behavior_type) AS l1_click
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 1 AND event_date = 30
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l1_click;
CREATE TABLE feature.pre_item_l1_click AS SELECT item_id, IFNULL(l1_click, 0) AS item_l1_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, COUNT(behavior_type) AS l1_click
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 1 AND event_date = 31
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后3天点击量
drop table if exists feature.train_item_l3_click;
CREATE TABLE feature.train_item_l3_click AS SELECT item_id, IFNULL(l3_click, 0) AS item_l3_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 3) AS l3_click
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 1
            AND event_date BETWEEN 28 AND 30
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l3_click;
CREATE TABLE feature.pre_item_l3_click AS SELECT item_id, IFNULL(l3_click, 0) AS item_l3_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 3) AS l3_click
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 1
            AND event_date BETWEEN 29 AND 31
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后7天点击量
drop table if exists feature.train_item_l7_click;
CREATE TABLE feature.train_item_l7_click AS SELECT item_id, IFNULL(l7_click, 0) AS item_l7_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 7) AS l7_click
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 1
            AND event_date BETWEEN 22 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l7_click;
CREATE TABLE feature.pre_item_l7_click AS SELECT item_id, IFNULL(l7_click, 0) AS item_l7_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 7) AS l7_click
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 1
            AND event_date BETWEEN 23 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后14天点击量
drop table if exists feature.train_item_l14_click;
CREATE TABLE feature.train_item_l14_click AS SELECT item_id, IFNULL(l14_click, 0) AS item_l14_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 14) AS l14_click
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 1
            AND event_date BETWEEN 15 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l14_click;
CREATE TABLE feature.pre_item_l14_click AS SELECT item_id, IFNULL(l14_click, 0) AS item_l14_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 14) AS l14_click
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 1
            AND event_date BETWEEN 16 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后28天点击量
drop table if exists feature.train_item_l28_click;
CREATE TABLE feature.train_item_l28_click AS SELECT item_id, IFNULL(l28_click, 0) AS item_l28_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 28) AS l28_click
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 1
            AND event_date BETWEEN 1 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l28_click;
CREATE TABLE feature.pre_item_l28_click AS SELECT item_id, IFNULL(l28_click, 0) AS item_l28_click FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 28) AS l28_click
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 1
            AND event_date BETWEEN 2 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;

# ======================================================
# 最后一天购买量
drop table if exists feature.train_item_l1_buy;
CREATE TABLE feature.train_item_l1_buy AS SELECT item_id, IFNULL(l1_buy, 0) AS item_l1_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, COUNT(behavior_type) AS l1_buy
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 4 AND event_date = 30
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l1_buy;
CREATE TABLE feature.pre_item_l1_buy AS SELECT item_id, IFNULL(l1_buy, 0) AS item_l1_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, COUNT(behavior_type) AS l1_buy
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 4 AND event_date = 31
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后3天购买量
drop table if exists feature.train_item_l3_buy;
CREATE TABLE feature.train_item_l3_buy AS SELECT item_id, IFNULL(l3_buy, 0) AS item_l3_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 3) AS l3_buy
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 28 AND 30
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l3_buy;
CREATE TABLE feature.pre_item_l3_buy AS SELECT item_id, IFNULL(l3_buy, 0) AS item_l3_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 3) AS l3_buy
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 29 AND 31
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后7天购买量
drop table if exists feature.train_item_l7_buy;
CREATE TABLE feature.train_item_l7_buy AS SELECT item_id, IFNULL(l7_buy, 0) AS item_l7_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 7) AS l7_buy
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 22 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l7_buy;
CREATE TABLE feature.pre_item_l7_buy AS SELECT item_id, IFNULL(l7_buy, 0) AS item_l7_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 7) AS l7_buy
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 23 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后14天购买量
drop table if exists feature.train_item_l14_buy;
CREATE TABLE feature.train_item_l14_buy AS SELECT item_id, IFNULL(l14_buy, 0) AS item_l14_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 14) AS l14_buy
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 15 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l14_buy;
CREATE TABLE feature.pre_item_l14_buy AS SELECT item_id, IFNULL(l14_buy, 0) AS item_l14_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 14) AS l14_buy
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 16 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后28天购买量
drop table if exists feature.train_item_l28_buy;
CREATE TABLE feature.train_item_l28_buy AS SELECT item_id, IFNULL(l28_buy, 0) AS item_l28_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 28) AS l28_buy
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 1 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l28_buy;
CREATE TABLE feature.pre_item_l28_buy AS SELECT item_id, IFNULL(l28_buy, 0) AS item_l28_buy FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(behavior_type) / 28) AS l28_buy
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 2 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# ===============================================================================================

# 最后一天交互量
drop table if exists feature.train_item_l1_inter;
CREATE TABLE feature.train_item_l1_inter AS SELECT item_id, IFNULL(l1_inter, 0) AS item_l1_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, COUNT(event_date) AS l1_inter
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date = 30
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l1_inter;
CREATE TABLE feature.pre_item_l1_inter AS SELECT item_id, IFNULL(l1_inter, 0) AS item_l1_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, COUNT(event_date) AS l1_inter
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date = 31
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后3天交互量
drop table if exists feature.train_item_l3_inter;
CREATE TABLE feature.train_item_l3_inter AS SELECT item_id, IFNULL(l3_inter, 0) AS item_l3_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(event_date) / 3) AS l3_inter
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 28 AND 30
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l3_inter;
CREATE TABLE feature.pre_item_l3_inter AS SELECT item_id, IFNULL(l3_inter, 0) AS item_l3_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(event_date) / 3) AS l3_inter
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 29 AND 31
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后7天交互量
drop table if exists feature.train_item_l7_inter;
CREATE TABLE feature.train_item_l7_inter AS SELECT item_id, IFNULL(l7_inter, 0) AS item_l7_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(event_date) / 7) AS l7_inter
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 22 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l7_inter;
CREATE TABLE feature.pre_item_l7_inter AS SELECT item_id, IFNULL(l7_inter, 0) AS item_l7_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(event_date) / 7) AS l7_inter
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 23 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后14天交互量
drop table if exists feature.train_item_l14_inter;
CREATE TABLE feature.train_item_l14_inter AS SELECT item_id, IFNULL(l14_inter, 0) AS item_l14_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(event_date) / 14) AS l14_inter
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 15 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l14_inter;
CREATE TABLE feature.pre_item_l14_inter AS SELECT item_id, IFNULL(l14_inter, 0) AS item_l14_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(event_date) / 14) AS l14_inter
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 16 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 最后28天交互量
drop table if exists feature.train_item_l28_inter;
CREATE TABLE feature.train_item_l28_inter AS SELECT item_id, IFNULL(l28_inter, 0) AS item_l28_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(event_date) / 28) AS l28_inter
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 1 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l28_inter;
CREATE TABLE feature.pre_item_l28_inter AS SELECT item_id, IFNULL(l28_inter, 0) AS item_l28_inter FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b, (COUNT(event_date) / 28) AS l28_inter
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 2 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# ===============================================================================================
# 商品在最近1天被多少不同用户购买
drop table if exists feature.train_item_l1_buy_user;
CREATE TABLE feature.train_item_l1_buy_user AS SELECT item_id, IFNULL(l1_buy_user, 0) AS item_l1_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l1_buy_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date = 30 AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l1_buy_user;
CREATE TABLE feature.pre_item_l1_buy_user AS SELECT item_id, IFNULL(l1_buy_user, 0) AS item_l1_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l1_buy_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date = 31 AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近3天被多少不同用户购买
drop table if exists feature.train_item_l3_buy_user;
CREATE TABLE feature.train_item_l3_buy_user AS SELECT item_id, IFNULL(l3_buy_user, 0) AS item_l3_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l3_buy_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 28 AND 30
            AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l3_buy_user;
CREATE TABLE feature.pre_item_l3_buy_user AS SELECT item_id, IFNULL(l3_buy_user, 0) AS item_l3_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l3_buy_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 29 AND 31
            AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近7天被多少不同用户购买
drop table if exists feature.train_item_l7_buy_user;
CREATE TABLE feature.train_item_l7_buy_user AS SELECT item_id, IFNULL(l7_buy_user, 0) AS item_l7_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l7_buy_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 22 AND 30
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l7_buy_user;
CREATE TABLE feature.pre_item_l7_buy_user AS SELECT item_id, IFNULL(l7_buy_user, 0) AS item_l7_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l7_buy_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 23 AND 31
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近14天被多少不同用户购买
drop table if exists feature.train_item_l14_buy_user;
CREATE TABLE feature.train_item_l14_buy_user AS SELECT item_id, IFNULL(l14_buy_user, 0) AS item_l14_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l14_buy_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 15 AND 30
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l14_buy_user;
CREATE TABLE feature.pre_item_l14_buy_user AS SELECT item_id, IFNULL(l14_buy_user, 0) AS item_l14_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l14_buy_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 16 AND 31
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近28天被多少不同用户购买
drop table if exists feature.train_item_l28_buy_user;
CREATE TABLE feature.train_item_l28_buy_user AS SELECT item_id, IFNULL(l28_buy_user, 0) AS item_l28_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l28_buy_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 1 AND 30
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l28_buy_user;
CREATE TABLE feature.pre_item_l28_buy_user AS SELECT item_id, IFNULL(l28_buy_user, 0) AS item_l28_buy_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l28_buy_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 2 AND 31
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# ===============================================================================================
# 商品在最近1天被多少不同用户点击
drop table if exists feature.train_item_l1_click_user;
CREATE TABLE feature.train_item_l1_click_user AS SELECT item_id, IFNULL(l1_click_user, 0) AS item_l1_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l1_click_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date = 30 AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l1_click_user;
CREATE TABLE feature.pre_item_l1_click_user AS SELECT item_id, IFNULL(l1_click_user, 0) AS item_l1_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l1_click_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date = 31 AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近3天被多少不同用户点击
drop table if exists feature.train_item_l3_click_user;
CREATE TABLE feature.train_item_l3_click_user AS SELECT item_id, IFNULL(l3_click_user, 0) AS item_l3_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l3_click_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 28 AND 30
            AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l3_click_user;
CREATE TABLE feature.pre_item_l3_click_user AS SELECT item_id, IFNULL(l3_click_user, 0) AS item_l3_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l3_click_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 29 AND 31
            AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近7天被多少不同用户点击
drop table if exists feature.train_item_l7_click_user;
CREATE TABLE feature.train_item_l7_click_user AS SELECT item_id, IFNULL(l7_click_user, 0) AS item_l7_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l7_click_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 22 AND 30
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l7_click_user;
CREATE TABLE feature.pre_item_l7_click_user AS SELECT item_id, IFNULL(l7_click_user, 0) AS item_l7_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l7_click_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 23 AND 31
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近14天被多少不同用户点击
drop table if exists feature.train_item_l14_click_user;
CREATE TABLE feature.train_item_l14_click_user AS SELECT item_id, IFNULL(l14_click_user, 0) AS item_l14_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l14_click_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 15 AND 30
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l14_click_user;
CREATE TABLE feature.pre_item_l14_click_user AS SELECT item_id, IFNULL(l14_click_user, 0) AS item_l14_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l14_click_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 16 AND 31
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近28天被多少不同用户点击
drop table if exists feature.train_item_l28_click_user;
CREATE TABLE feature.train_item_l28_click_user AS SELECT item_id, IFNULL(l28_click_user, 0) AS item_l28_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l28_click_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 1 AND 30
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l28_click_user;
CREATE TABLE feature.pre_item_l28_click_user AS SELECT item_id, IFNULL(l28_click_user, 0) AS item_l28_click_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l28_click_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 2 AND 31
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 1
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# ===============================================================================================
# 商品在最近1天被多少不同用户交互
drop table if exists feature.train_item_l1_inter_user;
CREATE TABLE feature.train_item_l1_inter_user AS SELECT item_id, IFNULL(l1_inter_user, 0) AS item_l1_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l1_inter_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date = 30
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l1_inter_user;
CREATE TABLE feature.pre_item_l1_inter_user AS SELECT item_id, IFNULL(l1_inter_user, 0) AS item_l1_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l1_inter_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date = 31
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近3天被多少不同用户交互
drop table if exists feature.train_item_l3_inter_user;
CREATE TABLE feature.train_item_l3_inter_user AS SELECT item_id, IFNULL(l3_inter_user, 0) AS item_l3_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l3_inter_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 28 AND 30
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l3_inter_user;
CREATE TABLE feature.pre_item_l3_inter_user AS SELECT item_id, IFNULL(l3_inter_user, 0) AS item_l3_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l3_inter_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 29 AND 31
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近7天被多少不同用户交互
drop table if exists feature.train_item_l7_inter_user;
CREATE TABLE feature.train_item_l7_inter_user AS SELECT item_id, IFNULL(l7_inter_user, 0) AS item_l7_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l7_inter_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 22 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l7_inter_user;
CREATE TABLE feature.pre_item_l7_inter_user AS SELECT item_id, IFNULL(l7_inter_user, 0) AS item_l7_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l7_inter_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 23 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近14天被多少不同用户交互
drop table if exists feature.train_item_l14_inter_user;
CREATE TABLE feature.train_item_l14_inter_user AS SELECT item_id, IFNULL(l14_inter_user, 0) AS item_l14_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l14_inter_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 15 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l14_inter_user;
CREATE TABLE feature.pre_item_l14_inter_user AS SELECT item_id, IFNULL(l14_inter_user, 0) AS item_l14_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l14_inter_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 16 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# 商品在最近28天被多少不同用户交互
drop table if exists feature.train_item_l28_inter_user;
CREATE TABLE feature.train_item_l28_inter_user AS SELECT item_id, IFNULL(l28_inter_user, 0) AS item_l28_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l28_inter_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 1 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
drop table if exists feature.pre_item_l28_inter_user;
CREATE TABLE feature.pre_item_l28_inter_user AS SELECT item_id, IFNULL(l28_inter_user, 0) AS item_l28_inter_user FROM
    washed.item_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_id AS item_id_b,
            COUNT(DISTINCT (user_id)) AS l28_inter_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 2 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY item_id) AS b ON a.item_id = b.item_id_b;
    
# ===============================================================================================
# 商品在最近1天人均购买量
drop table if exists feature.train_item_l1_ave_buy;
CREATE TABLE feature.train_item_l1_ave_buy AS 
SELECT item_id, ifnull(item_l1_buy/item_l1_buy_user,0) as item_l1_ave_buy FROM
    (select item_id,item_l1_buy from feature.train_item_l1_buy) a
        JOIN
    (select item_id as item_id_1,item_l1_buy_user from feature.train_item_l1_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l1_ave_buy;
CREATE TABLE feature.pre_item_l1_ave_buy AS 
SELECT item_id, ifnull(item_l1_buy/item_l1_buy_user,0) as item_l1_ave_buy FROM
    (select item_id,item_l1_buy from feature.pre_item_l1_buy) a
        JOIN
    (select item_id as item_id_1,item_l1_buy_user from feature.pre_item_l1_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近3天人均购买量
drop table if exists feature.train_item_l3_ave_buy;
CREATE TABLE feature.train_item_l3_ave_buy AS 
SELECT item_id, ifnull(item_l3_buy/item_l3_buy_user,0) as item_l3_ave_buy FROM
    (select item_id,item_l3_buy from feature.train_item_l3_buy) a
        JOIN
    (select item_id as item_id_1,item_l3_buy_user from feature.train_item_l3_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l3_ave_buy;
CREATE TABLE feature.pre_item_l3_ave_buy AS 
SELECT item_id, ifnull(item_l3_buy/item_l3_buy_user,0) as item_l3_ave_buy FROM
    (select item_id,item_l3_buy from feature.pre_item_l3_buy) a
        JOIN
    (select item_id as item_id_1,item_l3_buy_user from feature.pre_item_l3_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近7天人均购买量
drop table if exists feature.train_item_l7_ave_buy;
CREATE TABLE feature.train_item_l7_ave_buy AS 
SELECT item_id, ifnull(item_l7_buy/item_l7_buy_user,0) as item_l7_ave_buy FROM
    (select item_id,item_l7_buy from feature.train_item_l7_buy) a
        JOIN
    (select item_id as item_id_1,item_l7_buy_user from feature.train_item_l7_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l7_ave_buy;
CREATE TABLE feature.pre_item_l7_ave_buy AS 
SELECT item_id, ifnull(item_l7_buy/item_l7_buy_user,0) as item_l7_ave_buy FROM
    (select item_id,item_l7_buy from feature.pre_item_l7_buy) a
        JOIN
    (select item_id as item_id_1,item_l7_buy_user from feature.pre_item_l7_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近14天人均购买量
drop table if exists feature.train_item_l14_ave_buy;
CREATE TABLE feature.train_item_l14_ave_buy AS 
SELECT item_id, ifnull(item_l14_buy/item_l14_buy_user,0) as item_l14_ave_buy FROM
    (select item_id,item_l14_buy from feature.train_item_l14_buy) a
        JOIN
    (select item_id as item_id_1,item_l14_buy_user from feature.train_item_l14_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l14_ave_buy;
CREATE TABLE feature.pre_item_l14_ave_buy AS 
SELECT item_id, ifnull(item_l14_buy/item_l14_buy_user,0) as item_l14_ave_buy FROM
    (select item_id,item_l14_buy from feature.pre_item_l14_buy) a
    JOIN
    (select item_id as item_id_1,item_l14_buy_user from feature.pre_item_l14_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近28天人均购买量
drop table if exists feature.train_item_l28_ave_buy;
CREATE TABLE feature.train_item_l28_ave_buy AS 
SELECT item_id, ifnull(item_l28_buy/item_l28_buy_user,0) as item_l28_ave_buy FROM
    (select item_id,item_l28_buy from feature.train_item_l28_buy) a
        JOIN
    (select item_id as item_id_1,item_l28_buy_user from feature.train_item_l28_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l28_ave_buy;
CREATE TABLE feature.pre_item_l28_ave_buy AS 
SELECT item_id, ifnull(item_l28_buy/item_l28_buy_user,0) as item_l28_ave_buy FROM
    (select item_id,item_l28_buy from feature.pre_item_l28_buy) a
        JOIN
    (select item_id as item_id_1,item_l28_buy_user from feature.pre_item_l28_buy_user)b 
    ON (a.item_id = b.item_id_1);
    
# ===============================================================================================
# 商品在最近1天人均交互量
drop table if exists feature.train_item_l1_ave_inter;
CREATE TABLE feature.train_item_l1_ave_inter AS 
SELECT item_id, ifnull(item_l1_inter/item_l1_inter_user,0) as item_l1_ave_inter FROM
    (select item_id,item_l1_inter from feature.train_item_l1_inter) a
        JOIN
    (select item_id as item_id_1,item_l1_inter_user from feature.train_item_l1_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l1_ave_inter;
CREATE TABLE feature.pre_item_l1_ave_inter AS 
SELECT item_id, ifnull(item_l1_inter/item_l1_inter_user,0) as item_l1_ave_inter FROM
    (select item_id,item_l1_inter from feature.pre_item_l1_inter) a
        JOIN
    (select item_id as item_id_1,item_l1_inter_user from feature.pre_item_l1_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近3天人均交互量
drop table if exists feature.train_item_l3_ave_inter;
CREATE TABLE feature.train_item_l3_ave_inter AS 
SELECT item_id, ifnull(item_l3_inter/item_l3_inter_user,0) as item_l3_ave_inter FROM
    (select item_id,item_l3_inter from feature.train_item_l3_inter) a
        JOIN
    (select item_id as item_id_1,item_l3_inter_user from feature.train_item_l3_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l3_ave_inter;
CREATE TABLE feature.pre_item_l3_ave_inter AS 
SELECT item_id, ifnull(item_l3_inter/item_l3_inter_user,0) as item_l3_ave_inter FROM
    (select item_id,item_l3_inter from feature.pre_item_l3_inter) a
        JOIN
    (select item_id as item_id_1,item_l3_inter_user from feature.pre_item_l3_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近7天人均交互量
drop table if exists feature.train_item_l7_ave_inter;
CREATE TABLE feature.train_item_l7_ave_inter AS 
SELECT item_id, ifnull(item_l7_inter/item_l7_inter_user,0) as item_l7_ave_inter FROM
    (select item_id,item_l7_inter from feature.train_item_l7_inter) a
        JOIN
    (select item_id as item_id_1,item_l7_inter_user from feature.train_item_l7_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l7_ave_inter;
CREATE TABLE feature.pre_item_l7_ave_inter AS 
SELECT item_id, ifnull(item_l7_inter/item_l7_inter_user,0) as item_l7_ave_inter FROM
    (select item_id,item_l7_inter from feature.pre_item_l7_inter) a
        JOIN
    (select item_id as item_id_1,item_l7_inter_user from feature.pre_item_l7_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近14天人均交互量
drop table if exists feature.train_item_l14_ave_inter;
CREATE TABLE feature.train_item_l14_ave_inter AS 
SELECT item_id, ifnull(item_l14_inter/item_l14_inter_user,0) as item_l14_ave_inter FROM
    (select item_id,item_l14_inter from feature.train_item_l14_inter) a
        JOIN
    (select item_id as item_id_1,item_l14_inter_user from feature.train_item_l14_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l14_ave_inter;
CREATE TABLE feature.pre_item_l14_ave_inter AS 
SELECT item_id, ifnull(item_l14_inter/item_l14_inter_user,0) as item_l14_ave_inter FROM
    (select item_id,item_l14_inter from feature.pre_item_l14_inter) a
    JOIN
    (select item_id as item_id_1,item_l14_inter_user from feature.pre_item_l14_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近28天人均交互量
drop table if exists feature.train_item_l28_ave_inter;
CREATE TABLE feature.train_item_l28_ave_inter AS 
SELECT item_id, ifnull(item_l28_inter/item_l28_inter_user,0) as item_l28_ave_inter FROM
    (select item_id,item_l28_inter from feature.train_item_l28_inter) a
        JOIN
    (select item_id as item_id_1,item_l28_inter_user from feature.train_item_l28_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l28_ave_inter;
CREATE TABLE feature.pre_item_l28_ave_inter AS 
SELECT item_id, ifnull(item_l28_inter/item_l28_inter_user,0) as item_l28_ave_inter FROM
    (select item_id,item_l28_inter from feature.pre_item_l28_inter) a
        JOIN
    (select item_id as item_id_1,item_l28_inter_user from feature.pre_item_l28_inter_user)b 
    ON (a.item_id = b.item_id_1);
    
# ===============================================================================================
# 商品在最近1天人均点击量
drop table if exists feature.train_item_l1_ave_click;
CREATE TABLE feature.train_item_l1_ave_click AS 
SELECT item_id, ifnull(item_l1_click/item_l1_click_user,0) as item_l1_ave_click FROM
    (select item_id,item_l1_click from feature.train_item_l1_click) a
        JOIN
    (select item_id as item_id_1,item_l1_click_user from feature.train_item_l1_click_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l1_ave_click;
CREATE TABLE feature.pre_item_l1_ave_click AS 
SELECT item_id, ifnull(item_l1_click/item_l1_click_user,0) as item_l1_ave_click FROM
    (select item_id,item_l1_click from feature.pre_item_l1_click) a
        JOIN
    (select item_id as item_id_1,item_l1_click_user from feature.pre_item_l1_click_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近3天人均点击量
drop table if exists feature.train_item_l3_ave_click;
CREATE TABLE feature.train_item_l3_ave_click AS 
SELECT item_id, ifnull(item_l3_click/item_l3_click_user,0) as item_l3_ave_click FROM
    (select item_id,item_l3_click from feature.train_item_l3_click) a
        JOIN
    (select item_id as item_id_1,item_l3_click_user from feature.train_item_l3_click_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l3_ave_click;
CREATE TABLE feature.pre_item_l3_ave_click AS 
SELECT item_id, ifnull(item_l3_click/item_l3_click_user,0) as item_l3_ave_click FROM
    (select item_id,item_l3_click from feature.pre_item_l3_click) a
        JOIN
    (select item_id as item_id_1,item_l3_click_user from feature.pre_item_l3_click_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近7天人均点击量
drop table if exists feature.train_item_l7_ave_click;
CREATE TABLE feature.train_item_l7_ave_click AS 
SELECT item_id, ifnull(item_l7_click/item_l7_click_user,0) as item_l7_ave_click FROM
    (select item_id,item_l7_click from feature.train_item_l7_click) a
        JOIN
    (select item_id as item_id_1,item_l7_click_user from feature.train_item_l7_click_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l7_ave_click;
CREATE TABLE feature.pre_item_l7_ave_click AS 
SELECT item_id, ifnull(item_l7_click/item_l7_click_user,0) as item_l7_ave_click FROM
    (select item_id,item_l7_click from feature.pre_item_l7_click) a
        JOIN
    (select item_id as item_id_1,item_l7_click_user from feature.pre_item_l7_click_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近14天人均点击量
drop table if exists feature.train_item_l14_ave_click;
CREATE TABLE feature.train_item_l14_ave_click AS 
SELECT item_id, ifnull(item_l14_click/item_l14_click_user,0) as item_l14_ave_click FROM
    (select item_id,item_l14_click from feature.train_item_l14_click) a
        JOIN
    (select item_id as item_id_1,item_l14_click_user from feature.train_item_l14_click_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l14_ave_click;
CREATE TABLE feature.pre_item_l14_ave_click AS 
SELECT item_id, ifnull(item_l14_click/item_l14_click_user,0) as item_l14_ave_click FROM
    (select item_id,item_l14_click from feature.pre_item_l14_click) a
    JOIN
    (select item_id as item_id_1,item_l14_click_user from feature.pre_item_l14_click_user)b 
    ON (a.item_id = b.item_id_1);
    
# 商品在最近28天人均点击量
drop table if exists feature.train_item_l28_ave_click;
CREATE TABLE feature.train_item_l28_ave_click AS 
SELECT item_id, ifnull(item_l28_click/item_l28_click_user,0) as item_l28_ave_click FROM
    (select item_id,item_l28_click from feature.train_item_l28_click) a
        JOIN
    (select item_id as item_id_1,item_l28_click_user from feature.train_item_l28_click_user)b 
    ON (a.item_id = b.item_id_1);
    
drop table if exists feature.pre_item_l28_ave_click;
CREATE TABLE feature.pre_item_l28_ave_click AS 
SELECT item_id, ifnull(item_l28_click/item_l28_click_user,0) as item_l28_ave_click FROM
    (select item_id,item_l28_click from feature.pre_item_l28_click) a
        JOIN
    (select item_id as item_id_1,item_l28_click_user from feature.pre_item_l28_click_user)b 
    ON (a.item_id = b.item_id_1);