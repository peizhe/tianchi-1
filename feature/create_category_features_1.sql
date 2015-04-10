# ======================================================
# 最后7天购买量
drop table if exists feature.train_category_l7_buy;
CREATE TABLE feature.train_category_l7_buy AS SELECT category_id, IFNULL(l7_buy, 0) AS category_l7_buy FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b, (COUNT(behavior_type) / 7) AS l7_buy
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 22 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
drop table if exists feature.pre_category_l7_buy;
CREATE TABLE feature.pre_category_l7_buy AS SELECT category_id, IFNULL(l7_buy, 0) AS category_l7_buy FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b, (COUNT(behavior_type) / 7) AS l7_buy
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 23 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
# 最后14天购买量
drop table if exists feature.train_category_l14_buy;
CREATE TABLE feature.train_category_l14_buy AS SELECT category_id, IFNULL(l14_buy, 0) AS category_l14_buy FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b, (COUNT(behavior_type) / 14) AS l14_buy
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 15 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
drop table if exists feature.pre_category_l14_buy;
CREATE TABLE feature.pre_category_l14_buy AS SELECT category_id, IFNULL(l14_buy, 0) AS category_l14_buy FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b, (COUNT(behavior_type) / 14) AS l14_buy
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 16 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
# 最后28天购买量
drop table if exists feature.train_category_l28_buy;
CREATE TABLE feature.train_category_l28_buy AS SELECT category_id, IFNULL(l28_buy, 0) AS category_l28_buy FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b, (COUNT(behavior_type) / 28) AS l28_buy
    FROM
        washed.tianchi_p_1_30
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 1 AND 30
            AND event_date NOT IN (24 , 25)
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
drop table if exists feature.pre_category_l28_buy;
CREATE TABLE feature.pre_category_l28_buy AS SELECT category_id, IFNULL(l28_buy, 0) AS category_l28_buy FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b, (COUNT(behavior_type) / 28) AS l28_buy
    FROM
        washed.tianchi_p_2_31
    WHERE
        behavior_type = 4
            AND event_date BETWEEN 2 AND 31
            AND event_date NOT IN (24 , 25)
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
# ===============================================================================================
    
# 商品在最近7天被多少不同用户购买
drop table if exists feature.train_category_l7_buy_user;
CREATE TABLE feature.train_category_l7_buy_user AS SELECT category_id, IFNULL(l7_buy_user, 0) AS category_l7_buy_user FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b,
            COUNT(DISTINCT (user_id)) AS l7_buy_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 22 AND 30
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
drop table if exists feature.pre_category_l7_buy_user;
CREATE TABLE feature.pre_category_l7_buy_user AS SELECT category_id, IFNULL(l7_buy_user, 0) AS category_l7_buy_user FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b,
            COUNT(DISTINCT (user_id)) AS l7_buy_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 23 AND 31
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
# 商品在最近14天被多少不同用户购买
drop table if exists feature.train_category_l14_buy_user;
CREATE TABLE feature.train_category_l14_buy_user AS SELECT category_id, IFNULL(l14_buy_user, 0) AS category_l14_buy_user FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b,
            COUNT(DISTINCT (user_id)) AS l14_buy_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 15 AND 30
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
drop table if exists feature.pre_category_l14_buy_user;
CREATE TABLE feature.pre_category_l14_buy_user AS SELECT category_id, IFNULL(l14_buy_user, 0) AS category_l14_buy_user FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b,
            COUNT(DISTINCT (user_id)) AS l14_buy_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 16 AND 31
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
# 商品在最近28天被多少不同用户购买
drop table if exists feature.train_category_l28_buy_user;
CREATE TABLE feature.train_category_l28_buy_user AS SELECT category_id, IFNULL(l28_buy_user, 0) AS category_l28_buy_user FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b,
            COUNT(DISTINCT (user_id)) AS l28_buy_user
    FROM
        washed.tianchi_p_1_30
    WHERE
        event_date BETWEEN 1 AND 30
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
drop table if exists feature.pre_category_l28_buy_user;
CREATE TABLE feature.pre_category_l28_buy_user AS SELECT category_id, IFNULL(l28_buy_user, 0) AS category_l28_buy_user FROM
    washed.category_subset AS a
        LEFT OUTER JOIN
    (SELECT 
        item_category AS category_id_b,
            COUNT(DISTINCT (user_id)) AS l28_buy_user
    FROM
        washed.tianchi_p_2_31
    WHERE
        event_date BETWEEN 2 AND 31
            AND event_date NOT IN (24 , 25)
            AND behavior_type = 4
    GROUP BY category_id_b) AS b ON a.category_id = b.category_id_b;
    
# ===============================================================================================
    
# 商品在最近7天人均购买量
drop table if exists feature.train_category_l7_ave_buy;
CREATE TABLE feature.train_category_l7_ave_buy AS 
SELECT category_id, ifnull(category_l7_buy/category_l7_buy_user,0) as category_l7_ave_buy FROM
    (select category_id,category_l7_buy from feature.train_category_l7_buy) a
        JOIN
    (select category_id as category_id_1,category_l7_buy_user from feature.train_category_l7_buy_user)b 
    ON (a.category_id = b.category_id_1);
    
drop table if exists feature.pre_category_l7_ave_buy;
CREATE TABLE feature.pre_category_l7_ave_buy AS 
SELECT category_id, ifnull(category_l7_buy/category_l7_buy_user,0) as category_l7_ave_buy FROM
    (select category_id,category_l7_buy from feature.pre_category_l7_buy) a
        JOIN
    (select category_id as category_id_1,category_l7_buy_user from feature.pre_category_l7_buy_user)b 
    ON (a.category_id = b.category_id_1);
    
# 商品在最近14天人均购买量
drop table if exists feature.train_category_l14_ave_buy;
CREATE TABLE feature.train_category_l14_ave_buy AS 
SELECT category_id, ifnull(category_l14_buy/category_l14_buy_user,0) as category_l14_ave_buy FROM
    (select category_id,category_l14_buy from feature.train_category_l14_buy) a
        JOIN
    (select category_id as category_id_1,category_l14_buy_user from feature.train_category_l14_buy_user)b 
    ON (a.category_id = b.category_id_1);
    
drop table if exists feature.pre_category_l14_ave_buy;
CREATE TABLE feature.pre_category_l14_ave_buy AS 
SELECT category_id, ifnull(category_l14_buy/category_l14_buy_user,0) as category_l14_ave_buy FROM
    (select category_id,category_l14_buy from feature.pre_category_l14_buy) a
    JOIN
    (select category_id as category_id_1,category_l14_buy_user from feature.pre_category_l14_buy_user)b 
    ON (a.category_id = b.category_id_1);
    
# 商品在最近28天人均购买量
drop table if exists feature.train_category_l28_ave_buy;
CREATE TABLE feature.train_category_l28_ave_buy AS 
SELECT category_id, ifnull(category_l28_buy/category_l28_buy_user,0) as category_l28_ave_buy FROM
    (select category_id,category_l28_buy from feature.train_category_l28_buy) a
        JOIN
    (select category_id as category_id_1,category_l28_buy_user from feature.train_category_l28_buy_user)b 
    ON (a.category_id = b.category_id_1);
    
drop table if exists feature.pre_category_l28_ave_buy;
CREATE TABLE feature.pre_category_l28_ave_buy AS 
SELECT category_id, ifnull(category_l28_buy/category_l28_buy_user,0) as category_l28_ave_buy FROM
    (select category_id,category_l28_buy from feature.pre_category_l28_buy) a
        JOIN
    (select category_id as category_id_1,category_l28_buy_user from feature.pre_category_l28_buy_user)b 
    ON (a.category_id = b.category_id_1);

drop table if exists feature.train_category_l7_buy_user;
drop table if exists feature.pre_category_l7_buy_user;
drop table if exists feature.train_category_l14_buy_user;
drop table if exists feature.pre_category_l14_buy_user;
drop table if exists feature.train_category_l28_buy_user;
drop table if exists feature.pre_category_l28_buy_user;