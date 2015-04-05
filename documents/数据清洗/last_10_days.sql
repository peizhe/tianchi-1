use tianchi;
CREATE TABLE last_10_days_click AS SELECT user_id, COUNT(behavior_type) AS click_time FROM
    user
WHERE
    behavior_type = 1 AND event_date > 21
GROUP BY user_id;
CREATE TABLE last_10_days_favor AS SELECT user_id, COUNT(behavior_type) AS favor_time FROM
    user
WHERE
    behavior_type = 2 AND event_date > 21
GROUP BY user_id;
CREATE TABLE last_10_days_cart AS SELECT user_id, COUNT(behavior_type) AS cart_time FROM
    user
WHERE
    behavior_type = 3 AND event_date > 21
GROUP BY user_id;
CREATE TABLE last_10_days_buy AS SELECT user_id, COUNT(behavior_type) AS buy_time FROM
    user
WHERE
    behavior_type = 4 AND event_date > 21
GROUP BY user_id;

create table last_1 as
select distinct user_id from user;
create table last_2 as
select l1.user_id,c.click_time
from last_1 as l1
left join last_10_days_click as c
on l1.user_id=c.user_id;
create table last_3 as
select l2.user_id,l2.click_time,ca.cart_time
from last_2 as l2
left join last_10_days_cart as ca
on l2.user_id=ca.user_id;
create table last_4 as
select l3.*,f.favor_time
from last_3 as l3
left join last_10_days_favor as f
on l3.user_id=f.user_id;
create table last_10_info as
select l4.*,b.buy_time
from last_4 as l4
left join last_10_days_buy as b
on l4.user_id=b.user_id;

drop table last_1;
drop table last_2;
drop table last_3;
drop table last_4;
drop table last_10_days_buy;
drop table last_10_days_click;
drop table last_10_days_favor;
drop table last_10_days_cart;