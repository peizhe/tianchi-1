drop table if exists new.train_ui_ifbuy;
create table new.train_ui_ifbuy as
    select user_id,item_id,ifnull(ui_ifbuy,0) as ui_ifbuy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30)a
    left outer join
    (select user_id as user_id_1,item_id as item_id_1, 1 as ui_ifbuy from
    (select distinct user_id,item_id from washed.tianchi_p_ten_11_30 
    where behavior_type=4)c)b
    on (a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


drop table if exists newresult.train_ui_user_item_buy;
create table newresult.train_ui_user_item_buy as
select a.* from
(select * from newresult.train_ui_user_item)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_ifbuy from new.train_ui_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ui_ifbuy=1;



drop table if exists newresult.train_ui_user_item_nobuy;
create table newresult.train_ui_user_item_nobuy as
select a.* from
(select * from newresult.train_ui_user_item)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_ifbuy from new.train_ui_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ui_ifbuy=0;


drop table if exists newresult.train_ifbuy_buy;
create table newresult.train_ifbuy_buy as
select a.* from
(select * from newresult.train_ifbuy)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_ifbuy from new.train_ui_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ui_ifbuy=1;



drop table if exists newresult.train_ifbuy_nobuy;
create table newresult.train_ifbuy_nobuy as
select a.* from
(select * from newresult.train_ifbuy)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_ifbuy from new.train_ui_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ui_ifbuy=0;




