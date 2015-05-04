drop table if exists newresult.train_ifbuy;
create table newresult.train_ifbuy as
select * from(
select user_id,item_id,ifnull(ifbuy,0) as ifbuy from
(select distinct user_id,item_id 
	from washed.tianchi_p_ten_11_30)a
left outer join
(select user_id as user_id_1,item_id as item_id_1, 1 as ifbuy 
	from washed.tianchi_p_31_buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c;


drop table if exists newresult.train_ui_user_item_p;
create table newresult.train_ui_user_item_p as
select a.* from
(select * 
	from newresult.train_ui_user_item)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ifbuy
	from newresult.train_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ifbuy=1;


drop table if exists newresult.train_ui_user_item_n;
create table newresult.train_ui_user_item_n as
select a.* from
(select * 
	from newresult.train_ui_user_item)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ifbuy
	from newresult.train_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ifbuy=0;


drop table if exists newresult.train_ui_user_item_p4;
create table newresult.train_ui_user_item_p4 as
select * from newresult.train_ui_user_item_p 
union all
select * from newresult.train_ui_user_item_p
union all
select * from newresult.train_ui_user_item_p 
union all
select * from newresult.train_ui_user_item_p;


drop table if exists newresult.train_ui_user_item_p16;
create table newresult.train_ui_user_item_p16 as
select * from newresult.train_ui_user_item_p4 
union all
select * from newresult.train_ui_user_item_p4
union all
select * from newresult.train_ui_user_item_p4 
union all
select * from newresult.train_ui_user_item_p4;


drop table if exists newresult.train_ui_user_item_p32;
create table newresult.train_ui_user_item_p32 as
select * from newresult.train_ui_user_item_p16 
union all
select * from newresult.train_ui_user_item_p16;








drop table if exists newresult.train_110_n44160;
create table newresult.train_110_n44160 as 
	select * from newresult.train_ui_user_item_n order by rand() limit 44160;


drop table if exists newresult.train_110_1_10;
create table newresult.train_110_1_10 as 
	select * from newresult.train_ui_user_item_p32 
	union all
	select * from newresult.train_110_n44160;



drop table if exists newresult.train_ifbuy_p;
create table newresult.train_ifbuy_p as
	select ifbuy from newresult.train_ifbuy where ifbuy=1;

drop table if exists newresult.train_ifbuy_n;
create table newresult.train_ifbuy_n as
	select ifbuy from newresult.train_ifbuy where ifbuy=0;


drop table if exists newresult.train_ifbuy_p4;
create table newresult.train_ifbuy_p4 as
select * from newresult.train_ifbuy_p 
union all
select * from newresult.train_ifbuy_p
union all
select * from newresult.train_ifbuy_p 
union all
select * from newresult.train_ifbuy_p;


drop table if exists newresult.train_ifbuy_p16;
create table newresult.train_ifbuy_p16 as
select * from newresult.train_ifbuy_p4 
union all
select * from newresult.train_ifbuy_p4
union all
select * from newresult.train_ifbuy_p4 
union all
select * from newresult.train_ifbuy_p4;


drop table if exists newresult.train_ifbuy_p32;
create table newresult.train_ifbuy_p32 as
select * from newresult.train_ifbuy_p16 
union all
select * from newresult.train_ifbuy_p16;



drop table if exists newresult.train_ifbuy_n44160;
create table newresult.train_ifbuy_n44160 as 
	select * from newresult.train_ifbuy_n limit 44160;



drop table if exists newresult.train_ifbuy_1_10;
create table newresult.train_ifbuy_1_10 as 
	select * from newresult.train_ifbuy_p32 
	union all
	select * from newresult.train_ifbuy_n44160;









