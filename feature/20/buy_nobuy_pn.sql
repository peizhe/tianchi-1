drop table if exists newresult.train_ui_user_item_buy_p;
create table newresult.train_ui_user_item_buy_p as
select a.* from
(select * 
	from newresult.train_ui_user_item_buy)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ifbuy
	from newresult.train_ifbuy_buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ifbuy=1;


drop table if exists newresult.train_ui_user_item_buy_n;
create table newresult.train_ui_user_item_buy_n as
select a.* from
(select * 
	from newresult.train_ui_user_item_buy)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ifbuy
	from newresult.train_ifbuy_buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ifbuy=0;


drop table if exists newresult.train_ui_user_item_buy_p4;
create table newresult.train_ui_user_item_buy_p4 as
select * from newresult.train_ui_user_item_buy_p 
union all
select * from newresult.train_ui_user_item_buy_p
union all
select * from newresult.train_ui_user_item_buy_p 
union all
select * from newresult.train_ui_user_item_buy_p;


drop table if exists newresult.train_ui_user_item_buy_p16;
create table newresult.train_ui_user_item_buy_p16 as
select * from newresult.train_ui_user_item_buy_p4 
union all
select * from newresult.train_ui_user_item_buy_p4
union all
select * from newresult.train_ui_user_item_buy_p4 
union all
select * from newresult.train_ui_user_item_buy_p4;


drop table if exists newresult.train_ui_user_item_buy_p32;
create table newresult.train_ui_user_item_buy_p32 as
select * from newresult.train_ui_user_item_buy_p16 
union all
select * from newresult.train_ui_user_item_buy_p16;








drop table if exists newresult.train_110_buy_n1600;
create table newresult.train_110_buy_n1600 as 
	select * from newresult.train_ui_user_item_buy_n order by rand() limit 1600;


drop table if exists newresult.train_110_buy_1_5;
create table newresult.train_110_buy_1_5 as 
	select * from newresult.train_ui_user_item_buy_p32 
	union all
	select * from newresult.train_110_buy_n1600;



drop table if exists newresult.train_ifbuy_buy_p;
create table newresult.train_ifbuy_buy_p as
	select ifbuy from newresult.train_ifbuy_buy where ifbuy=1;

drop table if exists newresult.train_ifbuy_buy_n;
create table newresult.train_ifbuy_buy_n as
	select ifbuy from newresult.train_ifbuy_buy where ifbuy=0;


drop table if exists newresult.train_ifbuy_buy_p4;
create table newresult.train_ifbuy_buy_p4 as
select * from newresult.train_ifbuy_buy_p 
union all
select * from newresult.train_ifbuy_buy_p
union all
select * from newresult.train_ifbuy_buy_p 
union all
select * from newresult.train_ifbuy_buy_p;


drop table if exists newresult.train_ifbuy_buy_p16;
create table newresult.train_ifbuy_buy_p16 as
select * from newresult.train_ifbuy_buy_p4 
union all
select * from newresult.train_ifbuy_buy_p4
union all
select * from newresult.train_ifbuy_buy_p4 
union all
select * from newresult.train_ifbuy_buy_p4;


drop table if exists newresult.train_ifbuy_buy_p32;
create table newresult.train_ifbuy_buy_p32 as
select * from newresult.train_ifbuy_buy_p16 
union all
select * from newresult.train_ifbuy_buy_p16;



drop table if exists newresult.train_ifbuy_buy_n1600;
create table newresult.train_ifbuy_buy_n1600 as 
	select * from newresult.train_ifbuy_buy_n limit 1600;



drop table if exists newresult.train_ifbuy_buy_1_5;
create table newresult.train_ifbuy_buy_1_5 as 
	select * from newresult.train_ifbuy_buy_p32 
	union all
	select * from newresult.train_ifbuy_buy_n1600;
