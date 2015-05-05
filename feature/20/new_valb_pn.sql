drop table if exists newresult.valb_ifbuy;
create table newresult.valb_ifbuy as
select * from(
select user_id,item_id,ifnull(ifbuy,0) as ifbuy from
(select distinct user_id,item_id 
	from washed.tianchi_p_ten_9_28)a
left outer join
(select user_id as user_id_1,item_id as item_id_1, 1 as ifbuy 
	from washed.tianchi_p_29_buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c;


drop table if exists newresult.valb_ui_user_item_p;
create table newresult.valb_ui_user_item_p as
select a.* from
(select * 
	from newresult.valb_ui_user_item)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ifbuy
	from newresult.valb_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ifbuy=1;


drop table if exists newresult.valb_ui_user_item_n;
create table newresult.valb_ui_user_item_n as
select a.* from
(select * 
	from newresult.valb_ui_user_item)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ifbuy
	from newresult.valb_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ifbuy=0;


drop table if exists newresult.valb_ui_user_item_p4;
create table newresult.valb_ui_user_item_p4 as
select * from newresult.valb_ui_user_item_p 
union all
select * from newresult.valb_ui_user_item_p
union all
select * from newresult.valb_ui_user_item_p 
union all
select * from newresult.valb_ui_user_item_p;


drop table if exists newresult.valb_ui_user_item_p16;
create table newresult.valb_ui_user_item_p16 as
select * from newresult.valb_ui_user_item_p4 
union all
select * from newresult.valb_ui_user_item_p4
union all
select * from newresult.valb_ui_user_item_p4 
union all
select * from newresult.valb_ui_user_item_p4;


drop table if exists newresult.valb_ui_user_item_p32;
create table newresult.valb_ui_user_item_p32 as
select * from newresult.valb_ui_user_item_p16 
union all
select * from newresult.valb_ui_user_item_p16;








drop table if exists newresult.valb_110_n49280;
create table newresult.valb_110_n49280 as 
	select * from newresult.valb_ui_user_item_n order by rand() limit 49280;


drop table if exists newresult.valb_110_1_10;
create table newresult.valb_110_1_10 as 
	select * from newresult.valb_ui_user_item_p32 
	union all
	select * from newresult.valb_110_n49280;



drop table if exists newresult.valb_ifbuy_p;
create table newresult.valb_ifbuy_p as
	select ifbuy from newresult.valb_ifbuy where ifbuy=1;

drop table if exists newresult.valb_ifbuy_n;
create table newresult.valb_ifbuy_n as
	select ifbuy from newresult.valb_ifbuy where ifbuy=0;


drop table if exists newresult.valb_ifbuy_p4;
create table newresult.valb_ifbuy_p4 as
select * from newresult.valb_ifbuy_p 
union all
select * from newresult.valb_ifbuy_p
union all
select * from newresult.valb_ifbuy_p 
union all
select * from newresult.valb_ifbuy_p;


drop table if exists newresult.valb_ifbuy_p16;
create table newresult.valb_ifbuy_p16 as
select * from newresult.valb_ifbuy_p4 
union all
select * from newresult.valb_ifbuy_p4
union all
select * from newresult.valb_ifbuy_p4 
union all
select * from newresult.valb_ifbuy_p4;


drop table if exists newresult.valb_ifbuy_p32;
create table newresult.valb_ifbuy_p32 as
select * from newresult.valb_ifbuy_p16 
union all
select * from newresult.valb_ifbuy_p16;



drop table if exists newresult.valb_ifbuy_n49280;
create table newresult.valb_ifbuy_n49280 as 
	select * from newresult.valb_ifbuy_n limit 49280;



drop table if exists newresult.valb_ifbuy_1_10;
create table newresult.valb_ifbuy_1_10 as 
	select * from newresult.valb_ifbuy_p32 
	union all
	select * from newresult.valb_ifbuy_n49280;









