drop table if exists result.train_ifbuy;
create table result.train_ifbuy as
select * from(
select user_id,item_id,ifnull(ifbuy,0) as ifbuy from
(select distinct user_id,item_id 
	from washed.tianchi_p_ten_buy_1_30)a
left outer join
(select user_id as user_id_1,item_id as item_id_1, 1 as ifbuy 
	from washed.tianchi_p_31_buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c;



drop table if exists result.train_ui_user_item_category_p;
create table result.train_ui_user_item_category_p as
select a.* from
(select * 
	from result.train_ui_user_item_category)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ifbuy
	from result.train_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ifbuy=1;

116*300


drop table if exists result.train_ui_user_item_category_n;
create table result.train_ui_user_item_category_n as
select a.* from
(select * 
	from result.train_ui_user_item_category)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ifbuy
	from result.train_ifbuy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
where ifbuy=0;

drop table if exists result.train_ui_user_item_category_p4;
create table result.train_ui_user_item_category_p4 as
(select * from result.train_ui_user_item_category_p 
union all
select * from result.train_ui_user_item_category_p)a
union all
(select * from result.train_ui_user_item_category_p 
union all
select * from result.train_ui_user_item_category_p)b;



drop table if exists validate.score;
create table validate.score as
select common,recallnum,predictnum,
100*common/predictnum as predict_score,
100*common/recallnum as recall_score,
100*2*common/(predictnum+recallnum) as F1 from
(select common,recallnum,predictnum from
(select common,recallnum,flag from	
(	
select count(*) as common, 1 as flag from
(select * from validate.prediction)a
join
(select user_id as user_id_1,item_id as item_id_1 from washed.tianchi_p_31_buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1)
)c
join
(select count(*) as recallnum, 1 as flag_1 from washed.tianchi_p_31_buy)d
on(c.flag=d.flag_1)
)e
join
(select count(*) as predictnum, 1 as flag_1 from validate.prediction)f
on(e.flag=f.flag_1)
)g;



drop table if exists result.train_ifbuy_n27840;
create table result.train_ifbuy_n27840 as 
	select * from result.train_ifbuy_n limit 27840;


drop table if exists result.train_103_n27840;
create table result.train_103_n27840 as 
	select * from result.train_ui_user_item_category_n order by rand() limit 27840;


drop table if exists result.train_103_1_15;
create table result.train_103_1_15 as 
	select * from result.train_ui_user_item_category_p16 
	union all
	select * from result.train_103_n27840;



drop table if exists result.train_103_1_20;
create table result.train_103_1_20 as 
	select * from result.train_ui_user_item_category_p16 
	union all
	select * from result.train_103_n37120;






