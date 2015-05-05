/**ui特征第一类：*/

/*第一小类*/
drop table if exists test.ui_1_1;
create table test.ui_1_1 as 
select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy from
(select user_id,item_id,ui_l1_buy,ui_l2_buy from
(select user_id,item_id,ui_l1_buy from new.pre_ui_l1_buy)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l2_buy from new.pre_ui_l2_buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l3_buy from new.pre_ui_l3_buy)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1))e
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l5_buy from new.pre_ui_l5_buy)f
on(e.user_id=f.user_id_1 and e.item_id=f.item_id_1))g
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l7_buy from new.pre_ui_l7_buy)h
on(g.user_id=h.user_id_1 and g.item_id=h.item_id_1);

/*第二小类*/
drop table if exists test.ui_1_2;
create table test.ui_1_2 as 
select user_id,item_id,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk from
(select user_id,item_id,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk from
(select user_id,item_id,ui_l1_clk,ui_l2_clk,ui_l3_clk from
(select user_id,item_id,ui_l1_clk,ui_l2_clk from
(select user_id,item_id,ui_l1_clk from new.pre_ui_l1_clk)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l2_clk from new.pre_ui_l2_clk)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l3_clk from new.pre_ui_l3_clk)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1))e
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l5_clk from new.pre_ui_l5_clk)f
on(e.user_id=f.user_id_1 and e.item_id=f.item_id_1))g
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l7_clk from new.pre_ui_l7_clk)h
on(g.user_id=h.user_id_1 and g.item_id=h.item_id_1);

/*第三小类*/

drop table if exists test.ui_1_3;
create table test.ui_1_3 as 
select user_id,item_id,ui_l10_buy,ui_l14_buy,ui_l18_buy from
(select user_id,item_id,ui_l10_buy,ui_l14_buy from
(select user_id,item_id,ui_l10_buy from new.pre_ui_l10_buy)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l14_buy from new.pre_ui_l14_buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l18_buy from new.pre_ui_l18_buy)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1);


/*第四小类*/


drop table if exists test.ui_1_4;
create table test.ui_1_4 as 
select user_id,item_id,ui_l10_clk,ui_l14_clk,ui_l18_clk from
(select user_id,item_id,ui_l10_clk,ui_l14_clk from
(select user_id,item_id,ui_l10_clk from new.pre_ui_l10_clk)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l14_clk from new.pre_ui_l14_clk)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l18_clk from new.pre_ui_l18_clk)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1);


/*第五小类*/

drop table if exists test.ui_1_5;
create table test.ui_1_5 as 
select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy from test.ui_1_1)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l10_buy,ui_l14_buy,ui_l18_buy from test.ui_1_3)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


/*第六小类*/

drop table if exists test.ui_1_6;
create table test.ui_1_6 as 
select user_id,item_id,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk from
(select user_id,item_id,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk from test.ui_1_2)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l10_clk,ui_l14_clk,ui_l18_clk from test.ui_1_4)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);





/*ui特征第一类*/

drop table if exists test.ui_1;
create table test.ui_1 as 
select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk
 from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy
 from test.ui_1_5)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk
 from test.ui_1_6)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);




/**ui特征第二类*/


/*ui特征第二类*/

drop table if exists test.ui_2;
create table test.ui_2 as 
select user_id,item_id,ui_d12_buy,ui_d12_clk from
(select user_id,item_id,ui_d12_buy from new.pre_ui_d12_buy)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_d12_clk from new.pre_ui_d12_clk)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);



/**ui特征第三类*/

drop table if exists test.ui_3;
create table test.ui_3 as 
select user_id,item_id,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date from
(select user_id,item_id,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy from
(select user_id,item_id,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor from
(select user_id,item_id,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk from
(select user_id,item_id,ui_l_buy_date,ui_fl_buy_distance from
(select user_id,item_id,ui_l_buy_date from new.pre_ui_l_buy_date)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_fl_buy_distance from new.pre_ui_fl_buy_distance)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_actday_clk from new.pre_ui_l_actday_clk)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1))e
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_actday_favor from new.pre_ui_l_actday_favor)f
on(e.user_id=f.user_id_1 and e.item_id=f.item_id_1))g
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_actday_buy from new.pre_ui_l_actday_buy)h
on(g.user_id=h.user_id_1 and g.item_id=h.item_id_1))i
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_act_date from new.pre_ui_l_act_date)j
on(i.user_id=j.user_id_1 and i.item_id=j.item_id_1);

/**ui特征第四类*/

/*第一小类*/

drop table if exists test.ui_4_1;
create table test.ui_4_1 as 
select user_id,item_id,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act from
(select user_id,item_id,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act from
(select user_id,item_id,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act from
(select user_id,item_id,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy from
(select user_id,item_id,ui_l3_h1_buy,ui_l5_h1_buy from
(select user_id,item_id,ui_l3_h1_buy from new.pre_ui_l3_h1_buy)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l5_h1_buy from new.pre_ui_l5_h1_buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l7_h1_buy from new.pre_ui_l7_h1_buy)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1))e
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l3_h1_act from new.pre_ui_l3_h1_act)f
on(e.user_id=f.user_id_1 and e.item_id=f.item_id_1))g
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l5_h1_act from new.pre_ui_l5_h1_act)h
on(g.user_id=h.user_id_1 and g.item_id=h.item_id_1))i
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l7_h1_act from new.pre_ui_l7_h1_act)j
on(i.user_id=j.user_id_1 and i.item_id=j.item_id_1);


/*第二小类*/

drop table if exists test.ui_4_2;
create table test.ui_4_2 as 
select user_id,item_id,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart from
(select user_id,item_id,ui_f_buybefore_clk,ui_f_buybefore_favor from
(select user_id,item_id,ui_f_buybefore_clk from new.pre_ui_f_buybefore_clk)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_f_buybefore_favor from new.pre_ui_f_buybefore_favor)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_f_buybefore_cart from new.pre_ui_f_buybefore_cart)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1);


/*ui特征第四类*/


drop table if exists test.ui_4;
create table test.ui_4 as 
select user_id,item_id,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart 
from
(select user_id,item_id,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act
 from test.ui_4_1)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart
 from test.ui_4_2)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


/**ui特征第五类*/

/*第一小类*/

drop table if exists test.ui_5_1;
create table test.ui_5_1 as 
select user_id,item_id,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy from
(select user_id,item_id,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy from
(select user_id,item_id,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy from
(select user_id,item_id,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy from
(select user_id,item_id,ui_l7_clk2buy,ui_l14_clk2buy from
(select user_id,item_id,ui_l7_clk2buy from new.pre_ui_l7_clk2buy)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l14_clk2buy from new.pre_ui_l14_clk2buy)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l3_clk2buy from new.pre_ui_l3_clk2buy)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1))e
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l7_later2buy from new.pre_ui_l7_later2buy)f
on(e.user_id=f.user_id_1 and e.item_id=f.item_id_1))g
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l14_later2buy from new.pre_ui_l14_later2buy)h
on(g.user_id=h.user_id_1 and g.item_id=h.item_id_1))i
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l3_later2buy from new.pre_ui_l3_later2buy)j
on(i.user_id=j.user_id_1 and i.item_id=j.item_id_1);



/*第二小类*/

drop table if exists test.ui_5_2;
create table test.ui_5_2 as 
select user_id,item_id,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance from
(select user_id,item_id,ui_l_act_distance from new.pre_ui_l_act_distance)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_min_distance,ui_avg_distance,ui_max_distance from new.pre_ui_distance)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


/*ui特征第五类*/


drop table if exists test.ui_5;
create table test.ui_5 as 
select user_id,item_id,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance
from
(select user_id,item_id,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy
 from test.ui_5_1)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance
 from test.ui_5_2)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);


/*ui特征第6-1类*/
drop table if exists test.ui_6_1;
create table test.ui_6_1 as 
select user_id,item_id,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor,ui_l14_favor from
(select user_id,item_id,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor from
(select user_id,item_id,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart from
(select user_id,item_id,ui_l1_cart,ui_l2_cart,ui_l3_cart from
(select user_id,item_id,ui_l1_cart,ui_l2_cart from
(select user_id,item_id,ui_l1_cart from new.pre_ui_l1_cart)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l2_cart from new.pre_ui_l2_cart)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l3_cart from new.pre_ui_l3_cart)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1))e
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l7_cart from new.pre_ui_l7_cart)f
on(e.user_id=f.user_id_1 and e.item_id=f.item_id_1))g
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l7_favor from new.pre_ui_l7_favor)h
on(g.user_id=h.user_id_1 and g.item_id=h.item_id_1))i
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l14_favor from new.pre_ui_l14_favor)j
on(i.user_id=j.user_id_1 and i.item_id=j.item_id_1);

/*ui特征第6-2类*/

drop table if exists test.ui_6_2;
create table test.ui_6_2 as 
select user_id,item_id,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act,ui_l3_h1_only_act from
(select user_id,item_id,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act from
(select user_id,item_id,ui_l18_favor,ui_l_actnextday_other_distinct_act from
(select user_id,item_id,ui_l18_favor from new.pre_ui_l18_favor)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_actnextday_other_distinct_act from new.pre_ui_l_actnextday_other_distinct_act)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l1_h1_only_act from new.pre_ui_l1_h1_only_act)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1))e
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l3_h1_only_act from new.pre_ui_l3_h1_only_act)f
on(e.user_id=f.user_id_1 and e.item_id=f.item_id_1);


/*ui特征第6类*/
drop table if exists test.ui_6;
create table test.ui_6 as 
select user_id,item_id,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor,ui_l14_favor,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act,ui_l3_h1_only_act
from
(select user_id,item_id,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor,ui_l14_favor
 from test.ui_6_1)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act,ui_l3_h1_only_act
 from test.ui_6_2)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1);



/*ui全部特征（53+2）*/

drop table if exists newresult.pre_ui;
create table newresult.pre_ui as 
select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk,ui_d12_buy,ui_d12_clk,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor,ui_l14_favor,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act,ui_l3_h1_only_act from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk,ui_d12_buy,ui_d12_clk,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk,ui_d12_buy,ui_d12_clk,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk,ui_d12_buy,ui_d12_clk,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk,ui_d12_buy,ui_d12_clk from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk from test.ui_1)a
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_d12_buy,ui_d12_clk from test.ui_2)b
on(a.user_id=b.user_id_1 and a.item_id=b.item_id_1))c
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date from test.ui_3)d
on(c.user_id=d.user_id_1 and c.item_id=d.item_id_1))e
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart from test.ui_4)f
on(e.user_id=f.user_id_1 and e.item_id=f.item_id_1))g
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance from test.ui_5)h
on(g.user_id=h.user_id_1 and g.item_id=h.item_id_1))i
left outer join
(select user_id as user_id_1,item_id as item_id_1,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor,ui_l14_favor,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act,ui_l3_h1_only_act from test.ui_6)j
on(i.user_id=j.user_id_1 and i.item_id=j.item_id_1);



/**user特征第一类*/


/*第一小类*/


drop table if exists test.user_1_1;
create table test.user_1_1 as 
select user_id,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk from
(select user_id,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk from
(select user_id,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk from
(select user_id,user_l1_clk,user_l3_clk,user_l7_clk from
(select user_id,user_l1_clk,user_l3_clk from
(select user_id,user_l1_clk from new.pre_user_l1_clk)a
left outer join
(select user_id as user_id_1,user_l3_clk from new.pre_user_l3_clk)b
on(a.user_id=b.user_id_1))c
left outer join
(select user_id as user_id_1,user_l7_clk from new.pre_user_l7_clk)d
on(c.user_id=d.user_id_1))e
left outer join
(select user_id as user_id_1,user_l10_clk from new.pre_user_l10_clk)f
on(e.user_id=f.user_id_1))g
left outer join
(select user_id as user_id_1,user_l14_clk from new.pre_user_l14_clk)h
on(g.user_id=h.user_id_1))i
left outer join
(select user_id as user_id_1,user_l18_clk from new.pre_user_l18_clk)j
on(i.user_id=j.user_id_1);


/*第二小类*/


drop table if exists test.user_1_2;
create table test.user_1_2 as 
select user_id,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy from
(select user_id,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy from
(select user_id,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy from
(select user_id,user_l1_buy,user_l3_buy,user_l7_buy from
(select user_id,user_l1_buy,user_l3_buy from
(select user_id,user_l1_buy from new.pre_user_l1_buy)a
left outer join
(select user_id as user_id_1,user_l3_buy from new.pre_user_l3_buy)b
on(a.user_id=b.user_id_1))c
left outer join
(select user_id as user_id_1,user_l7_buy from new.pre_user_l7_buy)d
on(c.user_id=d.user_id_1))e
left outer join
(select user_id as user_id_1,user_l10_buy from new.pre_user_l10_buy)f
on(e.user_id=f.user_id_1))g
left outer join
(select user_id as user_id_1,user_l14_buy from new.pre_user_l14_buy)h
on(g.user_id=h.user_id_1))i
left outer join
(select user_id as user_id_1,user_l18_buy from new.pre_user_l18_buy)j
on(i.user_id=j.user_id_1);


/*user特征第一类*/

drop table if exists test.user_1;
create table test.user_1 as 
select user_id,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy
 from
(select user_id,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk
 from test.user_1_1)a
left outer join
(select user_id as user_id_1,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy
 from test.user_1_2)b
on(a.user_id=b.user_id_1);



/**user特征第二类*/


drop table if exists test.user_2;
create table test.user_2 as 
select user_id,ifnull(user_d12_clk,0) as user_d12_clk,ifnull(user_d12_buy,0) as user_d12_buy,user_l_act_date,user_l_buy_date from
(select user_id,user_d12_clk,user_d12_buy,user_l_act_date from
(select user_id,user_d12_clk,user_d12_buy from
(select user_id,user_d12_clk from new.pre_user_d12_clk)a
left outer join
(select user_id as user_id_1,user_d12_buy from new.pre_user_d12_buy)b
on(a.user_id=b.user_id_1))c
left outer join
(select user_id as user_id_1,user_l_act_date from new.pre_user_l_act_date)d
on(c.user_id=d.user_id_1))e
left outer join
(select user_id as user_id_1,user_l_buy_date from new.pre_user_l_buy_date)f
on(e.user_id=f.user_id_1);


/**user特征第三类*/

/*第一小类*/

drop table if exists test.user_3_1;
create table test.user_3_1 as 
select user_id,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy from
(select user_id,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy from
(select user_id,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy from
(select user_id,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy from
(select user_id,user_l7_clk2buy,user_l14_clk2buy from
(select user_id,user_l7_clk2buy from new.pre_user_l7_clk2buy)a
left outer join
(select user_id as user_id_1,user_l14_clk2buy from new.pre_user_l14_clk2buy)b
on(a.user_id=b.user_id_1))c
left outer join
(select user_id as user_id_1,user_l3_clk2buy from new.pre_user_l3_clk2buy)d
on(c.user_id=d.user_id_1))e
left outer join
(select user_id as user_id_1,user_l7_later2buy from new.pre_user_l7_later2buy)f
on(e.user_id=f.user_id_1))g
left outer join
(select user_id as user_id_1,user_l14_later2buy from new.pre_user_l14_later2buy)h
on(g.user_id=h.user_id_1))i
left outer join
(select user_id as user_id_1,user_l3_later2buy from new.pre_user_l3_later2buy)j
on(i.user_id=j.user_id_1);


/*第二小类*/

drop table if exists test.user_3_2;
create table test.user_3_2 as 
select user_id,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy from
(select user_id,user_l7_h1_buy,user_l14_h1_buy from
(select user_id,user_l7_h1_buy from new.pre_user_l7_h1_buy)a
left outer join
(select user_id as user_id_1,user_l14_h1_buy from new.pre_user_l14_h1_buy)b
on(a.user_id=b.user_id_1))c
left outer join
(select user_id as user_id_1,user_l3_h1_buy from new.pre_user_l3_h1_buy)d
on(c.user_id=d.user_id_1);

/*user特征第三类*/

drop table if exists test.user_3;
create table test.user_3 as 
select user_id,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy
 from
(select user_id,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy
 from test.user_3_1)a
left outer join
(select user_id as user_id_1,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy
 from test.user_3_2)b
on(a.user_id=b.user_id_1);



/*user特征第4-1类*/
drop table if exists test.user_4_1;
create table test.user_4_1 as 
select user_id,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor,user_l14_favor from
(select user_id,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor from
(select user_id,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor from
(select user_id,user_l1_cart,user_l3_cart,user_l7_cart from
(select user_id,user_l1_cart,user_l3_cart from
(select user_id,user_l1_cart from new.pre_user_l1_cart)a
left outer join
(select user_id as user_id_1,user_l3_cart from new.pre_user_l3_cart)b
on(a.user_id=b.user_id_1))c
left outer join
(select user_id as user_id_1,user_l7_cart from new.pre_user_l7_cart)d
on(c.user_id=d.user_id_1))e
left outer join
(select user_id as user_id_1,user_l7_favor from new.pre_user_l7_favor)f
on(e.user_id=f.user_id_1))g
left outer join
(select user_id as user_id_1,user_l10_favor from new.pre_user_l10_favor)h
on(g.user_id=h.user_id_1))i
left outer join
(select user_id as user_id_1,user_l14_favor from new.pre_user_l14_favor)j
on(i.user_id=j.user_id_1);


/*user特征第4-2类*/

drop table if exists test.user_4_2;
create table test.user_4_2 as 
select user_id,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy,user_l7_distinct_buy from
(select user_id,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy from
(select user_id,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy from
(select user_id,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk from
(select user_id,user_l1_distinct_clk,user_l3_distinct_clk from
(select user_id,user_l1_distinct_clk from new.pre_user_l1_distinct_clk)a
left outer join
(select user_id as user_id_1,user_l3_distinct_clk from new.pre_user_l3_distinct_clk)b
on(a.user_id=b.user_id_1))c
left outer join
(select user_id as user_id_1,user_l7_distinct_clk from new.pre_user_l7_distinct_clk)d
on(c.user_id=d.user_id_1))e
left outer join
(select user_id as user_id_1,user_l1_distinct_buy from new.pre_user_l1_distinct_buy)f
on(e.user_id=f.user_id_1))g
left outer join
(select user_id as user_id_1,user_l3_distinct_buy from new.pre_user_l3_distinct_buy)h
on(g.user_id=h.user_id_1))i
left outer join
(select user_id as user_id_1,user_l7_distinct_buy from new.pre_user_l7_distinct_buy)j
on(i.user_id=j.user_id_1);


/*user特征第4类*/

drop table if exists test.user_4;
create table test.user_4 as 
select user_id,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor,user_l14_favor,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy,user_l7_distinct_buy
 from
(select user_id,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor,user_l14_favor
 from test.user_4_1)a
left outer join
(select user_id as user_id_1,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy,user_l7_distinct_buy
 from test.user_4_2)b
on(a.user_id=b.user_id_1);


/*user全部特征（37+1）*/


drop table if exists newresult.pre_user;
create table newresult.pre_user as 
select user_id,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy,ifnull(user_d12_clk,0) as user_d12_clk,ifnull(user_d12_buy,0) as user_d12_buy,user_l_act_date,user_l_buy_date,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor,user_l14_favor,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy,user_l7_distinct_buy from
(select user_id,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy,ifnull(user_d12_clk,0) as user_d12_clk,ifnull(user_d12_buy,0) as user_d12_buy,user_l_act_date,user_l_buy_date,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy from
(select user_id,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy,ifnull(user_d12_clk,0) as user_d12_clk,ifnull(user_d12_buy,0) as user_d12_buy,user_l_act_date,user_l_buy_date from
(select user_id,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy from test.user_1)a
left outer join
(select user_id as user_id_1,ifnull(user_d12_clk,0) as user_d12_clk,ifnull(user_d12_buy,0) as user_d12_buy,user_l_act_date,user_l_buy_date from test.user_2)b
on(a.user_id=b.user_id_1))c
left outer join
(select user_id as user_id_1,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy from test.user_3)d
on(c.user_id=d.user_id_1))e
left outer join
(select user_id as user_id_1,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor,user_l14_favor,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy,user_l7_distinct_buy from test.user_4)f
on(e.user_id=f.user_id_1);



/*ui+user特征（90+2）*/

drop table if exists newresult.pre_ui_user;
create table newresult.pre_ui_user as 
select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk,ui_d12_buy,ui_d12_clk,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor,ui_l14_favor,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act,ui_l3_h1_only_act,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy,ifnull(user_d12_clk,0) as user_d12_clk,ifnull(user_d12_buy,0) as user_d12_buy,user_l_act_date,user_l_buy_date,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor,user_l14_favor,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy,user_l7_distinct_buy
 from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk,ui_d12_buy,ui_d12_clk,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor,ui_l14_favor,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act,ui_l3_h1_only_act
 from newresult.pre_ui)a
left outer join
(select user_id as user_id_1,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy,ifnull(user_d12_clk,0) as user_d12_clk,ifnull(user_d12_buy,0) as user_d12_buy,user_l_act_date,user_l_buy_date,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor,user_l14_favor,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy,user_l7_distinct_buy
 from newresult.pre_user)b
on(a.user_id=b.user_id_1);
