drop table if exists test.item_1;
create table test.item_1 as 
select item_id,item_l1_buy,item_l3_buy,item_l7_buy,item_l14_buy from
(select item_id,item_l1_buy,item_l3_buy,item_l7_buy from
(select item_id,item_l1_buy,item_l3_buy from
(select item_id,item_l1_buy from new.pre_item_l1_buy)a
left outer join
(select item_id as item_id_1,item_l3_buy from new.pre_item_l3_buy)b
on(a.item_id=b.item_id_1))c
left outer join
(select item_id as item_id_1,item_l7_buy from new.pre_item_l7_buy)d
on(c.item_id=d.item_id_1))e
left outer join
(select item_id as item_id_1,item_l14_buy from new.pre_item_l14_buy)f
on(e.item_id=f.item_id_1);



drop table if exists test.item_2;
create table test.item_2 as 
select item_id,item_l1_clk,item_l3_clk,item_l7_clk,item_l14_clk from
(select item_id,item_l1_clk,item_l3_clk,item_l7_clk from
(select item_id,item_l1_clk,item_l3_clk from
(select item_id,item_l1_clk from new.pre_item_l1_clk)a
left outer join
(select item_id as item_id_1,item_l3_clk from new.pre_item_l3_clk)b
on(a.item_id=b.item_id_1))c
left outer join
(select item_id as item_id_1,item_l7_clk from new.pre_item_l7_clk)d
on(c.item_id=d.item_id_1))e
left outer join
(select item_id as item_id_1,item_l14_clk from new.pre_item_l14_clk)f
on(e.item_id=f.item_id_1);


drop table if exists test.item_3;
create table test.item_3 as 
select item_id,item_l1_distinct_clk,item_l3_distinct_clk,item_l7_distinct_clk,item_l14_distinct_clk from
(select item_id,item_l1_distinct_clk,item_l3_distinct_clk,item_l7_distinct_clk from
(select item_id,item_l1_distinct_clk,item_l3_distinct_clk from
(select item_id,item_l1_distinct_clk from new.pre_item_l1_distinct_clk)a
left outer join
(select item_id as item_id_1,item_l3_distinct_clk from new.pre_item_l3_distinct_clk)b
on(a.item_id=b.item_id_1))c
left outer join
(select item_id as item_id_1,item_l7_distinct_clk from new.pre_item_l7_distinct_clk)d
on(c.item_id=d.item_id_1))e
left outer join
(select item_id as item_id_1,item_l14_distinct_clk from new.pre_item_l14_distinct_clk)f
on(e.item_id=f.item_id_1);


drop table if exists test.item_4;
create table test.item_4 as 
select item_id,item_l1_distinct_buy,item_l3_distinct_buy,item_l7_distinct_buy,item_l14_distinct_buy from
(select item_id,item_l1_distinct_buy,item_l3_distinct_buy,item_l7_distinct_buy from
(select item_id,item_l1_distinct_buy,item_l3_distinct_buy from
(select item_id,item_l1_distinct_buy from new.pre_item_l1_distinct_buy)a
left outer join
(select item_id as item_id_1,item_l3_distinct_buy from new.pre_item_l3_distinct_buy)b
on(a.item_id=b.item_id_1))c
left outer join
(select item_id as item_id_1,item_l7_distinct_buy from new.pre_item_l7_distinct_buy)d
on(c.item_id=d.item_id_1))e
left outer join
(select item_id as item_id_1,item_l14_distinct_buy from new.pre_item_l14_distinct_buy)f
on(e.item_id=f.item_id_1);


drop table if exists test.item_5;
create table test.item_5 as 
select item_id,item_l_buy_date,item_l_act_date,item_ifrebuy,item_distinct_rebuy from
(select item_id,item_l_buy_date,item_l_act_date,item_ifrebuy from
(select item_id,item_l_buy_date,item_l_act_date from
(select item_id,item_l_buy_date from new.pre_item_l_buy_date)a
left outer join
(select item_id as item_id_1,item_l_act_date from new.pre_item_l_act_date)b
on(a.item_id=b.item_id_1))c
left outer join
(select item_id as item_id_1,item_ifrebuy from new.pre_item_ifrebuy)d
on(c.item_id=d.item_id_1))e
left outer join
(select item_id as item_id_1,item_distinct_rebuy from new.pre_item_distinct_rebuy)f
on(e.item_id=f.item_id_1);



drop table if exists newresult.pre_item;
create table newresult.pre_item as 
select item_id,item_l1_buy,item_l3_buy,item_l7_buy,item_l14_buy,item_l1_clk,item_l3_clk,item_l7_clk,item_l14_clk,item_l1_distinct_clk,item_l3_distinct_clk,item_l7_distinct_clk,item_l14_distinct_clk,item_l1_distinct_buy,item_l3_distinct_buy,item_l7_distinct_buy,item_l14_distinct_buy,item_l_buy_date,item_l_act_date,item_ifrebuy,item_distinct_rebuy from
(select item_id,item_l1_buy,item_l3_buy,item_l7_buy,item_l14_buy,item_l1_clk,item_l3_clk,item_l7_clk,item_l14_clk,item_l1_distinct_clk,item_l3_distinct_clk,item_l7_distinct_clk,item_l14_distinct_clk,item_l1_distinct_buy,item_l3_distinct_buy,item_l7_distinct_buy,item_l14_distinct_buy from
(select item_id,item_l1_buy,item_l3_buy,item_l7_buy,item_l14_buy,item_l1_clk,item_l3_clk,item_l7_clk,item_l14_clk,item_l1_distinct_clk,item_l3_distinct_clk,item_l7_distinct_clk,item_l14_distinct_clk from
(select item_id,item_l1_buy,item_l3_buy,item_l7_buy,item_l14_buy,item_l1_clk,item_l3_clk,item_l7_clk,item_l14_clk from
(select item_id,item_l1_buy,item_l3_buy,item_l7_buy,item_l14_buy from test.item_1)a
left outer join
(select item_id as item_id_1,item_l1_clk,item_l3_clk,item_l7_clk,item_l14_clk from test.item_2)b
on(a.item_id=b.item_id_1))c
left outer join
(select item_id as item_id_1,item_l1_distinct_clk,item_l3_distinct_clk,item_l7_distinct_clk,item_l14_distinct_clk from test.item_3)d
on(c.item_id=d.item_id_1))e
left outer join
(select item_id as item_id_1,item_l1_distinct_buy,item_l3_distinct_buy,item_l7_distinct_buy,item_l14_distinct_buy from test.item_4)f
on(e.item_id=f.item_id_1))g
left outer join
(select item_id as item_id_1,item_l_buy_date,item_l_act_date,item_ifrebuy,item_distinct_rebuy from test.item_5)h
on(g.item_id=h.item_id_1);


drop table if exists newresult.pre_ui_user_item;
create table newresult.pre_ui_user_item as 
select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk,ui_d12_buy,ui_d12_clk,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor,ui_l14_favor,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act,ui_l3_h1_only_act,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy,ifnull(user_d12_clk,0) as user_d12_clk,ifnull(user_d12_buy,0) as user_d12_buy,user_l_act_date,user_l_buy_date,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor,user_l14_favor,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy,user_l7_distinct_buy,item_l1_buy,item_l3_buy,item_l7_buy,item_l14_buy,item_l1_clk,item_l3_clk,item_l7_clk,item_l14_clk,item_l1_distinct_clk,item_l3_distinct_clk,item_l7_distinct_clk,item_l14_distinct_clk,item_l1_distinct_buy,item_l3_distinct_buy,item_l7_distinct_buy,item_l14_distinct_buy,item_l_buy_date,item_l_act_date,item_ifrebuy,item_distinct_rebuy
 from
(select user_id,item_id,ui_l1_buy,ui_l2_buy,ui_l3_buy,ui_l5_buy,ui_l7_buy,ui_l10_buy,ui_l14_buy,ui_l18_buy,ui_l1_clk,ui_l2_clk,ui_l3_clk,ui_l5_clk,ui_l7_clk,ui_l10_clk,ui_l14_clk,ui_l18_clk,ui_d12_buy,ui_d12_clk,ui_l_buy_date,ui_fl_buy_distance,ui_l_actday_clk,ui_l_actday_favor,ui_l_actday_buy,ui_l_act_date,ui_l3_h1_buy,ui_l5_h1_buy,ui_l7_h1_buy,ui_l3_h1_act,ui_l5_h1_act,ui_l7_h1_act,ui_f_buybefore_clk,ui_f_buybefore_favor,ui_f_buybefore_cart,ui_l7_clk2buy,ui_l14_clk2buy,ui_l3_clk2buy,ui_l7_later2buy,ui_l14_later2buy,ui_l3_later2buy,ui_l_act_distance,ui_min_distance,ui_avg_distance,ui_max_distance,ui_l1_cart,ui_l2_cart,ui_l3_cart,ui_l7_cart,ui_l7_favor,ui_l14_favor,ui_l18_favor,ui_l_actnextday_other_distinct_act,ui_l1_h1_only_act,ui_l3_h1_only_act,user_l1_clk,user_l3_clk,user_l7_clk,user_l10_clk,user_l14_clk,user_l18_clk,user_l1_buy,user_l3_buy,user_l7_buy,user_l10_buy,user_l14_buy,user_l18_buy,ifnull(user_d12_clk,0) as user_d12_clk,ifnull(user_d12_buy,0) as user_d12_buy,user_l_act_date,user_l_buy_date,user_l7_clk2buy,user_l14_clk2buy,user_l3_clk2buy,user_l7_later2buy,user_l14_later2buy,user_l3_later2buy,user_l7_h1_buy,user_l14_h1_buy,user_l3_h1_buy,user_l1_cart,user_l3_cart,user_l7_cart,user_l7_favor,user_l10_favor,user_l14_favor,user_l1_distinct_clk,user_l3_distinct_clk,user_l7_distinct_clk,user_l1_distinct_buy,user_l3_distinct_buy,user_l7_distinct_buy
 from newresult.pre_ui_user)a
left outer join
(select item_id as item_id_1,item_l1_buy,item_l3_buy,item_l7_buy,item_l14_buy,item_l1_clk,item_l3_clk,item_l7_clk,item_l14_clk,item_l1_distinct_clk,item_l3_distinct_clk,item_l7_distinct_clk,item_l14_distinct_clk,item_l1_distinct_buy,item_l3_distinct_buy,item_l7_distinct_buy,item_l14_distinct_buy,item_l_buy_date,item_l_act_date,item_ifrebuy,item_distinct_rebuy
 from newresult.pre_item)b
on(a.item_id=b.item_id_1);