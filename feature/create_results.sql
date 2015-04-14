#alter table result.result add item_category int;
update result.result as t1
	left outer join
    (select item_id,item_category from tianchi.item) as t2
    on t1.item_id=t2.item_id
set t1.item_category=t2.item_category;