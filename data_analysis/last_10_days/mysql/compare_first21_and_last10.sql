use tianchi;
drop table first_20_info;
create table first_20_info as
select distinct user_id,count(*) as click_time from user
where behavior_type=1 and event_date<=21
group by user_id;

create table click as
select f.user_id,f.click_time,l.click_time as click_time_20,click_time_10
from first_20_info as f
right join last_10_info as l
on f.user_id=l.user_id;

drop table click;
drop table first_20_info;