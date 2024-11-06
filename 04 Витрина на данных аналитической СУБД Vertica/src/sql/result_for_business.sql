with user_group_messages as (
	select lgd.hk_group_id, count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages
	from STV2024060713__DWH.l_groups_dialogs lgd 
	left join STV2024060713__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id
	group by lgd.hk_group_id
), user_group_log as (
	select hg.hk_group_id, count(distinct lug.hk_user_id) as cnt_added_users
	from (select * 
		  from STV2024060713__DWH.h_groups
		  order by registration_dt
		  limit 10) as hg 
	left join (select * 
			   from STV2024060713__DWH.l_user_group_activity luga
			   where luga.hk_l_user_group_activity in (select hk_l_user_group_activity 
			   										   from STV2024060713__DWH.s_auth_history sah
			   										   where event = 'add')) as lug
    on hg.hk_group_id = lug.hk_group_id	
    group by hg.hk_group_id
)

select ugm.hk_group_id, 
	   cnt_added_users, 
	   cnt_users_in_group_with_messages, 
	   round(cnt_users_in_group_with_messages / cnt_added_users, 2) as group_conversion
from user_group_messages ugm 
join user_group_log ugl on ugm.hk_group_id = ugl.hk_group_id
order by group_conversion desc
