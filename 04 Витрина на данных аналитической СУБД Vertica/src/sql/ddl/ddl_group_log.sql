create table if not exists STV2024060713__STAGING.group_log (
	group_id int not null references STV2024060713__STAGING.groups,
	user_id int not null references STV2024060713__STAGING.users,
	user_id_from int references STV2024060713__STAGING.users,
	event varchar(6) not null,
	"datetime" timestamp not null check (("datetime" >= '2020-09-03') and ("datetime" <= current_date())),
	constraint group_log_unique unique (group_id, user_id, user_id_from, event, "datetime")
)
order by group_id, user_id
segmented by hash(group_id) all nodes
partition by "datetime"::date
group by calendar_hierarchy_day("datetime"::date, 3, 2); 