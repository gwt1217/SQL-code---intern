--hive写法

select a.music_id,b1.name,a.created_time,
coalesce(sum(download_cnt),0) as download_cnt,
coalesce(sum(use_cnt),0) as use_cnt,
coalesce(sum(download_uv),0) as download_uv,
coalesce(sum(use_uv),0) as use_uv
from
(select music_id,to_date(created_time) as created_time
from dwd.dwd_putong_followship_music_category_musics_a_d
where dt='2019-08-01'
group by music_id,to_date(created_time)
)a

left outer join
(select id,name,rn
from
(select 
get_json_object(data_raw_data,'$.externalData.id')  as id,
get_json_object(data_raw_data,'$.externalData.name') as name,
----清洗数据：一个音乐id可能有多个音乐名（包含破折号、双引号、清洗数据）；根据时间排序，得到最新的音乐名
row_number() over(partition by get_json_object(data_raw_data,'$.externalData.id') order by event_timestamp desc) as rn
from dwd.dwd_tantan_eventlog_reduced_i_d 
where dt='2019-08-01' and name rlike 'music.download' 
and get_json_object(data_raw_data,'$.externalData.id') is not null 
and length(get_json_object(data_raw_data,'$.externalData.id'))>0
and get_json_object(data_raw_data,'$.externalData.name') is not null 
and length(get_json_object(data_raw_data,'$.externalData.name'))>0
and ab_group rlike 'newUIFollower' 
)b0
where rn=1
)b1
on cast(a.music_id as string)=cast(b1.id as string)


left outer join
(select 
get_json_object(data_raw_data,'$.externalData.id')  as id,
count(1) as download_cnt,
count(distinct actor_user_id) as download_uv
from dwd.dwd_tantan_eventlog_reduced_i_d 
where dt='2019-08-01' and name rlike 'music.download' 
and get_json_object(data_raw_data,'$.externalData.id') is not null 
and length(get_json_object(data_raw_data,'$.externalData.id'))>0
and get_json_object(data_raw_data,'$.externalData.name') is not null 
and length(get_json_object(data_raw_data,'$.externalData.name'))>0
and ab_group rlike 'newUIFollower' 
group by get_json_object(data_raw_data,'$.externalData.id')
)b
on cast(a.music_id as string)=cast(b.id as string)
left outer join
(
select 
get_json_object(data_raw_data,'$.externalData.musicid')  as id,
count(1) as use_cnt,
count(distinct actor_user_id) as use_uv
from dwd.dwd_tantan_eventlog_reduced_i_d 
where dt='2019-08-01' and name rlike 'camera.moment.posted' 
and get_json_object(data_raw_data,'$.externalData.musicid') is not null 
and length(get_json_object(data_raw_data,'$.externalData.musicid'))>0
and ab_group rlike 'newUIFollower' 
group by get_json_object(data_raw_data,'$.externalData.musicid')
)c
on cast(a.music_id as string)=cast(c.id as string) 
group by a.music_id,b1.name,a.created_time
