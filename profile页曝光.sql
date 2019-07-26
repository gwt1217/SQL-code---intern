source /etc/profile
ds={{ds}}
d_1=`date -d "$ds -1 days" +"%Y-%m-%d"`
export HADOOP_USER_NAME=gaowenting
hive -e "
set hive.execution.engine=mr;
set mapred.job.queue.name=datalake-da;
set mapreduce.reduce.memory.mb=8192;
set mapred.reduce.tasks=10;
SET mapred.job.name=gwt_followtolike_event_5_{{ds}};

CREATE TABLE IF NOT EXISTS da.gwt_followtolike_event_5 (
os string,
group_id string, 
gender string,
group_name string,
profile_shown_source string,
pv bigint,
uv bigint

) COMMENT 'profile页曝光'
PARTITIONED BY (
  dt string COMMENT 'date partition, format is yyyy-MM-dd')
STORED AS PARQUET;

insert overwrite table da.gwt_followtolike_event_5 PARTITION(dt='{{ds}}')


select  os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end as group_id,
gender,group_name,
profile_shown_source,
COALESCE(sum(pv),0) AS pv,
COUNT(DISTINCT case when pv >0 then a.user_id else null end) AS uv
FROM


(select user_id, dt,os, group_id as ab_group_id,gender,group_name

from dws.dws_ab_user_full_i_d
where dt='{{ds}}'  AND group_id IN (2934,2935) 
and length(os)>0 and os is not null and status='default' and ab_status='default') a


left outer join 

(
SELECT  dt, actor_user_id, get_json_object(data_raw_data,'$.externalData.source') as profile_shown_source,
COUNT(1) as pv
FROM dwd.dwd_tantan_eventlog_reduced_i_d
WHERE  dt='{{ds}}' AND (name rlike 'explore.profile.shown')
GROUP BY dt, actor_user_id,get_json_object(data_raw_data,'$.externalData.source')
)b
on a.dt =b.dt and a.user_id=b.actor_user_id
group by os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end,
gender,group_name,
profile_shown_source

UNION ALL


select  os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end as group_id,
gender,group_name,
'all' as history_match_source,
COALESCE(sum(pv),0) AS pv,
COUNT(DISTINCT case when pv >0 then a.user_id else null end) AS uv
FROM


(select user_id, dt,os, group_id as ab_group_id,gender,group_name

from dws.dws_ab_user_full_i_d
where dt='{{ds}}'  AND group_id IN (2934,2935) and length(os)>0 
and os is not null and status='default' and ab_status='default') a


left outer join 

(
SELECT  dt, actor_user_id, 
COUNT(1) as pv
FROM dwd.dwd_tantan_eventlog_reduced_i_d
WHERE dt='{{ds}}' AND (name rlike 'explore.profile.shown')
GROUP BY dt, actor_user_id
)b
on a.dt =b.dt and a.user_id=b.actor_user_id
group by os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end,
gender,group_name

"
