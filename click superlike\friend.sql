source /etc/profile
ds={{ds}}
d_1=`date -d "$ds -1 days" +"%Y-%m-%d"`
export HADOOP_USER_NAME=gaowenting
hive -e "
set hive.execution.engine=mr;
set mapred.job.queue.name=datalake-da;
set mapreduce.reduce.memory.mb=8192;
set mapred.reduce.tasks=10;
SET mapred.job.name=gwt_followtolike_event_6_{{ds}};

CREATE TABLE IF NOT EXISTS da.gwt_followtolike_event_6 (
os string,
group_id string, 
gender string,
group_name string,

superlike_click bigint,
superlike_click_uv bigint,
friend_click bigint,
friend_click_uv bigint

) COMMENT 'click superlike/friend'
PARTITIONED BY (
  dt string COMMENT 'date partition, format is yyyy-MM-dd')
STORED AS PARQUET;

insert overwrite table da.gwt_followtolike_event_6 PARTITION(dt='{{ds}}')


select  os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end as group_id,
gender,group_name,
COALESCE(sum(superlike_click),0) AS superlike_click,
COUNT(DISTINCT case when superlike_click >0 then a.user_id else null end) AS superlike_click_uv,
COALESCE(sum(friend_click),0) AS friend_click,
COUNT(DISTINCT case when friend_click >0 then a.user_id else null end) AS friend_click_uv

FROM


(select user_id, dt,os, group_id as ab_group_id,gender,group_name

from dws.dws_ab_user_full_i_d
where dt='{{ds}}'  AND group_id IN (2934,2935) 
and length(os)>0 and os is not null and status='default' and ab_status='default') a


left outer join 

(
SELECT  dt, actor_user_id, 
count(distinct case when name rlike 'superlike.click' then 1 else null end) as superlike_click,
count(distinct case when name rlike 'friend.click' then 1 else null end) as friend_click
FROM dwd.dwd_tantan_eventlog_reduced_i_d
WHERE  dt='{{ds}}' AND name rlike 'superlike.click|friend.click'
GROUP BY dt, actor_user_id
)b
on a.dt =b.dt and a.user_id=b.actor_user_id
group by os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end,
gender,group_name


"
