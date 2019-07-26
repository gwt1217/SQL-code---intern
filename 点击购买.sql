source /etc/profile
ds={{ds}}
d_1=`date -d "$ds -1 days" +"%Y-%m-%d"`
export HADOOP_USER_NAME=gaowenting
hive -e "
set hive.execution.engine=mr;
set mapred.job.queue.name=datalake-da;
set mapreduce.reduce.memory.mb=8192;
set mapred.reduce.tasks=10;
SET mapred.job.name=gwt_followtolike_event_2_{{ds}};

CREATE TABLE IF NOT EXISTS da.gwt_followtolike_event_2 (
os string,
group_id string, 
gender string,
group_name string,
purchase_product_type string,
pv bigint,
uv bigint

) COMMENT '点击购买'
PARTITIONED BY (
  dt string COMMENT 'date partition, format is yyyy-MM-dd')
STORED AS PARQUET;

insert overwrite table da.gwt_followtolike_event_2 PARTITION(dt='{{ds}}')


select  os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end as group_id,
gender,group_name,
purchase_product_type,
COALESCE(sum(pv),0) AS pv,
COUNT(DISTINCT case when pv >0 then a.user_id else null end) AS uv
FROM


(select user_id, dt,os, group_id as ab_group_id,gender,group_name

from dws.dws_ab_user_full_i_d
where dt='{{ds}}'  AND group_id IN (2934,2935) and length(os)>0 and os is not null and status='default' and ab_status='default') a


left outer join 

(
SELECT  dt, actor_user_id, get_json_object(data_raw_data,'$.externalData.productType') as purchase_product_type,
COUNT(1) as pv
FROM dwd.dwd_tantan_eventlog_reduced_i_d
WHERE  dt='{{ds}}' AND (name rlike 'purchase_page.purchase_button.click'
AND get_json_object(data_raw_data,'$.externalData.showFrom')='nearby')
GROUP BY dt, actor_user_id, get_json_object(data_raw_data,'$.externalData.productType')
)b
on a.dt =b.dt and a.user_id=b.actor_user_id
group by os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end,
gender,group_name,
purchase_product_type

UNION ALL


select  os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end as group_id,
gender,group_name,
'all' as purchase_product_type,
COALESCE(sum(pv),0) AS pv,
COUNT(DISTINCT case when pv >0 then a.user_id else null end) AS uv
FROM


(select user_id, dt,os, group_id as ab_group_id,gender,group_name

from dws.dws_ab_user_full_i_d
where dt='{{ds}}'  AND group_id IN (2934,2935) and length(os)>0 and os is not null and status='default' and ab_status='default') a


left outer join 

(
SELECT  dt, actor_user_id, 
COUNT(1) as pv
FROM dwd.dwd_tantan_eventlog_reduced_i_d
WHERE dt='{{ds}}' AND (name rlike 'purchase_page.purchase_button.click'
AND get_json_object(data_raw_data,'$.externalData.showFrom')='nearby')
GROUP BY dt, actor_user_id
)b
on a.dt =b.dt and a.user_id=b.actor_user_id
group by os, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end,
gender,group_name

"
