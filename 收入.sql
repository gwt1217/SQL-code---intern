SELECT a.dt, a.os, a.gender,  a.group_name, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end as group_id, 
if_from_auto,
product_type,
COUNT(DISTINCT CASE WHEN amount>0 THEN b.user_id ELSE NULL END) AS buyer,
COALESCE(SUM(amount),0) AS amount,
COUNT(DISTINCT a.user_id) AS dau
FROM
(select user_id, dt,os, group_id as ab_group_id,gender,group_name

from dws.dws_ab_user_full_i_d
where dt>='2019-07-21'  AND group_id IN (2934,2935) and length(os)>0 and os is not null and status='default' and ab_status='default') a

LEFT OUTER JOIN

(select 
to_date(order_updated_time) as dt,
CASE WHEN if_from_auto=1 THEN '续费'
WHEN  if_from_auto=0 THEN '新购买' 
ELSE '未购买' END AS if_from_auto,
(case when product_type in  ('vip','seeWhoLikedMe','superLike') then product_type else 'other' end ) as product_type,
amount,user_id
from data_analytics.wanglili_success_order_details 
where 
to_date(order_updated_time)>='2019-07-21')b

ON a.dt=b.dt AND a.user_id=b.user_id

GROUP BY 
a.dt, a.os, a.gender, a.group_name, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end , 
 if_from_auto,
product_type


UNION ALL 


SELECT a.dt, a.os, a.gender, a.group_name, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end as group_id, 
'all' as if_from_auto,
product_type,
COUNT(DISTINCT CASE WHEN amount>0 THEN b.user_id ELSE NULL END) AS buyer,
COALESCE(SUM(amount),0) AS amount,
COUNT(DISTINCT a.user_id) AS dau
FROM
(select user_id, dt,os, group_id as ab_group_id,gender,group_name

from dws.dws_ab_user_full_i_d
where dt>='2019-07-21'  AND group_id IN (2934,2935) and length(os)>0 and os is not null and status='default' and ab_status='default') a

LEFT OUTER JOIN

(select 
to_date(order_updated_time) as dt,
(case when product_type in  ('vip','seeWhoLikedMe','superLike') then product_type else 'other' end ) as product_type,
amount,
user_id
from data_analytics.wanglili_success_order_details 
where 
to_date(order_updated_time)>='2019-07-21')b

ON a.dt=b.dt AND a.user_id=b.user_id

GROUP BY 
a.dt, a.os, a.gender, a.group_name, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end , 
product_type


UNION ALL 


SELECT a.dt, a.os, a.gender, a.group_name, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end as group_id, 
 if_from_auto,
'all' as product_type,
COUNT(DISTINCT CASE WHEN amount>0 THEN b.user_id ELSE NULL END) AS buyer,
COALESCE(SUM(amount),0) AS amount,
COUNT(DISTINCT a.user_id) AS dau
FROM
(select user_id, dt,os, group_id as ab_group_id,gender,group_name

from dws.dws_ab_user_full_i_d
where dt>='2019-07-21'  AND group_id IN (2934,2935) and length(os)>0 and os is not null and status='default' and ab_status='default') a

LEFT OUTER JOIN

(select 
to_date(order_updated_time) as dt,
amount,
CASE WHEN if_from_auto=1 THEN '续费'
WHEN  if_from_auto=0 THEN '新购买' 
ELSE '未购买' END AS if_from_auto,
user_id
from data_analytics.wanglili_success_order_details 
where 
to_date(order_updated_time)>='2019-07-21')b

ON a.dt=b.dt AND a.user_id=b.user_id

GROUP BY 
a.dt, a.os, a.gender, a.group_name, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end , 
if_from_auto

UNION ALL 


SELECT a.dt, a.os, a.gender,  a.group_name, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end as group_id, 
 'all' as if_from_auto,
'all' as product_type,
COUNT(DISTINCT CASE WHEN amount>0 THEN b.user_id ELSE NULL END) AS buyer,
COALESCE(SUM(amount),0) AS amount,
COUNT(DISTINCT a.user_id) AS dau
FROM
(select user_id, dt,os, group_id as ab_group_id,gender,group_name

from dws.dws_ab_user_full_i_d
where dt>='2019-07-21'  AND group_id IN (2934,2935) and length(os)>0 and os is not null and status='default' and ab_status='default') a

LEFT OUTER JOIN

(select 
to_date(order_updated_time) as dt,
amount,
user_id
from data_analytics.wanglili_success_order_details 
where 
to_date(order_updated_time)>='2019-07-21')b

ON a.dt=b.dt AND a.user_id=b.user_id

GROUP BY 
a.dt, a.os, a.gender,a.group_name, 
case when ab_group_id=2934 then '对照组'
when ab_group_id=2935 then '实验组' 
else 'other' end
