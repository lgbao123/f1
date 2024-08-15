-- Databricks notebook source
-- MAGIC %run ../Includes/configuration

-- COMMAND ----------

use hive_metastore.f1_presentation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Ranking Driver

-- COMMAND ----------

create or replace temp view v_rank_driver
as
select 
  driver_name,
  count(*) as total_race,
  round(avg(point),2) as avg_point,
  rank() over(order by avg(point) desc) as rk
from caculated_race_results
group by driver_name
having count(*) > 50
order by avg_point desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Find dominant driver

-- COMMAND ----------

SELECT 
  season,
  driver_name,
  count(*) as total_race,
  round(avg(point),2) as avg_point
from caculated_race_results
WHERE driver_name in 
  (SELECT driver_name from v_rank_driver WHERE rk <=10)
GROUP BY season,driver_name

-- COMMAND ----------

SELECT 
  driver_name,
  count(*) as total_race,
  round(avg(point),2) as avg_point
from caculated_race_results
WHERE driver_name in 
  (SELECT driver_name from v_rank_driver WHERE rk <=10)
GROUP BY driver_name
ORDER BY avg_point desc