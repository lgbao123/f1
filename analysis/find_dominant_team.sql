-- Databricks notebook source
-- MAGIC %run ../Includes/configuration

-- COMMAND ----------

use hive_metastore.f1_presentation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Ranking Driver

-- COMMAND ----------

create or replace temp view v_rank_team
as
select 
  team,
  count(*) as total_race,
  round(avg(point),2) as avg_point,
  rank() over(order by avg(point) desc) as rk
from caculated_race_results
group by team
having count(*) > 100
order by avg_point desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Find dominant driver

-- COMMAND ----------

SELECT 
  season,
  team,
  count(*) as total_race,
  round(avg(point),2) as avg_point
from caculated_race_results
WHERE team in 
  (SELECT team from v_rank_team WHERE rk <=10)
GROUP BY season,team

-- COMMAND ----------

SELECT 
  team,
  count(*) as total_race,
  round(avg(point),2) as avg_point
from caculated_race_results
WHERE team in 
  (SELECT team from v_rank_team WHERE rk <=10)
GROUP BY team
ORDER BY avg_point desc