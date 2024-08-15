-- Databricks notebook source
-- MAGIC %run ../Includes/configuration

-- COMMAND ----------

use hive_metastore.f1_processed

-- COMMAND ----------

CREATE or REPLACE table hive_metastore.f1_presentation.caculated_race_results
as 
SELECT 
  races.season,
  concat (drivers.family_name, ' ',drivers.given_name) as driver_name,
  constructors.constructors_name as team,
  position,
  11-position as point
FROM results
  join races on races.race_id = results.race_id
  join drivers on drivers.driver_id = results.driver_id
  join constructors on constructors.constructor_id = results.constructor_id
where position <=10



-- COMMAND ----------

select * from  hive_metastore.f1_presentation.caculated_race_results