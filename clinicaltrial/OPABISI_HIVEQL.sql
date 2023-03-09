-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Confirmation that datasets exists in the location

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/FileStore/tables/clinicaltrial_2021.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/FileStore/tables/mesh.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("dbfs:/FileStore/tables/pharma.csv")

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/user/hive/warehouse/clinicaltrial_2021

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/user/hive/warehouse/mesh

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC rm -r dbfs:/user/hive/warehouse/pharma

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create Table design from DF and save the Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Hive_2021 = spark.read.options(delimiter = "|", header = True, inferSchema = True).csv("dbfs:/FileStore/tables/clinicaltrial_2021.csv")
-- MAGIC Hive_mesh_2021 = spark.read.options(delimiter = ",", header = True, inferSchema=True).csv("dbfs:/FileStore/tables/mesh.csv")
-- MAGIC Hive_pharma_2021 = spark.read.options(delimiter = ",", header = True, inferSchema = True).csv("dbfs:/FileStore/tables/pharma.csv")
-- MAGIC 
-- MAGIC 
-- MAGIC Hive_2021.write.format("parquet").mode("overwrite").saveAsTable("clinicaltrial_2021")
-- MAGIC Hive_pharma_2021.write.format("parquet").mode("overwrite").saveAsTable("pharma")
-- MAGIC Hive_mesh_2021.write.format("parquet").mode("overwrite").saveAsTable("mesh")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Confirmation that Table exists

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT *
-- MAGIC FROM clinicaltrial_2021

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 1

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --Question 1
-- MAGIC SELECT COUNT(DISTINCT(Id)) as Distinct_studies_count
-- MAGIC FROM clinicaltrial_2021
-- MAGIC WHERE Id IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 2

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --Question 2
-- MAGIC SELECT Type, COUNT(*) as frequency
-- MAGIC FROM clinicaltrial_2021
-- MAGIC WHERE Type IS NOT NULL
-- MAGIC GROUP BY Type
-- MAGIC ORDER BY frequency DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 3

-- COMMAND ----------

--Question 3
SELECT topconditions, COUNT(topconditions) AS frequency
FROM clinicaltrial_2021 LATERAL VIEW explode(split(conditions,',')) as topconditions
GROUP BY topconditions
ORDER BY frequency DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##ALTERNATIVELY, SUBQUERY METHOD WAS USED TO ACHIEVE SAME RESULTS

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --Question 3
-- MAGIC SELECT Top_conditions, COUNT(*) as frequency
-- MAGIC FROM (SELECT Conditions, EXPLODE(SPLIT(Conditions, ",")) as Top_conditions 
-- MAGIC FROM clinicaltrial_2021
-- MAGIC WHERE Conditions IS NOT NULL)
-- MAGIC GROUP BY Top_conditions
-- MAGIC ORDER BY frequency DESC
-- MAGIC LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 4

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --Question 4 
-- MAGIC SELECT *
-- MAGIC FROM mesh

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT substring(tree, 1, 3) as root, COUNT(*) as frequency
-- MAGIC FROM (SELECT Conditions, EXPLODE(SPLIT(Conditions, ",")) as top_conditions 
-- MAGIC FROM clinicaltrial_2021 
-- MAGIC WHERE Conditions IS NOT NULL) c
-- MAGIC INNER JOIN mesh m
-- MAGIC ON c.top_conditions = m.term
-- MAGIC GROUP BY root
-- MAGIC ORDER BY frequency DESC
-- MAGIC LIMIT 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 5

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT *
-- MAGIC FROM pharma

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --Question 5
-- MAGIC SELECT Sponsor, COUNT(*) as frequency
-- MAGIC FROM clinicaltrial_2021
-- MAGIC WHERE Sponsor NOT IN (SELECT DISTINCT(Parent_Company) FROM pharma)
-- MAGIC GROUP BY Sponsor
-- MAGIC ORDER BY frequency DESC
-- MAGIC LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Question 6

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --Question 6
-- MAGIC SELECT SUBSTRING(Completion,1,3) AS Month, count(Completion) AS frequency
-- MAGIC FROM clinicaltrial_2021
-- MAGIC WHERE status = 'Completed' 
-- MAGIC AND Completion LIKE '%2021%'
-- MAGIC GROUP BY Month
-- MAGIC ORDER BY unix_timestamp(Month, 'MMM') 
-- MAGIC LIMIT 12

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##VISUALIZATION FOR COMPLETED STUDIES

-- COMMAND ----------

--DROP VIEW completed_studies

-- COMMAND ----------

CREATE VIEW completed_studies AS
SELECT SUBSTRING(Completion,1,3) AS Month, count(Completion) AS frequency
FROM clinicaltrial_2021
WHERE status = 'Completed' 
AND Completion LIKE '%2021%'
GROUP BY Month
ORDER BY unix_timestamp(Month, 'MMM') 
LIMIT 12

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import *
-- MAGIC import pyspark
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC 
-- MAGIC 
-- MAGIC HivePlot_Completed = HiveContext(sc)
-- MAGIC 
-- MAGIC df = HivePlot_Completed.sql("select * from completed_studies")
-- MAGIC df = df.toPandas()
-- MAGIC df.set_index('Month').plot(kind = 'bar')
-- MAGIC plt.grid()
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##FURTHER ANALYSIS FOR UNCOMPLETED STUDIES

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --Further Analysis for uncompleted studies
-- MAGIC SELECT  SUBSTRING(Completion,1,3) AS Month, count(Completion) AS frequency
-- MAGIC FROM clinicaltrial_2021
-- MAGIC WHERE status != 'Completed' 
-- MAGIC AND Completion LIKE '%2021%'
-- MAGIC GROUP BY Completion 
-- MAGIC ORDER BY unix_timestamp(Month, 'MMM') 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##VISUALIZATION FOR UNCOMPLETED STUDIES

-- COMMAND ----------

--DROP VIEW uncompleted_studies

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE VIEW uncompleted_studies AS
-- MAGIC SELECT  SUBSTRING(Completion,1,3) AS Month, count(Completion) AS frequency
-- MAGIC FROM clinicaltrial_2021
-- MAGIC WHERE status != 'Completed' 
-- MAGIC AND Completion LIKE '%2021%'
-- MAGIC GROUP BY Completion 
-- MAGIC ORDER BY unix_timestamp(Month, 'MMM') 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import *
-- MAGIC import pyspark
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC 
-- MAGIC 
-- MAGIC HivePlot_Uncompleted = HiveContext(sc)
-- MAGIC 
-- MAGIC df = HivePlot_Uncompleted.sql("select * from uncompleted_studies")
-- MAGIC df = df.toPandas()
-- MAGIC df.set_index('Month').plot(kind = 'bar')
-- MAGIC plt.grid()
-- MAGIC plt.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##UNCOMPLETED STUDIES DESCRIPTION

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --Further Analysis for uncompleted studies decsription
-- MAGIC SELECT  Status AS Description, count(Completion) AS frequency
-- MAGIC FROM clinicaltrial_2021
-- MAGIC WHERE status != 'Completed' 
-- MAGIC AND Completion LIKE '%2021%'
-- MAGIC GROUP BY status
-- MAGIC ORDER BY frequency DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##VISUALIZATION FOR UNCOMPLETED STUDIES DESCRIPTION

-- COMMAND ----------

--DROP VIEW uncompleted_studies_description

-- COMMAND ----------

CREATE VIEW uncompleted_studies_description AS
SELECT  Status AS Description, count(Completion) AS frequency
FROM clinicaltrial_2021
WHERE status != 'Completed' 
AND Completion LIKE '%2021%'
GROUP BY status
ORDER BY frequency DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import *
-- MAGIC import pyspark
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC 
-- MAGIC 
-- MAGIC HivePlot_Uncompleted_description = HiveContext(sc)
-- MAGIC 
-- MAGIC df = HivePlot_Uncompleted_description.sql("select * from uncompleted_studies_description")
-- MAGIC df = df.toPandas()
-- MAGIC df.set_index('Description').plot(kind = 'bar')
-- MAGIC plt.grid()
-- MAGIC plt.show()

-- COMMAND ----------


