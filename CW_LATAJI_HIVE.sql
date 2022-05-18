-- Databricks notebook source
create table if not exists clinicaltrial_2021
using csv
options(header='true',
delimiter='|',
inferSchema='true',
path='/FileStore/tables/clinicaltrial_2021.csv');

-- COMMAND ----------

select * from clinicaltrial_2021
limit 3;

-- COMMAND ----------

create table if not exists pharma
using csv
options(header='true',
delimiter=',',
inferSchema='true',
path='/FileStore/tables/pharma.csv');

-- COMMAND ----------

create table if not exists mesh
using csv
options(header='true',
delimiter=',',
inferSchema='true',
path='/FileStore/tables/mesh.csv');
 cache table mesh

-- COMMAND ----------

select * from mesh
limit 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Answer-question 1
-- MAGIC The number of studies in the dataset

-- COMMAND ----------

select count(*) from clinicaltrial_2021;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Answer-question 2
-- MAGIC list all the types and frequencies

-- COMMAND ----------

select Type, count(*) as count
from clinicaltrial_2021
where Type !=''
group by Type
order by count desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Answer-question 3
-- MAGIC The top 5 conditions (from Conditions) with their frequencies

-- COMMAND ----------

create view if not exists condition as
select Clin_view.z as separated_condition
from (select explode(split(Conditions, ',')) as z from clinicaltrial_2021) Clin_view;

-- COMMAND ----------

select separated_condition, count(*) as count
from condition
where separated_condition !=''
group by separated_condition
order by count desc
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Answer-question 4
-- MAGIC mapped each condition to one or more hierarchy codes.Shows the 5
-- MAGIC most frequent roots after map
-- MAGIC done.

-- COMMAND ----------

select left(tree, 3) as root, count(*) as count
from condition, mesh 
where mesh.term = condition.separated_condition
group by root
order by count desc
limit 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Answer-question 5
-- MAGIC Find the 10 most common sponsors that are not pharmaceutical companies, along with the number
-- MAGIC of clinical trials they have sponsored

-- COMMAND ----------

create view if not exists Table_spons as
select Sponsor
from clinicaltrial_2021;

-- COMMAND ----------

create view if not exists Table_pharma as
select Parent_Company
from pharma;

-- COMMAND ----------

create view if not exists ClinPharma_view as
select * from Table_spons
where Table_spons.Sponsor not in (select Parent_Company from Table_pharma);

-- COMMAND ----------

select Sponsor, count(*) as count
from ClinPharma_view
where Sponsor is not null
group by Sponsor
order by count desc
limit 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Answer-question 6
-- MAGIC Plot number of completed studies each month in a given year – for the submission dataset, the year
-- MAGIC is 2021

-- COMMAND ----------

select left(Completion, 3) as month, count(*) as count
from clinicaltrial_2021
where Status = 'Completed' and right(Completion, 4) = '2021'
group by month
order by count desc;

-- COMMAND ----------



-- COMMAND ----------


