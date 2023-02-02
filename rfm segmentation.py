# Databricks notebook source
# MAGIC %md #RFM Segmentation

# COMMAND ----------

# MAGIC %md ## Intro

# COMMAND ----------

# DBTITLE 0,Intro
# MAGIC %md
# MAGIC 
# MAGIC RFM is a method used for analyzing customer value. [wikipedia](https://en.wikipedia.org/wiki/RFM_%28market_research%29)
# MAGIC 
# MAGIC RFM stands for the three dimensions:
# MAGIC 
# MAGIC   * <b>[R] Recency</b> – How recently did the customer purchase?
# MAGIC   * <b>[F] Frequency</b> – How often do they purchase?
# MAGIC   * <b>[M] Monetary Value</b> – How much do they spend?
# MAGIC 
# MAGIC From a marketing perspective, it is valuable to understand the characteristics and preferences of your best customers for at least two reasons: 
# MAGIC   1) to continue to provide this group with what they’re looking for and keep them as customers, and 
# MAGIC   2) to target your marketing efforts toward prospects who are most likely to respond.
# MAGIC   
# MAGIC 
# MAGIC ## Why RFM? 
# MAGIC 
# MAGIC RFM segmentation was first employed by direct marketers sending catalogs via direct mail in the 1930s and 1940s. Catalogers would maintain and update a 3×5 index card for every customer in their file. Each index card was then forced ranked by when the customer made their last purchase, how often they purchased, and how much the customer had spent in their lifetime.
# MAGIC 
# MAGIC They had proven time and time again that customers with high RFM were the customers most likely to respond to new catalog deliveries. Their main objective for “ranking” customers in this fashion was to avoid sending costly print catalogs to customers who were unlikely to convert.
# MAGIC 
# MAGIC 
# MAGIC ## Assumptions
# MAGIC 
# MAGIC - Customers who have purchased more recently are more likely to purchase again when compared to customers who have purchased less recently.
# MAGIC - Customers who purchase more frequently are more likely to purchase again when compared to customers who have purchased only once, or less frequently.
# MAGIC - Customers who have higher total monetary spend are more likely to purchase again in the future when compared to customers who have spent less monetarily.

# COMMAND ----------

# MAGIC %md ## Demo dataset

# COMMAND ----------

# DBTITLE 1,CDNOW dataset 
# MAGIC %md
# MAGIC 
# MAGIC The file CDNOW_master.txt contains the entire purchase history up to the end of June 1998 of the cohort of 23,570 individuals who made their first-ever purchase at CDNOW in the first quarter of 1997. This CDNOW dataset was first used by Fader and Hardie (2001).
# MAGIC 
# MAGIC Each record in this file, 69,659 in total, comprises four fields: the customer's ID, the date of the transaction, the number of CDs purchased, and the dollar value of the transaction.
# MAGIC 
# MAGIC See [Notes on the CDNOW Master Data Set](http://brucehardie.com/notes/026/) for details of how the [1/10th systematic sample ](http://brucehardie.com/datasets/CDNOW_sample.zip) used in many papers was created. 
# MAGIC 
# MAGIC Reference:
# MAGIC Fader, Peter S. and Bruce G.,S. Hardie, (2001), "Forecasting Repeat Sales at CDNOW: A Case Study," Interfaces, 31 (May-June), Part 2 of 2, S94-S107.
# MAGIC 
# MAGIC 
# MAGIC Data set can be loaded from [here](https://www.brucehardie.com/datasets)

# COMMAND ----------

# DBTITLE 1,Upload sample data
# MAGIC %md
# MAGIC Databricks has a number of built in datasets to experiment with. To list available run <code>dbutils.fs.ls('dbfs:/databricks-datasets')</code> in a new cell.<br> 
# MAGIC Or, we can manually upload dataset to databricks DBFS: `File` > `Upload data to DBFS`. By default the dataset is saved to /FileStore/shared_uploads/{email}

# COMMAND ----------

dbutils.fs.ls('/FileStore/shared_uploads/')

# COMMAND ----------

# MAGIC %md ## Explore CDNOW sample

# COMMAND ----------

# MAGIC %md ### Load & prepare sample data

# COMMAND ----------

# Set path to data 

path = '{specify path to sample data}'

# customer's ID, the date of the transaction, the number of CDs purchased, and the dollar value of the transaction.
cdnow_sample = spark.read.format('csv').load(path, sep=' ')

# select and fix column types.
# Note: there is a difference between data in sampple and full data set.
# below script works for sample data. to use it for full dataset some tweaks might be necessary.
orders = cdnow_sample.selectExpr('CAST(_c2 as LONG) as customer_id', 
                                 'TO_DATE(_c3, "yyyyMMdd") as transaction_date', 
                                 'CAST(_c8 as DECIMAL(4, 2)) as amount')



# Uncoment if use CDNOW master
# orders = cdnow_df.selectExpr('CAST(_c1 as LONG) as customer_id', 
#                          'TO_DATE(_c2, "yyyyMMdd") as transaction_date', 
#                          'CAST(_c7 as DECIMAL(4, 2)) as amount')

orders.printSchema()

# COMMAND ----------

display(orders)

# COMMAND ----------

# DBTITLE 0,Filter missing values
# Filter missing values 
orders = orders.dropna(subset=['amount'])

# Register as a temp table to enable exploration with SQL 
orders.createOrReplaceTempView('orders')

# COMMAND ----------

# MAGIC %md ### Explore data

# COMMAND ----------

# DBTITLE 1,Exploring data using SQL 
# MAGIC %sql
# MAGIC SELECT customer_id, MIN(transaction_date) as first_visit_date, MAX(transaction_date) as last_visit_date FROM orders GROUP BY customer_id;

# COMMAND ----------

# DBTITLE 1,Collect sample summary with sql
# MAGIC %sql 
# MAGIC SELECT (
# MAGIC   (SELECT COUNT(*) FROM orders) as sample_records_count,
# MAGIC   (SELECT COUNT(DISTINCT(customer_id)) FROM orders) as unique_customers_count,
# MAGIC   (SELECT MIN(transaction_date) FROM orders) as min_date,
# MAGIC   (SELECT MAX(transaction_date) FROM orders) as max_date
# MAGIC ) as cdnow_sample_smr;

# COMMAND ----------

# DBTITLE 1,Capture output in python
# output is captured as _sqldf spark. dataframe.
cdnow_sample_smr = _sqldf.collect()[0].cdnow_sample_smr
print(cdnow_sample_smr)

# COMMAND ----------

# DBTITLE 1,Summary plot using python seaborn 
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

%matplotlib inline

smpl_pandas_df = spark.read.table('orders').toPandas()
smpl_pandas_df.head()

# plot spendings per transacti
fig, ax = plt.subplots(figsize=(14, 8))
sns.histplot(data=smpl_pandas_df, x='amount', ax=ax)
ax.set_title('Dsictibution of spending per transaction')

# COMMAND ----------

# DBTITLE 1,Using R ...
# MAGIC %r
# MAGIC library(sparklyr)
# MAGIC library(dplyr)
# MAGIC 
# MAGIC sc <- spark_connect(method="databricks")
# MAGIC r_df <- sparklyr::spark_read_table(sc, 'orders')

# COMMAND ----------

# DBTITLE 1,Aggregated summary plot using R ggplot
# MAGIC %r
# MAGIC library(ggplot2)
# MAGIC gr <- r_df %>% group_by(customer_id) %>%
# MAGIC   summarise(
# MAGIC     count = n(), 
# MAGIC     total_spent = sum(amount, na.rm = TRUE), 
# MAGIC     )
# MAGIC 
# MAGIC options(repr.plot.width=800, repr.plot.height=500) 
# MAGIC ggplot(gr, aes(x=total_spent)) + geom_histogram() + ggtitle('Distribution of total amount spend (aggregated per customers)')

# COMMAND ----------

# DBTITLE 1,Gentle look at cohorts retention rates
from pyspark.sql import functions as f
from pyspark.sql import Window

cohort_df = (
    spark.read.table('orders')
    .withColumn('order_year_month', f.date_format('transaction_date', 'yyyy-MM'))
    .withColumn('first_visit_date', f.min('transaction_date').over(Window.partitionBy('customer_id')))
    .withColumn('cohort', f.date_format('first_visit_date', 'yyyy-MM'))
    .withColumn('period', f.months_between('order_year_month', 'cohort'))
)

display( 
    cohort_df
    .groupby('cohort', 'period')
    .agg(f.count_distinct('customer_id').alias('n'))
    .withColumn('n0', f.sum(f.when(f.col('period') == 0, f.col('n'))).over(Window.partitionBy('cohort')))
    .filter(f.col('period')!=0)
    .withColumn('retention_rate', f.round(f.col('n')/f.col('n0'), 2))
    .selectExpr('cohort', 
                'period as months_since_join', 
                'n as total_active_customers', 
                'n0 as total_cohort_customers', 
                'retention_rate')
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## RFM Segmentation

# COMMAND ----------

# MAGIC %md ### Calculate RFM summary

# COMMAND ----------

import datetime

orders = spark.read.table('orders')

# assume we are at time point which is next to last available date in dataset.
run_date = cdnow_sample_smr.max_date + datetime.timedelta(days=1)

rfm = (
    orders
    .groupby('customer_id')
    .agg( 
         f.min('transaction_date').alias('first_visit_date'),
         f.max('transaction_date').alias('last_visit_date'),
         f.datediff(f.lit(run_date), f.max('transaction_date')).alias('recency'),
         (f.count_distinct('transaction_date') - 1).alias('frequency'),
         f.mean('amount').alias('monetary'),
         f.sum('amount').alias('total_amount')) 
)

display(rfm.select('customer_id', 'recency', 'frequency', 'monetary'))

# COMMAND ----------

# DBTITLE 1,Simplest customer ranking
from pyspark.sql import Window

# rank castomers based on recency ASC, frequency DESC, monetary DESC
display(
   rfm.withColumn('rank', f.dense_rank().over(Window.orderBy(f.asc('recency'), f.desc('frequency'), f.desc('monetary'))))
) 

# COMMAND ----------

# MAGIC %md ### Bucketize RFM values 

# COMMAND ----------

from pyspark.ml.feature import QuantileDiscretizer

# Initialize quantile discretizer
qd = QuantileDiscretizer(
    inputCols=['recency', 'frequency', 'monetary'], 
    outputCols=['recency_score', 'frequency_score', 'monetary_score'], 
    numBucketsArray=[5, 5, 5]
)
qd.setHandleInvalid('keep')

# learn quantiles 
bucketizer = qd.fit(rfm)

# Examine quantiles learned by bucketizer
for c, bins in zip(['recency', 'frequency', 'monetary'], bucketizer.getSplitsArray()):
    print(f'{c} quantiles: {bins}')

# apply bucketizer to rfm data 
rfm_score = bucketizer.transform(rfm)

# post processing: convert double into int for scores (to cleanu .0 when concatenation is done)
rfm_score = (
    rfm_score
    .withColumn('recency_score', f.col('recency_score').cast('integer'))
    .withColumn('frequency_score', f.col('frequency_score').cast('integer'))
    .withColumn('monetary_score', f.col('monetary_score').cast('integer'))
)
    
# post processing: fix recency score direction (so higher recency score - the better )
max_recency_score = rfm_score.selectExpr('MAX(recency_score) as mrs').collect()[0].mrs
print(f'Max recency score: {max_recency_score}')
rfm_score = rfm_score.withColumn('recency_score', f.lit(max_recency_score) - f.col('recency_score'))

rfm_score.createOrReplaceTempView('rfm_scores')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM rfm_scores ORDER BY recency_score DESC,  frequency_score DESC, monetary_score DESC, total_amount DESC;

# COMMAND ----------

# MAGIC %md ### Simple segmentaion ideas

# COMMAND ----------

def stack_rfm_metrics(dataframe, pivot_col='score'):
    """Helper method to stack recency, frequency and monetary values as one column"""
  
    return (
        dataframe
        .withColumn('frequency', f.col('frequency').cast('integer'))
        .withColumn('monetary', f.col('monetary').cast('integer'))
        .selectExpr(pivot_col, "stack(3, 'recency', recency, 'frequency', frequency, 'monetary', monetary) as (metric, value)")
    )

# COMMAND ----------

# DBTITLE 1,Idea 1: rank = recency score + frequency score +  monetary score
rfm_score = spark.read.table('rfm_scores')

display(
    rfm_score
    .withColumn('score', f.col('recency_score') + f.col('frequency_score') + f.col('monetary_score'))
    .transform(stack_rfm_metrics)
)

# COMMAND ----------

# DBTITLE 1,Idea 2: rank = concat(recency_score,  frequency_score, monetary_score)
display(
   rfm_score
  .withColumn('score', f.concat('recency_score', 'frequency_score', 'monetary_score'))
  .transform(stack_rfm_metrics)
)

# COMMAND ----------

# MAGIC %md ## Segmentation with K-means

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

rfm_score = spark.read.table('rfm_scores')

assembler = VectorAssembler(inputCols=['recency', 'frequency', 'monetary'], 
                            outputCol='features')

scaler = StandardScaler(inputCol='features', outputCol='features_scaled')


kmm = KMeans(featuresCol='features_scaled', predictionCol='cluster', k=5)

model_pipeline = Pipeline(stages=[assembler, scaler, kmm])
model = model_pipeline.fit(rfm_score)

# COMMAND ----------

rmf_clusters = model.transform(rfm_score)
display(rmf_clusters)

# COMMAND ----------

from pyspark.ml.functions import vector_to_array

display(rmf_clusters
        .select('features_scaled', 'cluster')
        .withColumn('rfm_scaled', vector_to_array('features_scaled'))
        .withColumn('r_scaled', f.col('rfm_scaled').getItem(0))
        .withColumn('f_scaled', f.col('rfm_scaled').getItem(1))
        .withColumn('m_scaled', f.col('rfm_scaled').getItem(2))
        .drop('features_scaled', 'rfm_scaled')
       )

# COMMAND ----------

display(
    rmf_clusters
    .filter(f.col('cluster') == 2)
    .select('customer_id', 'recency', 'frequency', 'monetary', 'total_amount')
    .sort(f.desc('total_amount'))   
)

# MAGIC COMMAND ----------

# MAGIC %md ## Some quesitions to consider ....

# MAGIC 1. Plot total transaction count time series with your tool of choise 
# MAGIC 2. It seems like March 23-28 what a time moment when customer transactions went down. How many loyal customers retains? Note: there is a need to reserach and define which customer to call loyal. 
# MAGIC 3. How to visualise churned cuatomers? (churned - or sometimes called `dead` customers - those who stopped # MAGIC purchasing cd.)
# MAGIC 4. Use Silhuette method to identify optimal number of clusters 
# MAGIC COMMAND ----------