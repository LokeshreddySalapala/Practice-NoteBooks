# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading CSV file**

# COMMAND ----------

dbutils.fs.ls("/Volumes/databricksmaster/default/databricks/")

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).load('/Volumes/databricksmaster/default/databricks/BigMartSales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Reading JSON file**

# COMMAND ----------

dbutils.fs.ls("/Volumes/databricksmaster/default/databricks/")

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema', True).option('header', True).load('/Volumes/databricksmaster/default/databricks/drivers.json')


# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Schema Definition**

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **DDL SCHEMA**

# COMMAND ----------

my_ddl_schema = '''
  Item_Identifier string,
  Item_Weight string,
  Item_Fat_Content string,
  Item_Visibility double,
  Item_Type string,
  Item_MRP double,
  Outlet_Identifier string,
  Outlet_Establishment_Year integer,
  Outlet_Size string,
  Outlet_Location_Type string,
  Outlet_Type string,
  Item_Outlet_Sales double
'''

# COMMAND ----------

df = spark.read.format('csv').schema(my_ddl_schema).option('header', True).load('/Volumes/databricksmaster/default/databricks/BigMartSales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Struct Type() Schema

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

my_stuct_schema = StructType([
                        StructField('Item_Identifier',StringType(),True),
                        StructField('Item_Weight',StringType(),True),
                        StructField('Item_Fat_Content',StringType(),True),
                        StructField('Item_Visibility',StringType(),True),
                        StructField('Item_Type',StringType(),True),
                        StructField('Item_MRP',StringType(),True),
                        StructField('Outlet_Identifier',StringType(),True),
                        StructField('Outlet_Establishment_Year',StringType(),True),
                        StructField('Outlet_Size',StringType(),True),
                        StructField('Outlet_Location_Type',StringType(),True),
                        StructField('Outlet_Type',StringType(),True),
                        StructField('Item_Outlet_Sales',StringType(),True)
])


# COMMAND ----------

df = spark.read.format('csv').schema(my_stuct_schema).option('header', True).load('/Volumes/databricksmaster/default/databricks/BigMartSales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select

# COMMAND ----------

df.display()

# COMMAND ----------

df_sel = df.select('Item_Identifier','Item_Weight','Item_Fat_Content')
df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter/Where

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario -1**

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario - 2**

# COMMAND ----------

df.filter( (col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10 )).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario - 3**

# COMMAND ----------

df.filter( (col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn Renamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_WT').display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### With Column

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario-1  Creating New Column**

# COMMAND ----------

df = df.withColumn('Flag',lit('New'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformation from both column and create new column**

# COMMAND ----------

df.withColumn('Multiply',col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario -2 : Modify the the column values**

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','Reg')) \
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','LF')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast('string')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort/OrderBy

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario -1**

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario -2**

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario -3**

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,0]).display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Scenario -4**

# COMMAND ----------

df.sort(["Item_Weight","Item_Visibility"],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

df.drop("Item_Visibility").display()

# COMMAND ----------

df.drop("Item_Visibility","Item_Type").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(subset=["Item_Type"]).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union and Union ByName

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Preparing Dataframes

# COMMAND ----------

data1 = [(1,'Lokesh'),
         (2,'Ravi')]
Schema1 = 'id STRING,name STRING'
df1 = spark.createDataFrame(data1,Schema1)

data2 = [(3,'Reddy'),
         (4,'Rahul')]
Schema2 = 'id STRING,name STRING'
df2 = spark.createDataFrame(data2,Schema2)


# COMMAND ----------

df1.display()
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **UNION**

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **UNION BYNAME**

# COMMAND ----------

data1 = [('Lokesh',1),
         ('Ravi',2)]
Schema1 = 'id STRING,name STRING'
df1 = spark.createDataFrame(data1,Schema1)

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.display()
df2.display()
df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## String Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### INITCAP() , UPPER(), LOWER()

# COMMAND ----------

df.select(initcap("Item_Type").alias("Initcap_Item_Type")).display()
df.select(upper("Item_Type").alias("upper_Item_Type")).display()
df.select(lower("Item_Type").alias("lower_Item_Type")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Functions

# COMMAND ----------

# MAGIC %md
# MAGIC **Current Date**

# COMMAND ----------

df = df.withColumn('curr_date',current_date())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Date_Add()**

# COMMAND ----------

df = df.withColumn('week_after',date_add('curr_date',7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Date Sub()**
# MAGIC

# COMMAND ----------

df.withColumn('week_before', date_sub('curr_date', 7)).display()

# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date', -7))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATEDIFF

# COMMAND ----------

df = df.withColumn('datediff',datediff('curr_date','week_after'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date_Format

# COMMAND ----------

df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))
df.display()
                
                   

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping Nulls**

# COMMAND ----------

df.dropna('all').display()

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Filling Null Values

# COMMAND ----------

# MAGIC %md
# MAGIC **Filll all values with provided value**

# COMMAND ----------

df.fillna('Not Available').display()
df.fillna(0).display()


# COMMAND ----------

df.fillna('Notavailable',subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Split and Indexing

# COMMAND ----------

# MAGIC %md
# MAGIC **Split**

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Indexing**

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Explode

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Array Contains

# COMMAND ----------

df_exp.withColumn('Type_flag',array_contains('Outlet_Type', 'Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group_By

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario-1

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario-2

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Scenario-3

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('sum'),avg('Item_MRP').alias('avg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect List

# COMMAND ----------

data = [('user1','book1'), ('user1','book2'), ('user2','book2'), ('user3','book1'), ('user2','book4')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data, schema)

df_book.display()


# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book').alias('book_count')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PIVOT
# MAGIC

# COMMAND ----------

df.groupBy("Item_Type").pivot("Outlet_Size").sum("Item_MRP").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHEN-OTHERWISE

# COMMAND ----------

# MAGIC %md
# MAGIC #####Scenario-1

# COMMAND ----------

df = df.withColumn('veg_flag', when(col('Item_Type')=='Meat','Non-veg').otherwise('veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Scenario-2

# COMMAND ----------

df.display()

# COMMAND ----------

df_veg = df.withColumn('veg_flag', when(col('Item_Type')=='Meat','Non-veg').otherwise('veg'))

# COMMAND ----------

df_veg.display()

# COMMAND ----------

from pyspark.sql.functions import when, col, lower

df_veg.withColumn('veg_exp_flag',when((col('veg_flag')=='veg') & (col('Item_MRP')<100), 'Veg_Inexpensive').when((col('veg_flag')=='veg') & (col('Item_MRP')>100), 'Veg_Expensive').otherwise('Non_veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### JOINS

# COMMAND ----------

dataj1 = [(1,'john','d01'),(2,'mary','d02'),(3,'peter','d03'),(4,'paul','d03'),(5,'jack','d05'),(6,'jill','d06')]

schemaj1 = 'emp_id String, emp_name String, dept_id String'

dfj1 = spark.createDataFrame(dataj1, schemaj1)

dataj2 = [('d01','HR'),('d02','IT'), ('d03','Sales'), ('d04','Finance'), ('d05','Marketing')]

schemaj2 = 'dept_id String, department String'

dfj2 = spark.createDataFrame(dataj2, schemaj2)

# COMMAND ----------

dfj1.display()
dfj2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inner Join

# COMMAND ----------

dfj1.join(dfj2, dfj1['dept_id']==dfj2['dept_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Left Join

# COMMAND ----------

dfj1.join(dfj2, dfj1['dept_id']==dfj2['dept_id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Right Join

# COMMAND ----------

dfj1.join(dfj2, dfj1['dept_id']==dfj2['dept_id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ANTI join

# COMMAND ----------

dfj1.join(dfj2, dfj1['dept_id']==dfj2['dept_id'], 'anti').display()
dfj2.join(dfj1, dfj2['dept_id']==dfj1['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### ROW_NUMBER()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

df.withColumn('rowCol',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rank()

# COMMAND ----------

from pyspark.sql.functions import rank
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn('RANK_Value',rank().over(Window.orderBy('Item_Identifier'))).display()
df.withColumn(
    'RANK_Value',
    rank().over(Window.orderBy(col('Item_Identifier').desc()))
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dense_Rank

# COMMAND ----------

df.withColumn('RANK',rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('DENSE_RANK',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Cumulative SUM

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Summation order by ItemType

# COMMAND ----------

df.withColumn('Cumulative_Sum',sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cumulative with preceeding

# COMMAND ----------

df.withColumn('Cumulative_Sum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cumulative with Following Values

# COMMAND ----------

df.withColumn('Total_Sum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Defined Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step-1

# COMMAND ----------

def my_function(x ):
    return x*x


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-2
# MAGIC

# COMMAND ----------

my_udf = udf(my_function)


# COMMAND ----------

df.withColumn('Square',my_udf(col('Item_MRP'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing in CSV file

# COMMAND ----------

df.write.format('csv').save('/Volumes/databricksmaster/default/databricks/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Writing Modes

# COMMAND ----------

# MAGIC %md
# MAGIC #####Append

# COMMAND ----------

df.write.format('csv').mode('append').save('/Volumes/databricksmaster/default/databricks/CSV/data.csv')
#######
df.write.format('csv').mode('append').option('path','/Volumes/databricksmaster/default/databricks/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Overwrite

# COMMAND ----------

df.write.format('csv').mode('overwrite').option('path','/Volumes/databricksmaster/default/databricks/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Error

# COMMAND ----------

df.write.format('csv').mode('error').option('path','/Volumes/databricksmaster/default/databricks/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Ignore

# COMMAND ----------

df.write.format('csv').mode('ignore').option('path','/Volumes/databricksmaster/default/databricks/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parquet file format

# COMMAND ----------

df.write.format('parquet').mode('overwrite').option('path','/Volumes/databricksmaster/default/databricks/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table
# MAGIC

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable('my_Table')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating TempView

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_MRP >100

# COMMAND ----------

df_sql = spark.sql('select * from my_view where Item_MRP > 100')

# COMMAND ----------

df_sql.display()