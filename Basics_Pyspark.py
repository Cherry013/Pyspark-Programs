from pyspark.sql import SparkSession

from pyspark.sql.functions import ( col, concat, concat_ws )

from pyspark.sql.window import Window
from pyspark.sql.types import (
StructType, StructField,
StringType, IntegerType, DoubleType, DateType, TimestampType
)


spark = SparkSession.builder\
    .appName('Basics_Pyspark')\
    .getOrCreate()

df_csv = spark.read.csv('data/raw/customers.csv',header=True, inferSchema=True)
# df_json = spark.read.json('data/raw/payments.json')
# df_parquet = spark.read.parquet('data/bronze/customers')

df_csv.show(5)
# df_json.show()
# df_parquet.show()

print(spark.version)

customer_schema = StructType([
    StructField('customer_id', StringType(), True),
    StructField('first_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('email', StringType(), True),
    StructField('Phone', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('signup_date', StringType(), True),
    StructField('created_at', StringType(), True),
    StructField('loyalty_tier', StringType(), True)
])

customers = spark.read.csv(
    'data/raw/customers.csv',
    header=True,
    schema=customer_schema
)
customers.printSchema()
customers.show(5, truncate=False)
customers = customers.withColumn(
    'name',
    concat_ws(' ', col('first_name'), col('last_name'))
)

customers.select(
    'customer_id',
    'name',
    'email',
    'Phone',
    'signup_date',
    'loyalty_tier'
).show(6, truncate=False)

customers_selected = customers.select(
    'customer_id',
    'name',
    'email',
    'signup_date',
    'loyalty_tier'
)

customers_selected.show(10)

spark.stop()

