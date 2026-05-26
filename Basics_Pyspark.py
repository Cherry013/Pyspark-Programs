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

# df_csv = spark.read.csv('data/raw/customers.csv',header=True, inferSchema=True)
# df_json = spark.read.json('data/raw/payments.json')
# df_parquet = spark.read.parquet('data/bronze/customers')

# df_csv.show(5)
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
# customers.printSchema()
# customers.show(5, truncate=False)
customers = customers.withColumn(
    'name',
    concat_ws(' ', col('first_name'), col('last_name'))
)

customers.printSchema()

# customers.select(
#     'customer_id',
#     'name',
#     'email',
#     'Phone',
#     'signup_date',
#     'loyalty_tier'
# ).show(6, truncate=False)

customers = customers.select(
    'customer_id',
    col('name').alias('customer_name'),
    'email',
    'city',
    'signup_date',
    'loyalty_tier'
)

customers.show(10)
# customers.printSchema()
# customers.count()
# print(customers.columns)
# customers.describe().show()
# customers.explain()

customers.filter(col('email').like('%.org')).show(5)
# isNotNull is for not Null Values
customers.where(col('loyalty_tier').isNull()).show(5)
# it is used to drop Null Values
customers.dropna().show(5)
# fillna is used to fill Null values with some default values
customers.fillna({'city':'Unknown','loyalty_tier':'No Tier'}).show(5)

customers.where(col('loyalty_tier') == "No Tier").show(5)
# for filter you can use &(and) |(or) ~(not)
customers.filter((col('city') == 'Hyderabad') | (col('loyalty_tier') == "gold")).show(5)
# List based filtering
customers.filter(col('loyalty_tier').isin("bronze", 'silver', 'gold')).show(5)

# Range based filtering using between(start,end)
# ex:- df.filter(col('age).between(0,18)).show()

# String Filtering using .contains(str), .startswith("C00"), .endswith(".com")
customers.filter(col('email').contains('.com')).show(5)

# Regex-based filtering
customers.filter(col('email').rlike(r"^[A-Za-z0-9._%+-]+@+[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")).show()


spark.stop()

