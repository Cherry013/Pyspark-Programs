from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col, trim, lower, upper, initcap, split, to_timestamp, current_date, current_timestamp, regexp_replace,
concat_ws
)

from pyspark.sql.types import (
StructField,StructType,StringType,
IntegerType,DoubleType
)



spark = SparkSession.builder.appName("cleaning").getOrCreate()


# df = spark.read.csv("data/vgsales.csv",header=True,inferSchema=True)
# df.show()
# df.printSchema()
game_schema = StructType([
    StructField('Rank', IntegerType(), True),
    StructField('Name', StringType(), True),
    StructField('Platform', StringType(), True),
    StructField('Year', IntegerType(), True),
    StructField('Genre', StringType(), True),
    StructField('Publisher', StringType(), True),
    StructField('NA_Sales', DoubleType(), True),
    StructField('EU_Sales', DoubleType(), True),
    StructField('JP_Sales', DoubleType(), True),
    StructField('Other_Sales', DoubleType(), True),
    StructField('Global_Sales', DoubleType(), True),
])

games = spark.read.csv('data/vgsales.csv', header=True, schema=game_schema)
# games.show()
# games.printSchema()

df = games.select(games.Rank, games.Name, games.Platform, games.Year, games.Genre, games.Publisher, games.Global_Sales)
# df.show()
# df.withColumn('Name',trim(col('Name'))).show()
# df.withColumn('Name',lower(col('Name'))).show()
# df.withColumn('Publisher',upper(col('Publisher'))).show()
# df.withColumn('Platform',initcap(col('Platform'))).show()
# df.withColumn('Name',split(col('Name'),' ')).show()


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
#
# customers.show()
# customers.printSchema()

cs = customers.withColumn('created_at',to_timestamp(col('created_at'),'yyyy-MM-dd HH:mm:ss'))
cs.printSchema()
cs.show()
cs = cs.withColumn(
    'Name',
    concat_ws(' ', col('first_name'),col('last_name'))
)

cs.printSchema()

cs = cs.withColumn('Phone',regexp_replace(col('Phone'),'[^0-9]',''))

cs.select(
    cs.customer_id,
    cs.Name,
    cs.email,
    cs.Phone,
    cs.city,
    cs.state,
    cs.signup_date,
    cs.loyalty_tier
).show()

spark.stop()