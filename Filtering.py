from pyspark.sql  import SparkSession

from pyspark.sql.types import (
StructType,  StructField,
IntegerType, FloatType, DecimalType, StringType, DoubleType, DateType, TimestampType
)

from pyspark.sql.functions import (
col, concat_ws, concat
)

spark = SparkSession.builder.appName("Cleaning").getOrCreate()

df = spark.read.csv("data/vgsales.csv",header=True, inferSchema=True)
df.printSchema()

games_schema = StructType([
    StructField('Rank', IntegerType(), True),
    StructField('Name', StringType(), True),
    StructField('Platform', StringType(), True),
    StructField('Year', IntegerType(), True),
    StructField('Genre', StringType(), True),
    StructField('Publisher', StringType(), True),
    StructField('NA_Sales', DoubleType(), True),
])

games = spark.read.csv('data/vgsales.csv', header=True, schema=games_schema)
games.printSchema()
games.show()

games = games.select(games.Rank, col('Name'), games.Publisher, col('Year'), col('Genre'), col('Platform'), col('NA_Sales'))
games.printSchema()
games.show()


