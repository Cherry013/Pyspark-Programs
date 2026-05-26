import os

python_path = r"D:\CSE-A\Learning Pyspark\.venv\Scripts\python.exe"

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Test")
    .master("local[*]")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(1,'A'),(2,'B'),(3,'C')],
    ['id','Name']
)

df.show()

spark.stop()
