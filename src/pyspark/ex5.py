from pyspark.sql import SparkSession
from pyspark.sql.functions import transform, from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

"""transform works only on array columns."""

json_string="""{
    "customer_ids": [
        {
            "id": 1,
            "name": "siva"
        },
        {
            "id": 2,
            "name": "ramarao"
        },
        {
            "id": 3,
            "name": "durga"
        },
        {
            "id": 4,
            "name": "deechu"
        },
        {
            "id": 5,
            "name": "pardhu"
        },
        {
            "id": 6,
            "name": "nandhu"
        }
    ]
}"""

schema = StructType([StructField("customer_ids",ArrayType(StructType([StructField("id", IntegerType(), True), StructField("name", StringType(), True),])))])
spark = SparkSession.builder.getOrCreate()
# Convert string into RDD
rdd = spark.sparkContext.parallelize([json_string])
df=spark.read.json(rdd)
df.printSchema()
df.show(truncate=False)

spark.stop()