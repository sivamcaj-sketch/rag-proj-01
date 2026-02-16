from pyspark.sql import SparkSession
from pyspark.sql.functions import transform, from_json, col, struct, map_from_entries
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

"""transform works only on array columns."""

json_string="""{
    "customer_ids": [
        {
            "id": 101,
            "name": "siva"
        },
        {
            "id": "AQ9999999SDFSD",
            "name": "Aadhar"
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
df=df.withColumn("id_map",map_from_entries(transform("customer_ids",lambda x:struct(x["id"],x["name"]))))\
    .withColumn("id",col("id_map")[""])
df.select("id_map").show(truncate=False)
df.printSchema()
spark.stop()