from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import random

spark = SparkSession.builder.appName('job-for-pi').getOrCreate()
NUM_SAMPLES = 10000

def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

count = spark.sparkContext.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))
