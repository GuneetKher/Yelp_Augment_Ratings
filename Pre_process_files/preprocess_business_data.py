import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, length 
import pandas as pd

# spark-submit preprocess_business_data.py business_data/* out

def main(inputs, output):
    
    yelp_business_df = spark.read.json(inputs)
    yelp_business_df = yelp_business_df.repartition(32)    
    yelp_business_df.createOrReplaceTempView('yelp_business_df')
    
    #The attribute RestaurantsTakeOut is NOT NULL for restaurants
    yelp_business_df = spark.sql("""SELECT stars, business_id, categories from yelp_business_df where attributes.RestaurantsTakeOut IS NOT NULL""")   
    yelp_business_df.write.csv(output, mode='overwrite') 

     

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)


