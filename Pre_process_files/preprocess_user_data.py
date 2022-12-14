import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types

# spark-submit preprocess_business_data.py user_data out

def main(inputs, output):
    
    yelp_user_df = spark.read.json(inputs)
    yelp_user_df = yelp_user_df[['review_count','average_stars','user_id']]    
    yelp_user_df.write.csv(output, mode='overwrite')   

    

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)