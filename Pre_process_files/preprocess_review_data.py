import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, length 

# spark-submit preprocess_review_data.py review_data out

# UDF to calculate text length
@functions.udf(returnType=types.IntegerType())
def length_calculator(text):
    return len(text)


def main(inputs, output):
        
    yelp_review_df = spark.read.json(inputs)
    yelp_review_df = yelp_review_df.withColumn('length',length_calculator(yelp_review_df['text']))
    yelp_review_df = yelp_review_df[['stars','user_id','business_id','review_id','length','text']]   
    yelp_review_df.write.csv(output, mode='overwrite')

        

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)