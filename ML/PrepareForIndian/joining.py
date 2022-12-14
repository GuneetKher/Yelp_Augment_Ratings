"""
This file is used to join the business dataset and the user dataset which were the output of the intermediate.py
"""

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types

# add more functions as necessary

# defining schema
processed_schema = types.StructType([
    types.StructField('user_id', types.StringType()),
    types.StructField('review_count', types.IntegerType()),
    types.StructField('average_user_rating', types.FloatType()),
    types.StructField('business_star_rating', types.FloatType()),
    types.StructField('length_of_review_text', types.IntegerType()),
    types.StructField('categories', types.StringType()),
    types.StructField('review_star_rating', types.FloatType()),
    types.StructField('review_id', types.StringType()),
    types.StructField('business_id', types.StringType()),
    types.StructField('review_text', types.StringType())
])

user_schema = types.StructType([
    types.StructField('u_user_id', types.StringType()),
    types.StructField('avg_user_review_len', types.FloatType()),
    types.StructField('avg_user_rating', types.FloatType()),
    types.StructField('avg_user_polarity', types.FloatType()),
    types.StructField('avg_user_subjectivity', types.FloatType())
])
business_schema = types.StructType([
    types.StructField('b_business_id', types.StringType()),
    types.StructField('avg_business_review_len', types.FloatType()),
    types.StructField('avg_business_polarity', types.FloatType()),
    types.StructField('avg_business_subjectivity', types.FloatType())
])

def main(inputs,input_user,input_business,output):
    processed = spark.read.csv(inputs, schema=processed_schema)
    user = spark.read.csv(input_user, schema=user_schema)
    business = spark.read.csv(input_business, schema=business_schema)

    processed = processed.join(user,user['u_user_id']==processed['user_id'],'inner')
    processed = processed.select(
        'user_id','business_star_rating','categories','review_star_rating','review_id','business_id',
        'avg_user_review_len','avg_user_rating','avg_user_polarity','avg_user_subjectivity'
        )  
    processed = processed.join(business,business['b_business_id']==processed['business_id'],'inner') 
    processed = processed.select(
        'user_id','business_star_rating','categories','review_star_rating','review_id','business_id',
        'avg_user_review_len','avg_user_rating','avg_user_polarity','avg_user_subjectivity',
        'avg_business_review_len','avg_business_polarity','avg_business_subjectivity'
        )     

    processed.write.option("header",True).csv(output,mode='overwrite',compression='gzip')
 
if __name__ == '__main__':
    inputs = sys.argv[1]
    input_user = sys.argv[2]
    input_business = sys.argv[3]
    output = sys.argv[4]
    spark = SparkSession.builder.appName('Consolidate data').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs,input_user,input_business,output)

