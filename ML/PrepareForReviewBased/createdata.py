"""
This code will join the ETL resulatant dataset with
the text processed dataset containing polarity and subjectivity and then 
creates two datasets which have important features grouped by business id and user id respectively.
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

sentiment_schema = types.StructType([
    types.StructField('s_review_id', types.StringType()),
    types.StructField('user_id', types.StringType()),
    types.StructField('review_text', types.StringType()),
    types.StructField('length_of_review_text', types.IntegerType()),
    types.StructField('polarity', types.FloatType()),
    types.StructField('subjectivity', types.FloatType())
])

def main(input_processed,input_sentiment,output):
    processed = spark.read.csv(input_processed, schema=processed_schema)
    sentiment = spark.read.csv(input_sentiment, schema=sentiment_schema)

    processed = processed.filter(functions.lower(processed['categories']).contains('indian'))

    # Selecting relevant columns from the datasets
    processed = processed.select('review_id','review_count','categories','average_user_rating','review_star_rating','business_star_rating','business_id')
    sentiment = sentiment.select('s_review_id','user_id','length_of_review_text','polarity','subjectivity')

    # Performing inner join on the two datasets and selecting relevant columns
    result = processed.join(sentiment,processed['review_id']==sentiment['s_review_id'],'inner')
    result = result.select('review_count','average_user_rating','business_star_rating','length_of_review_text','categories','review_star_rating','review_id','s_review_id','polarity','subjectivity')
    
    result.write.option('header',True).csv(output,compression='gzip')


if __name__ == '__main__':
    input_processed = sys.argv[1]
    input_sentiment = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('Group data').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_processed,input_sentiment,output)
