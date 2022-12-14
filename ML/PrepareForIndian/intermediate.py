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

def main(input_processed,input_sentiment,output_user,output_business):
    processed = spark.read.csv(input_processed, schema=processed_schema)
    sentiment = spark.read.csv(input_sentiment, schema=sentiment_schema)

    processed = processed.filter(functions.lower(processed['categories']).contains('indian'))

    # Selecting relevant columns from the datasets
    processed = processed.select('review_id','average_user_rating','business_star_rating','business_id')
    sentiment = sentiment.select('s_review_id','user_id','length_of_review_text','polarity','subjectivity')

    # Performing inner join on the two datasets and selecting relevant columns
    result = processed.join(sentiment,processed['review_id']==sentiment['s_review_id'],'inner')
    result = result.select('review_id','user_id','business_id','average_user_rating','length_of_review_text','polarity','subjectivity')
    
    # Grouping by user ID to find the average features for each user
    result_user = result.groupby('user_id').agg(
        functions.avg('length_of_review_text').alias('avg_review_len'),
        functions.first('average_user_rating'),
        functions.avg('polarity').alias('avg_polarity'),
        functions.avg('subjectivity').alias('avg_subjectivity'),
        )
    
    # Grouping by business ID to find the average features for each business
    result_business = result.groupby('business_id').agg(
        functions.avg('length_of_review_text').alias('avg_business_review_len'),
        functions.avg('polarity').alias('avg_business_polarity'),
        functions.avg('subjectivity').alias('avg_business_subjectivity'),
        )

    result_user.write.csv(output_user,compression='gzip')
    result_business.write.csv(output_business,compression='gzip')





if __name__ == '__main__':
    input_processed = sys.argv[1]
    input_sentiment = sys.argv[2]
    output_user = sys.argv[3]
    output_business = sys.argv[4]
    spark = SparkSession.builder.appName('Group data').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(input_processed,input_sentiment,output_user,output_business)

