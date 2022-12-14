from pyspark.sql import SparkSession, functions, types
from textblob import TextBlob
from nltk.sentiment import SentimentIntensityAnalyzer
import pyspark.sql.functions
import nltk
import json
import sys
import os
# os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
nltk.downloader.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()


# pip3 install textblob
# pip3 install nltk
# OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES spark-submit review_pyspark.py ../../Cluster\ Code/Folder

# defining the schema for the input data
observation_schema = types.StructType([
    types.StructField('user_id', types.StringType()),
    types.StructField('review_count', types.IntegerType()),
    types.StructField('average_user_rating', types.DoubleType()),
    types.StructField('business_star_rating', types.DoubleType()),
    types.StructField('length_of_review_text', types.IntegerType()),
    types.StructField('categories', types.StringType()),
    types.StructField('review_star_rating', types.DoubleType()),
    types.StructField('review_id', types.StringType()),
    types.StructField('business_id', types.StringType()),
    types.StructField('review_text', types.StringType())
])

# function to get polarity of the review text using nltk
get_polarity = functions.udf(lambda x: sia.polarity_scores(x).get('compound'), types.FloatType())

# function to get subjectivity of the review text using textblob
get_subjectivity = functions.udf(lambda x: TextBlob(x).sentiment.subjectivity, types.FloatType())


def main(inputs, output):

    # reading the input data
    full_df = spark.read.csv(inputs, schema=observation_schema)

    # selecting the required columns from the input data
    required_df = full_df.select('review_id', 'user_id', 'review_text', 'length_of_review_text')

    # adding the polarity and subjectivity columns to the dataframe
    df_with_pol_and_sub = required_df.withColumn('polarity', get_polarity(required_df['review_text'])).withColumn('subjectivity', get_subjectivity(required_df['review_text']))
    
    # writing the review sentiment data to the output path
    df_with_pol_and_sub.write.csv(output, mode='overwrite')
    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Review DF').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
