from pyspark.sql.functions import col, length
import pandas as pd
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# spark-submit reviewTrendAnalysis.py /user/ksa211/Yelp/yelp_academic_dataset_review.json


def main(inputs, output_path):
    # Read the Yelp review data
    yelp_review_df = spark.read.json(inputs)

    # Creating partitions for faster processing
    yelp_review_df = yelp_review_df.repartition(10000)

    # Reading the date and time column to get date only
    yelp_add_date = yelp_review_df.withColumn(
        'review_date', functions.split(yelp_review_df['date'], ' ').getItem(0))

    # Reading the date column to get year only
    yelp_add_year = yelp_add_date.withColumn(
        'review_year', functions.split(yelp_add_date['review_date'], '-').getItem(0))

    # Selecting the required columns and grouping by date to get the count of reviews per day and sorting by date
    yelp_required_df = yelp_add_date.select('review_id', 'review_date').groupBy(
        'review_date').agg(functions.count('review_id').alias('count')).sort('review_date')

    yelp_required_df.show(5)

    # Writing the output to a csv file
    output = yelp_required_df.coalesce(1)
    output.write.csv(output_path+'_reviewNumberTrend', header=True)

    # Selecting the required columns and grouping by date to get the count of users per year and sorting by date
    yelp_required_df_yearly = yelp_add_year.select('review_id', 'review_year').groupBy(
        'review_year').agg(functions.count('review_id').alias('count')).sort('review_year')

    yelp_required_df_yearly.show(5)

    # Writing the output to a csv file
    output = yelp_required_df_yearly.coalesce(1)
    output.write.csv(output_path+'_reviewNumberTrendYearly', header=True)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
