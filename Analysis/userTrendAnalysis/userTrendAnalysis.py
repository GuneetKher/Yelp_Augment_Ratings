from pyspark.sql.functions import col, length
import pandas as pd
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# spark-submit userTrendAnalysis.py /user/ksa211/Yelp/yelp_academic_dataset_user.json


def main(inputs, output_path):

    # Read the Yelp user data
    yelp_user_df = spark.read.json(inputs)

    # Creating partitions for faster processing
    yelp_user_df = yelp_user_df.repartition(10000)

    # Reading the date and time column to get date only
    yelp_add_date = yelp_user_df.withColumn('joining_date', functions.split(
        yelp_user_df['yelping_since'], ' ').getItem(0))

    yelp_add_year = yelp_add_date.withColumn(
        'joining_year', functions.split(yelp_add_date['joining_date'], '-').getItem(0))

    # Selecting the required columns and grouping by date to get the count of users per day and sorting by date
    yelp_required_df = yelp_add_date.select('user_id', 'joining_date').groupBy(
        'joining_date').agg(functions.count('user_id').alias('count')).sort('joining_date')

    yelp_required_df.show(5)

    # Writing the output to a csv file
    output = yelp_required_df.coalesce(1)
    output.write.csv(output_path+'_userJoiningTrend', header=True)

    # Selecting the required columns and grouping by date to get the count of users per year and sorting by date
    yelp_required_df_yearly = yelp_add_year.select('user_id', 'joining_year').groupBy(
        'joining_year').agg(functions.count('user_id').alias('count')).sort('joining_year')

    yelp_required_df_yearly.show(5)

    # Writing the output to a csv file
    output = yelp_required_df_yearly.coalesce(1)
    output.write.csv(output_path+'_userJoiningTrendYearly', header=True)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
