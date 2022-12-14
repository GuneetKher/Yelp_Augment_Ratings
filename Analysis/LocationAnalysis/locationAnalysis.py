from pyspark.sql.functions import col, length
import pandas as pd
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# spark-submit locationAnalysis.py ../../Cluster\ Code/Yelp/yelp_academic_dataset_business.json out - Local
# spark-submit locationAnalysis.py /user/ksa211/Yelp/yelp_academic_dataset_business.json locationAnalysis - Cluster


def main(inputs, output):

    yelp_business_df = spark.read.json(inputs)
    yelp_business_df = yelp_business_df.repartition(32)

    # Creating temporary view for SQL queries
    yelp_business_df.createOrReplaceTempView('yelp_business_df')

    # reading required columns from the dataframe
    yelp_business_df = spark.sql(
        "SELECT business_id, LOWER(city) as city_lower, LOWER(state) as state_lower, stars, review_count from yelp_business_df where attributes.RestaurantsTakeOut IS NOT NULL")

    # aggregating the data by state
    group_by_state = yelp_business_df.groupBy('state_lower').agg(
        functions.count('business_id').alias('count'), functions.avg('stars').alias('average_rating'), functions.avg('review_count').alias('average_review_count')).sort('state_lower')

    # aggregating the data by state and city
    group_by_city = yelp_business_df.groupBy(['state_lower', 'city_lower']).agg(
        functions.count('business_id').alias('count'), functions.avg('stars').alias('average_rating'), functions.avg('review_count').alias('average_review_count')).sort('state_lower', 'city_lower')

    # writing the data to the output location
    group_by_state.show(5)
    group_by_city.show(5)
    group_by_state.write.csv(output+"_state", mode='overwrite')
    group_by_city.write.csv(output+"_city", mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
