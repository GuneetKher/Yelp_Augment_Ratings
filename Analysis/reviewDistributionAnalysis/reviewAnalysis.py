from pyspark.sql.functions import col, length
import pandas as pd
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# spark-submit reviewAnalysis.py /user/ksa211/Yelp/yelp_academic_dataset_review.json


def main(inputs, output_path):

    yelp_review_df = spark.read.json(inputs)
    yelp_review_df = yelp_review_df.repartition(10000)

    # Creating temporary view for SQL queries
    yelp_review_df.createOrReplaceTempView('yelp_review_df')

    # Total number of reviews
    yelp_total_review_df = spark.sql(
        "SELECT count(review_id) from yelp_review_df")

    # Number of reviews with less than 1 star
    yelp_review_1_df = spark.sql(
        "SELECT count(review_id) from yelp_review_df where stars <= 1")
    # Number of reviews with more than 1 star and less than equal to 2 stars
    yelp_review_2_df = spark.sql(
        "SELECT count(review_id) from yelp_review_df where stars <= 2 and stars > 1")
    # Number of reviews with more than 2 stars and less than equal to 3 stars
    yelp_review_3_df = spark.sql(
        "SELECT count(review_id) from yelp_review_df where stars <= 3 and stars > 2")
    # Number of reviews with more than 3 stars and less than equal to 4 stars
    yelp_review_4_df = spark.sql(
        "SELECT count(review_id) from yelp_review_df where stars <= 4 and stars > 3")
    # Number of reviews with more than 4 stars and less than equal to 5 stars
    yelp_review_5_df = spark.sql(
        "SELECT count(review_id) from yelp_review_df where stars <= 5 and stars > 4")

    count_1 = yelp_review_1_df.collect()[0][0]
    count_2 = yelp_review_2_df.collect()[0][0]
    count_3 = yelp_review_3_df.collect()[0][0]
    count_4 = yelp_review_4_df.collect()[0][0]
    count_5 = yelp_review_5_df.collect()[0][0]

    print("Number of reviews with rating less than equal to 1: ", count_1)
    print("Number of reviews with rating between 1 and 2: ", count_2)
    print("Number of reviews with rating between 2 and 3: ", count_3)
    print("Number of reviews with rating between 3 and 4: ", count_4)
    print("Number of reviews with rating between 4 and 5: ", count_5)

    percentage_1 = (count_1/yelp_total_review_df.collect()[0][0])*100
    percentage_2 = (count_2/yelp_total_review_df.collect()[0][0])*100
    percentage_3 = (count_3/yelp_total_review_df.collect()[0][0])*100
    percentage_4 = (count_4/yelp_total_review_df.collect()[0][0])*100
    percentage_5 = (count_5/yelp_total_review_df.collect()[0][0])*100

    print("Percentage of reviews with rating less than equal to 1: ", percentage_1)
    print("Percentage of reviews with rating between 1 and 2: ", percentage_2)
    print("Percentage of reviews with rating between 2 and 3: ", percentage_3)
    print("Percentage of reviews with rating between 3 and 4: ", percentage_4)
    print("Percentage of reviews with rating between 4 and 5: ", percentage_5)

    schema = types.StructType([
        types.StructField('Table', types.StringType()),
        types.StructField('Count', types.IntegerType()),
        types.StructField('Percentage', types.DoubleType()),
    ])

    data = [('Rating <= 1', count_1, percentage_1), ('Rating <= 2 and >1', count_2, percentage_2),
            ('Rating <= 3 and >2', count_3,
             percentage_3), ('Rating <= 4 and >3', count_4, percentage_4),
            ('Rating <= 5 and >4', count_5, percentage_5)]

    output = spark.createDataFrame(data, schema=schema)
    output.show(5)
    final_output = output.coalesce(1)
    final_output.write.csv(output_path, header=True)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
