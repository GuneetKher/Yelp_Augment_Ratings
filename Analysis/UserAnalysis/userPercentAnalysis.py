from pyspark.sql.functions import col, length
import pandas as pd
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# spark-submit userPercentAnalysis.py /user/ksa211/Yelp/yelp_academic_dataset_user.json


def main(inputs, output_path):

    yelp_user_df = spark.read.json(inputs)
    yelp_user_df = yelp_user_df.repartition(10000)

    # Creating temporary view for SQL queries
    yelp_user_df.createOrReplaceTempView('yelp_user_df')

    # Total number of users
    yelp_total_user_df = spark.sql("SELECT count(user_id) from yelp_user_df")

    # Number of users with less than 10 reviews
    yelp_user_10_df = spark.sql(
        "SELECT count(user_id) from yelp_user_df where review_count < 10")

    # Number of users with more than equal to 10 reviews and less than  20 reviews
    yelp_user_20_df = spark.sql(
        "SELECT count(user_id) from yelp_user_df where review_count < 20 and review_count >= 10")

    # Number of users with more than equal to 20 reviews and less than  30 reviews
    yelp_user_30_df = spark.sql(
        "SELECT count(user_id) from yelp_user_df where review_count < 30 and review_count >= 20")

    # Number of users with more than equal to 30 reviews and less than  40 reviews
    yelp_user_40_df = spark.sql(
        "SELECT count(user_id) from yelp_user_df where review_count < 40 and review_count >= 30")

    # Number of users with more than equal to 40 reviews and less than  50 reviews
    yelp_user_50_df = spark.sql(
        "SELECT count(user_id) from yelp_user_df where review_count < 50 and review_count >= 40")

    # Number of users with more than equal to 50 reviews
    yelp_user_50_plus_df = spark.sql(
        "SELECT count(user_id) from yelp_user_df where review_count >= 50")

    count_10 = yelp_user_10_df.collect()[0][0]
    count_20 = yelp_user_20_df.collect()[0][0]
    count_30 = yelp_user_30_df.collect()[0][0]
    count_40 = yelp_user_40_df.collect()[0][0]
    count_50 = yelp_user_50_df.collect()[0][0]
    count_50_plus = yelp_user_50_plus_df.collect()[0][0]

    print("Total Users: ", yelp_total_user_df.collect()[0][0])
    print("Number of users with less than 10 reviews: ", count_10)
    print("Number of users with reviews between 10 and 20: ", count_20)
    print("Number of users with reviews between 20 and 30: ", count_30)
    print("Number of users with reviews between 30 and 40: ", count_40)
    print("Number of users with reviews between 40 and 50: ", count_50)
    print("Number of users with more than 50 reviews: ", count_50_plus)

    percentage_10 = (count_10/yelp_total_user_df.collect()[0][0])*100
    percentage_20 = (count_20/yelp_total_user_df.collect()[0][0])*100
    percentage_30 = (count_30/yelp_total_user_df.collect()[0][0])*100
    percentage_40 = (count_40/yelp_total_user_df.collect()[0][0])*100
    percentage_50 = (count_50/yelp_total_user_df.collect()[0][0])*100
    percentage_50_plus = (count_50_plus/yelp_total_user_df.collect()[0][0])*100

    print("Percentage of users with less than 10 reviews: ", percentage_10)
    print("Percentage of users with reviews between 10 and 20: ", percentage_20)
    print("Percentage of users with reviews between 20 and 30: ", percentage_30)
    print("Percentage of users with reviews between 30 and 40: ", percentage_40)
    print("Percentage of users with reviews between 40 and 50: ", percentage_50)
    print("Percentage of users with more than 50 reviews: ", percentage_50_plus)

    schema = types.StructType([
        types.StructField('Table', types.StringType()),
        types.StructField('Count', types.IntegerType()),
        types.StructField('Percentage', types.DoubleType()),
    ])

    data = [('Less than 10', count_10, percentage_10), ('Between 10 and 20', count_20, percentage_20),
            ('Between 20 and 30', count_30, percentage_30), ('Between 30 and 40', count_40, percentage_40),
            ('Between 40 and 50', count_50, percentage_50), ('More than 50', count_50_plus, percentage_50_plus)]

    output = spark.createDataFrame(data, schema=schema)
    output.show(5)

    # Writing the output to a csv file
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
