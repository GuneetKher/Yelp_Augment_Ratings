from pyspark.sql.functions import col, length
import pandas as pd
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# spark-submit joinedTrendAnalysis.py /user/ksa211/reviewNumberTrend /user/ksa211/userJoiningTrend /user/ksa211/reviewNumberTrendYearly /user/ksa211/userJoiningTrendYearly

review_schema = types.StructType([
    types.StructField('review_date', types.DateType()),
    types.StructField('review_count', types.IntegerType())
])

user_schema = types.StructType([
    types.StructField('user_date', types.DateType()),
    types.StructField('user_count', types.IntegerType())
])

review_schema_yearly = types.StructType([
    types.StructField('review_year', types.DateType()),
    types.StructField('review_count', types.IntegerType())
])

user_schema_yearly = types.StructType([
    types.StructField('user_year', types.DateType()),
    types.StructField('user_count', types.IntegerType())
])


def main(review, user, review_yearly, user_yearly, output):

    # Read the review data from previous analysis
    review_df = spark.read.csv(review, schema=review_schema)

    # Creating temporary view for SQL queries
    review_df.createOrReplaceTempView('review_df')

    # Read the user data from previous analysis
    user_df = spark.read.csv(user, schema=user_schema)

    # Creating temporary view for SQL queries
    user_df.createOrReplaceTempView('user_df')

    # Read the review data from previous analysis
    review_yearly_df = spark.read.csv(
        review_yearly, schema=review_schema_yearly)

    # Creating temporary view for SQL queries
    review_yearly_df.createOrReplaceTempView('review_yearly_df')

    # Read the user data from previous analysis
    user_yearly_df = spark.read.csv(user_yearly, schema=user_schema_yearly)

    # Creating temporary view for SQL queries
    user_yearly_df.createOrReplaceTempView('user_yearly_df')

    # Joining the dataframes
    joining_df = spark.sql(
        """SELECT review_df.review_date, review_df.review_count, user_df.user_count from review_df inner join user_df on review_df.review_date=user_df.user_date""")

    joining_df.show(5)

    # Writing the output to a csv file
    joining_df.write.csv('joinedTrend', header=True)

    # Joining the dataframes
    joining_yearly_df = spark.sql(
        """SELECT review_yearly_df.review_year, review_yearly_df.review_count, user_yearly_df.user_count from review_yearly_df inner join user_yearly_df on review_yearly_df.review_year=user_yearly_df.user_year""")

    joining_yearly_df.show(5)

    joining_yearly_df.createOrReplaceTempView('joining_yearly_df')

    # Query to get cumulative sum of reviews and users yearly
    join_year_cumulative = spark.sql(
        """SELECT review_year, review_count, user_count, sum(review_count) OVER (ORDER BY review_year) as review_cumulative, sum(user_count) OVER (ORDER BY review_year) as user_cumulative from joining_yearly_df""")

    join_year_cumulative.show(5)

    # Writing the output to a csv file
    join_year_cumulative.write.csv(output, header=True)


if __name__ == '__main__':
    review = sys.argv[1]
    user = sys.argv[2]
    review_y = sys.argv[3]
    user_y = sys.argv[4]
    output = sys.argv[5]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(review, user, review_y, user_y, output)
