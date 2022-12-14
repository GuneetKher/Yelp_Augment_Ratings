from pyspark.sql.functions import col, length
import pandas as pd
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# spark-submit top_categories_by_user.py output_b output_r output_u output_cat_review_count

def split_categories(row):
    # splitting the categories and ignoring the null values
    if row is None:
        return []
    else:
        return row.split(',')

user_schema = types.StructType([
types.StructField('review_count', types.IntegerType()), 
types.StructField('average_user_rating', types.DoubleType()),
types.StructField('user_id', types.StringType()) 
])

business_schema = types.StructType([
types.StructField('business_star_rating', types.DoubleType()), 
types.StructField('business_id', types.StringType()),
types.StructField('categories', types.StringType())
])

review_schema = types.StructType([
types.StructField('review_star_rating', types.DoubleType()), 
types.StructField('user_id', types.StringType()), 
types.StructField('business_id', types.StringType()),
types.StructField('review_id', types.StringType()),
types.StructField('length_of_review_text', types.IntegerType()),
types.StructField('review_text', types.StringType())
])

category_schema = types.StructType([
types.StructField('Category', types.StringType()),
types.StructField('Count', types.IntegerType()),
])

def main(inp1,inp2,inp3, output_path):

    yelp_business_df = spark.read.csv(inp1, schema=business_schema)
    yelp_business_df.printSchema()
    yelp_business_df.createOrReplaceTempView('yelp_business_df')

    yelp_review_df = spark.read.csv(inp2, schema=review_schema)
    yelp_review_df.printSchema()
    yelp_review_df.createOrReplaceTempView('yelp_review_df')

    yelp_user_df = spark.read.csv(inp3, schema=user_schema)
    yelp_user_df.printSchema()
    yelp_user_df.createOrReplaceTempView('yelp_user_df')

    
    joined_data = spark.sql("""Select categories from yelp_business_df inner join yelp_review_df\
                                on yelp_review_df.business_id = yelp_business_df.business_id\
                                inner join yelp_user_df on  yelp_review_df.user_id = yelp_user_df.user_id""")

    # selecting only categories column and converting it to rdd to flattening the categories
    categories = joined_data.select('categories').rdd.flatMap(lambda x: x)

    # splitting the categories and flattening the list
    all_categories = categories.map(split_categories).flatMap(lambda x: x)

    # removing the white spaces and converting to lower case to make it case insensitive
    all_categories = all_categories.map(lambda x: x.strip().lower())

    # counting the categories
    all_categories = all_categories.map(lambda x: (x, 1))

    # reducing the categories to get the count
    category_count = all_categories.reduceByKey(lambda x, y: x+y)

    # sorting the categories based on the count
    category_sorted = category_count.sortBy(lambda x: x[1], ascending=False)

    # converting to required format
    category_formatting = category_sorted.map(lambda x: (x[0], x[1]))

    # creating the dataframe
    output = spark.createDataFrame(category_formatting, schema=category_schema)

    # writing to the file the top 100 restaurant categories
    output.limit(100).write.csv(output_path, mode='overwrite')


if __name__ == '__main__':
    inp1 = sys.argv[1]
    inp2 = sys.argv[2]
    inp3 = sys.argv[3]
    output = sys.argv[4]
    spark = SparkSession.builder.appName('Review DF').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inp1,inp2,inp3, output)