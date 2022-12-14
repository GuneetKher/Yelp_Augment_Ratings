from pyspark.sql.functions import col, length
import pandas as pd
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+


# spark-submit categoryAnalysis.py /user/ksa211/Yelp/yelp_academic_dataset_business.json categoryAnalysis

def split_categories(row):
    # splitting the categories and ignoring the null values
    if row is None:
        return []
    else:
        return row.split(',')


def main(inputs, output_path):
    # main logic starts here

    # read the business data
    full_df = spark.read.json(inputs)

    # selecting only categories column and converting it to rdd to flattening the categories
    categories = full_df.select('categories').rdd.flatMap(lambda x: x)

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

    # collecting to write to the file using dataframe
    category_and_count = category_formatting.collect()

    schema = types.StructType([
        types.StructField('Category', types.StringType()),
        types.StructField('Count', types.IntegerType()),
    ])

    # creating the dataframe
    output = spark.createDataFrame(category_and_count, schema=schema)
    output.show(5)

    # writing to the file
    final_output = output.coalesce(1)
    final_output.write.csv(output_path, header=True)


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Review DF').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
