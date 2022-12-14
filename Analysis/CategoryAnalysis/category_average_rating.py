from pyspark.sql.functions import col, length
import pandas as pd
from pyspark.sql import SparkSession, functions, types
import json
import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

# spark-submit category_average_rating.py output_b output_r output_u output_cat_rating


def split_categories(row):
    if(row[0] is None):
        return []
    line = row[0].split(',')
    for w in line:
        yield(w.strip().lower(),row[1])

    
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

final_schema = types.StructType([
types.StructField('category', types.StringType()), 
types.StructField('star_rating', types.DoubleType())
])


def main(inp1,inp2,inp3, output):
    # main logic starts here

    yelp_business_df = spark.read.csv(inp1, schema=business_schema)
    yelp_business_df.printSchema()
    yelp_business_df.createOrReplaceTempView('yelp_business_df')

    yelp_review_df = spark.read.csv(inp2, schema=review_schema)
    yelp_review_df.printSchema()
    yelp_review_df.createOrReplaceTempView('yelp_review_df')

    yelp_user_df = spark.read.csv(inp3, schema=user_schema)
    yelp_user_df.printSchema()
    yelp_user_df.createOrReplaceTempView('yelp_user_df')

    
    joined_data = spark.sql("""Select categories,review_star_rating from yelp_business_df inner join yelp_review_df\
                                on yelp_review_df.business_id = yelp_business_df.business_id\
                                inner join yelp_user_df on  yelp_review_df.user_id = yelp_user_df.user_id """)

    # selecting categories and stars column and converting it to rdd
    categories = joined_data.select('categories','review_star_rating').rdd.map(lambda x: (x[0],x[1]))
    
    # splitting the categories and flattening the list
    all_categories = categories.flatMap(split_categories)
    all_categories = all_categories.filter(lambda x:x is not None)
    
    # converting rdd to dataframe for outputing the final file
    full_DF = spark.createDataFrame(all_categories, schema = final_schema)
    full_DF.createOrReplaceTempView('full_DF')
    
    # calculating the average rating for the categories
    joined_data = spark.sql("""Select category,avg(star_rating) as average_rating from full_DF group by category order by average_rating desc""")

    joined_data.write.csv(output,mode='overwrite')


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