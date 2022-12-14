import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, length 
import pandas as pd

# spark-submit top_restaurant_by_state_city.py /user/ksa211/Yelp/yelp_academic_dataset_business.json output_top_rest

state_schema = types.StructType([ 
types.StructField("name",types.StringType(),True), 
types.StructField("state",types.StringType(),True), 
types.StructField("review_count",types.IntegerType(),True)
])

city_schema = types.StructType([ 
types.StructField("name",types.StringType(),True), 
types.StructField("state",types.StringType(),True), 
types.StructField("city",types.StringType(),True),
types.StructField("review_count",types.IntegerType(),True)
])

def main(inputs, output):
    
    yelp_business_df = spark.read.json(inputs)
    yelp_business_df = yelp_business_df.repartition(32)
    yelp_business_df.createOrReplaceTempView('yelp_business_df')
    
    # reading required columns from the dataframe
    yelp_business_df = spark.sql("SELECT name, LOWER(city) as city, LOWER(state) as state,review_count from yelp_business_df where attributes.RestaurantsTakeOut IS NOT NULL")
    yelp_business_df.createOrReplaceTempView('yelp_business_df')

    # aggregating the data by state
    group_by_state = spark.sql("""SELECT max(review_count) as max_review_count ,state from yelp_business_df group by state""")
    group_by_state.createOrReplaceTempView('group_by_state')
    group_by_state = spark.sql("""SELECT name, group_by_state.state, max_review_count as review_count from group_by_state inner join yelp_business_df ybd on\
                            ybd.review_count=group_by_state.max_review_count and ybd.state=group_by_state.state\
                            order by max_review_count desc, group_by_state.state, name""")

    # aggregating the data by state and city
    group_by_city = spark.sql("""SELECT max(review_count) as max_review_count ,state, city from yelp_business_df group by state,city""")
    group_by_city.createOrReplaceTempView('group_by_city')
    group_by_city = spark.sql("""SELECT name, group_by_city.state,group_by_city.city, max_review_count as review_count from group_by_city inner join yelp_business_df ybd on\
                            ybd.review_count=group_by_city.max_review_count and ybd.state=group_by_city.state and ybd.city=group_by_city.city
                            order by max_review_count desc, group_by_city.state, group_by_city.city, name limit 10""")

    # writing the data to the output location    
    group_by_state.write.option("header", "true").csv(output+"_state", mode='overwrite')
    group_by_city.write.option("header", "true").csv(output+"_city", mode='overwrite')

     

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)