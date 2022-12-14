import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, length 
import pandas as pd

# spark-submit category_ranking.py output_cat_review_count output_cat_rating output_cat_rank

count_schema = types.StructType([
types.StructField('category', types.StringType()), 
types.StructField('count', types.IntegerType())
])

rating_schema = types.StructType([
types.StructField('category', types.StringType()), 
types.StructField('rating', types.DoubleType())
])


def main(inp1,inp2, output):
    
    yelp_count_df = spark.read.csv(inp1, schema=count_schema)
    yelp_count_df.printSchema()
    yelp_count_df.createOrReplaceTempView('yelp_count_df')

    yelp_rating_df = spark.read.csv(inp2, schema=rating_schema)
    yelp_rating_df.printSchema()
    yelp_rating_df.createOrReplaceTempView('yelp_rating_df')
    
    ranked_data = spark.sql("""WITH mr (m_rating) as (select max(rating) from yelp_rating_df),\
                            mc (m_count) as (select max(count) from yelp_count_df) \
                            select yelp_count_df.category,((rating/mr.m_rating)*0.4+(count/mc.m_count)*0.6) as rank from mr,mc,yelp_count_df inner join\
                            yelp_rating_df on yelp_count_df.category = yelp_rating_df.category order by rank desc""")
    ranked_data.write.csv(output, mode='overwrite') 
   

     

if __name__ == '__main__':
    inp1 = sys.argv[1]
    inp2 = sys.argv[2]
    output = sys.argv[3]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inp1,inp2, output)


