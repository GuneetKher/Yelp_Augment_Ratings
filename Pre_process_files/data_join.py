import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import json
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import col, length 


# spark-submit data_join.py output_b output_r output_u output_join

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

user_schema = types.StructType([
types.StructField('review_count', types.IntegerType()), 
types.StructField('average_user_rating', types.DoubleType()),
types.StructField('user_id', types.StringType()) 
])

def main(inp1,inp2,inp3, output):
    
    df_b = spark.read.csv(inp1, schema=business_schema)
    df_b.printSchema()
    df_b.createOrReplaceTempView('df_b')

    df_r = spark.read.csv(inp2, schema=review_schema)
    df_r.printSchema()
    df_r.createOrReplaceTempView('df_r')

    df_u = spark.read.csv(inp3, schema=user_schema)
    df_u.printSchema()
    df_u.createOrReplaceTempView('df_u')

    
    joined_data = spark.sql("""SELECT df_r.user_id,df_u.review_count, average_user_rating,business_star_rating, length_of_review_text,categories,\
                            review_star_rating, review_id,df_b.business_id,review_text from df_u inner join df_r on df_r.user_id=df_u.user_id inner join df_b \
                            on df_r.business_id=df_b.business_id""")
    joined_data.write.csv(output, mode='overwrite')
    
    

if __name__ == '__main__':
    inp1 = sys.argv[1]
    inp2 = sys.argv[2]
    inp3 = sys.argv[3]
    output = sys.argv[4]
    spark = SparkSession.builder.appName('BD Project').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inp1,inp2,inp3,output)