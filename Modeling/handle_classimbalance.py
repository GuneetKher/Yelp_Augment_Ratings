# Gradient Boosted Trees Regression

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('ml train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.sql.window import Window	
from pyspark.sql.functions import row_number

data_schema = types.StructType([
    types.StructField('user_id', types.StringType()),
    types.StructField('business_star_rating', types.FloatType()),
    types.StructField('categories', types.StringType()),
    types.StructField('review_star_rating', types.FloatType()),
    types.StructField('review_id', types.StringType()),
    types.StructField('business_id', types.StringType()),
    types.StructField('avg_user_review_len', types.FloatType()),
    types.StructField('avg_user_rating', types.FloatType()),
    types.StructField('avg_user_polarity', types.FloatType()),
    types.StructField('avg_user_subjectivity', types.FloatType()),
    types.StructField('avg_business_review_len', types.FloatType()),
    types.StructField('avg_business_polarity', types.FloatType()),
    types.StructField('avg_business_subjectivity', types.FloatType()),
])




def main(inputs,out_train,out_test):
    data = spark.read.csv(inputs, schema=data_schema,header='True').cache()
    # data_1 = data.filter(data['review_star_rating']==1.0) #count = 615427
    # data_2 = data.filter(data['review_star_rating']==2.0) #count = 425497
    # data_3 = data.filter(data['review_star_rating']==3.0) #count = 571858
    # data_4 = data.filter(data['review_star_rating']==4.0) #count = 1198525
    # data_5 = data.filter(data['review_star_rating']==5.0) #count = 2251548

    # data_new = data_1.limit(425000)
    # data_new = data_new.union(data_2.limit(425000))
    # data_new = data_new.union(data_3.limit(425000))
    # data_new = data_new.union(data_4.limit(425000))
    # data_new = data_new.union(data_5.limit(425000))
    
    # data.createOrReplaceTempView("DATA")
    # df_train = spark.sql("SELECT * from DATA order by Rand() ")

    window = Window.partitionBy("review_star_rating").orderBy(functions.rand())
    data = data.withColumn("row_number", row_number().over(window))
    data_train = data.filter(data['row_number']<=425000)
    data_test_large = data.filter(data['row_number']>425000)
    data_test = data.filter((data['row_number']>425000) & (data['row_number']<=425497))
    # data.show()

    data_train.createOrReplaceTempView("DATA")
    data_train = spark.sql("SELECT * from DATA order by Rand() ")

    data_test.createOrReplaceTempView("DATA")
    data_test = spark.sql("SELECT * from DATA order by Rand() ")

    data_test_large.createOrReplaceTempView("DATA")
    data_test_large = spark.sql("SELECT * from DATA order by Rand() ")

    data_train.write.csv(out_train,header="True",compression="gzip")
    data_test.write.csv(out_test,header="True",compression="gzip")
    data_test_large.write.csv(out_test+"_large",header="True",compression="gzip")




    
if __name__ == '__main__':
    inputs = sys.argv[1]
    out_train = sys.argv[2]
    out_test = sys.argv[3]
    main(inputs,out_train,out_test)
