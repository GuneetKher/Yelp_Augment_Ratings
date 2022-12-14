# Gradient Boosted Trees Regression

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('ml train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator



data_schema = types.StructType([
    types.StructField('review_count', types.IntegerType()),
    types.StructField('average_user_rating', types.FloatType()),
    types.StructField('business_star_rating', types.FloatType()),
    types.StructField('length_of_review_text', types.IntegerType()),
    types.StructField('categories', types.StringType()),
    types.StructField('review_star_rating', types.FloatType()),
    types.StructField('review_id', types.StringType()),
    types.StructField('s_review_id', types.StringType()),
    types.StructField('polarity', types.FloatType()),
    types.StructField('subjectivity', types.FloatType())
])


def main(inputs,model_file):
    data = spark.read.csv(inputs, schema=data_schema,header='True')
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    

    
    feature_assembler = VectorAssembler(
        inputCols=[
            'review_count', 
            'average_user_rating', 
            'business_star_rating',
            'length_of_review_text',
            'polarity',
            'subjectivity'
            ],
        outputCol='features'
    )

    regressor = GBTRegressor(
        featuresCol='features',
        labelCol='review_star_rating'
        )

    
    pipeline = Pipeline(stages=[feature_assembler, regressor])
    model = pipeline.fit(train)
    predictions = model.transform(validation)


    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='review_star_rating',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='review_star_rating',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)

    print('r2 score for model pre scaling: %g' % (r2, ))
    print('rmse score for model pre scaling: %g' % (rmse, ))
    
    predictions = predictions.withColumn('new_prediction',functions.round(predictions['prediction']*2)/2)
    predictions = predictions.withColumn('new_prediction',functions.when(predictions['new_prediction']>5,5).when(predictions['new_prediction']<0,0).otherwise(predictions['new_prediction']))
    predictions.show()
    
    r2_evaluator = RegressionEvaluator(
        predictionCol='new_prediction', labelCol='review_star_rating',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)

    rmse_evaluator = RegressionEvaluator(predictionCol='new_prediction', labelCol='review_star_rating',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)



    print('r2 score for model: %g' % (r2, ))
    print('rmse score for model: %g' % (rmse, ))


    model.write().overwrite().save(model_file)
 

    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
