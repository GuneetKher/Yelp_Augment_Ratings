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




def main(inputs,model_file):
    data = spark.read.csv(inputs, schema=data_schema,header='True')
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    

    
    feature_assembler = VectorAssembler(
        inputCols=[
            'business_star_rating',
            # 'avg_user_review_len',
            'avg_user_rating',
            'avg_user_polarity',
            'avg_user_subjectivity',
            # 'avg_business_review_len',
            'avg_business_polarity',
            'avg_business_subjectivity'
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

    predictions.cache()

    # Validation testing without rounding to rating scale

    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='review_star_rating',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='review_star_rating',
            metricName='rmse')


    rmse = rmse_evaluator.evaluate(predictions)


    print('r2 score for model without rounding to rating scale: %g' % (r2, ))
    print('rmse score for model without rounding to rating scale: %g' % (rmse, ))

    # Validation testing after rounding to rating scale
    predictions = predictions.withColumn('prediction',functions.when(predictions['prediction']>5,5).when(predictions['prediction']<0,0).otherwise(predictions['prediction']))
    predictions = predictions.withColumn('new_prediction',functions.round(predictions['prediction']*2)/2)

    r2_evaluator = RegressionEvaluator(
        predictionCol='new_prediction', labelCol='review_star_rating',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)

    rmse_evaluator = RegressionEvaluator(predictionCol='new_prediction', labelCol='review_star_rating',
            metricName='rmse')


    rmse = rmse_evaluator.evaluate(predictions)


    print('r2 score for model after rounding to rating scale: %g' % (r2, ))
    print('rmse score for model after rounding to rating scale: %g' % (rmse, ))


    # Validation testing after predicting a rating range
    predictions = predictions.withColumn('new_prediction_upper',functions.ceil(predictions['prediction']*2)/2)
    predictions = predictions.withColumn('new_prediction_lower',functions.floor(predictions['prediction']*2)/2)

    # predictions.show()

    evaluation = predictions.select('review_star_rating','new_prediction_upper','new_prediction_lower')
    evaluation.cache()

    positive = evaluation.filter((evaluation['review_star_rating'] == evaluation['new_prediction_lower']) | (evaluation['review_star_rating'] == evaluation['new_prediction_upper']))
    print('Accuracy after predicting a rating range = ',positive.count()/evaluation.count())
    



    model.write().overwrite().save(model_file)
 

    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
