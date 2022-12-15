# MultiLayer Perceptron Classification
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('ml train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # make sure we have Spark 2.4+

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler

from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


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
    types.StructField('row_number', types.FloatType())
])



def main(inputs,model_file):
    data = spark.read.csv(inputs, schema=data_schema,header='True')
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    

    
    feature_assembler = VectorAssembler(
        inputCols=[
            'business_star_rating',
            'avg_user_rating',
            'avg_user_polarity',
            'avg_user_subjectivity',
            'avg_business_polarity',
            'avg_business_subjectivity'
            ],
        outputCol='features'
    )

    classifier = MultilayerPerceptronClassifier(
    layers=[6,60,40, 6],
    featuresCol='features',
    labelCol='review_star_rating'
    )

    pipeline = Pipeline(stages=[feature_assembler, classifier])
    model = pipeline.fit(train)
    predictions = model.transform(validation)
    predictions.show()

    evaluator = MulticlassClassificationEvaluator(
        predictionCol='prediction', labelCol='review_star_rating'
    )
    score = evaluator.evaluate(predictions)

    print('Validation score for model: %g' % (score, ))

    model.write().overwrite().save(model_file)

    
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
