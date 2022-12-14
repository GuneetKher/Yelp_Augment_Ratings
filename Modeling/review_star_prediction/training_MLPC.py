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

    classifier = MultilayerPerceptronClassifier(
    layers=[6, 60, 11],
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
