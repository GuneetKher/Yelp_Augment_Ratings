echo "Doing sentiment analysis (make sure to install NLTK and TextBlob libraries for python [run in local])"
OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES spark-submit "$PWD/../SentimentAnalysis/review_pyspark.py" $PWD/../data/Dataset/output_join $PWD/../data/Dataset/sentiments
echo "Please run in local, this script did not work in cluster"