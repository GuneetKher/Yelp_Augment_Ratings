echo "Analysis 5: Count the number of different review ratings present in the dataset"
spark-submit "$PWD/../../Analysis/reviewDistributionAnalysis/reviewAnalysis.py" data/Dataset/review_data data/Analysis/ratingAnalysis
