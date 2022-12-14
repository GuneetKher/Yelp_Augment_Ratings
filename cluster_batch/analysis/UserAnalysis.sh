echo "Analysis 4: What percentage of users has given how many reviews"
spark-submit "$PWD/../../Analysis/UserAnalysis/userPercentAnalysis.py" data/Dataset/user_data data/Analysis/userAnalysis

