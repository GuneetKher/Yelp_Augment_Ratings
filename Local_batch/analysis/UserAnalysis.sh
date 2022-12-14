echo "Analysis 4: What percentage of users has given how many reviews"
spark-submit "$PWD/../../Analysis/UserAnalysis/userPercentAnalysis.py" $PWD/../../data/Dataset/user_data $PWD/../../data/Analysis/userAnalysis

