echo "Analysis 6: Check the review count trend with the user count trend over the years"
spark-submit "$PWD/../../Analysis/reviewTrendAnalysis/reviewTrendAnalysis.py" data/Dataset/review_data data/Analysis/review_trend
spark-submit "$PWD/../../Analysis/userTrendAnalysis/userTrendAnalysis.py" data/Dataset/user_data data/Analysis/user_trend
spark-submit "$PWD/../../Analysis/trendsTogether/joinedTrendAnalysis.py" data/Analysis/review_trend_reviewNumberTrend data/Analysis/user_trend_userJoiningTrend data/Analysis/review_trend_reviewNumberTrendYearly data/Analysis/user_trend_userJoiningTrendYearly data/Analysis/jointAnalysis