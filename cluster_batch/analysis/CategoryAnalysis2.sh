echo "Analysis 7: Analyze the restaurant categories based on the review count and business rating to find the most popular restaurant category"
echo "Running preprocessing scripts - Business"
spark-submit "$PWD/../../Pre_process_files/preprocess_business_data.py" data/Dataset/business_data data/Analysis/output_business
echo "Running preprocessing scripts - Review"
spark-submit "$PWD/../../Pre_process_files/preprocess_review_data.py" data/Dataset/review_data data/Analysis/output_review
echo "Running preprocessing scripts - User"
spark-submit "$PWD/../../Pre_process_files/preprocess_user_data.py" data/Dataset/user_data data/Analysis/output_user
spark-submit "$PWD/../../Analysis/CategoryAnalysis/top_categories_by_user.py" data/Analysis/output_business data/Analysis/output_review data/Analysis/output_user data/Analysis/output_cat_review_count
spark-submit "$PWD/../../Analysis/CategoryAnalysis/category_average_rating.py" data/Analysis/output_business data/Analysis/output_review data/Analysis/output_user data/Analysis/output_cat_rating
spark-submit "$PWD/../../Analysis/CategoryAnalysis/category_ranking.py" data/Analysis/output_cat_review_count data/Analysis/output_cat_rating data/Analysis/output_cat_rank