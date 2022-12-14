echo "Analysis 7: Analyze the restaurant categories based on the review count and business rating to find the most popular restaurant category"
echo "Running preprocessing scripts - Business"
spark-submit "$PWD/../../Pre_process_files/preprocess_business_data.py" $PWD/../../data/Dataset/business_data $PWD/../../data/Analysis/output_business
echo "Running preprocessing scripts - Review"
spark-submit "$PWD/../../Pre_process_files/preprocess_review_data.py" $PWD/../../data/Dataset/review_data $PWD/../../data/Analysis/output_review
echo "Running preprocessing scripts - User"
spark-submit "$PWD/../../Pre_process_files/preprocess_user_data.py" $PWD/../../data/Dataset/user_data $PWD/../../data/Analysis/output_user
spark-submit "$PWD/../../Analysis/CategoryAnalysis/top_categories_by_user.py" $PWD/../../data/Analysis/output_business $PWD/../../data/Analysis/output_review $PWD/../../data/Analysis/output_user $PWD/../../data/Analysis/output_cat_review_count
spark-submit "$PWD/../../Analysis/CategoryAnalysis/category_average_rating.py" $PWD/../../data/Analysis/output_business $PWD/../../data/Analysis/output_review $PWD/../../data/Analysis/output_user $PWD/../../data/Analysis/output_cat_rating
spark-submit "$PWD/../../Analysis/CategoryAnalysis/category_ranking.py" $PWD/../../data/Analysis/output_cat_review_count $PWD/../../data/Analysis/output_cat_rating $PWD/../../data/Analysis/output_cat_rank