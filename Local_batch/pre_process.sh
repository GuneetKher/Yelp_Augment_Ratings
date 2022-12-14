echo "Running preprocessing scripts"
echo "Running preprocessing scripts - Business"
spark-submit "$PWD/../Pre_process_files/preprocess_business_data.py" "$PWD/../data/Dataset/business_data" $PWD/../data/Dataset/output_business
echo "Running preprocessing scripts - Review"
spark-submit $PWD/../Pre_process_files/preprocess_review_data.py $PWD/../data/Dataset/review_data $PWD/../data/Dataset/output_review
echo "Running preprocessing scripts - User"
spark-submit $PWD/../Pre_process_files/preprocess_user_data.py $PWD/../data/Dataset/user_data $PWD/../data/Dataset/output_user
echo "Running preprocessing scripts - Joining"
spark-submit $PWD/../Pre_process_files/data_join.py $PWD/../data/Dataset/output_business $PWD/../data/Dataset/output_review $PWD/../data/Dataset/output_user $PWD/../data/Dataset/output_join
echo "Finished Pre processing"
read -n 1 -p "Press any key to continue" "user_finish"