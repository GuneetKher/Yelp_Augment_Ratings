echo "Preparing Datasets for ML Modeling"
echo "Working on existing review based rating prediction dataset"
spark-submit "$PWD/../ML/PrepareForReviewBased/createdata.py" $PWD/../data/Dataset/output_join $PWD/../data/Dataset/sentiments $PWD/../data/Dataset/ReviewDataset
echo "Working on unseen review based rating prediction dataset"
spark-submit "$PWD/../ML/PrepareForUserBased/intermediate.py" $PWD/../data/Dataset/output_join $PWD/../data/Dataset/sentiments $PWD/../data/Dataset/ML_userbased_user $PWD/../data/Dataset/ML_userbased_business
spark-submit "$PWD/../ML/PrepareForUserBased/joining.py" $PWD/../data/Dataset/output_join $PWD/../data/Dataset/ML_userbased_user $PWD/../data/Dataset/ML_userbased_business $PWD/../data/Dataset/ML_userbased_dataset
echo "Working on unseen review based rating prediction dataset for Indian cuisine"
spark-submit "$PWD/../ML/PrepareForIndian/intermediate.py" $PWD/../data/Dataset/output_join $PWD/../data/Dataset/sentiments $PWD/../data/Dataset/ML_Indian_user $PWD/../data/Dataset/ML_Indian_business
spark-submit "$PWD/../ML/PrepareForIndian/joining.py" $PWD/../data/Dataset/output_join $PWD/../data/Dataset/ML_Indian_user $PWD/../data/Dataset/ML_Indian_business $PWD/../data/Dataset/ML_Indian_dataset

 