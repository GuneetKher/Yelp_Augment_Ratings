echo "Preparing Datasets for ML Modeling"
echo "Working on existing review based rating prediction dataset"
spark-submit "$PWD/../ML/PrepareForReviewBased/createdata.py" data/Dataset/output_join data/Dataset/sentiments data/Dataset/ReviewDataset
echo "Working on unseen review based rating prediction dataset"
spark-submit "$PWD/../ML/PrepareForUserBased/intermediate.py" data/Dataset/output_join data/Dataset/sentiments data/Dataset/ML_userbased_user data/Dataset/ML_userbased_business
spark-submit "$PWD/../ML/PrepareForUserBased/joining.py" data/Dataset/output_join data/Dataset/ML_userbased_user data/Dataset/ML_userbased_business data/Dataset/ML_userbased_dataset
echo "Working on unseen review based rating prediction dataset for Indian cuisine"
spark-submit "$PWD/../ML/PrepareForIndian/intermediate.py" data/Dataset/output_join data/Dataset/sentiments data/Dataset/ML_Indian_user data/Dataset/ML_Indian_business
spark-submit "$PWD/../ML/PrepareForIndian/joining.py" data/Dataset/output_join data/Dataset/ML_Indian_user data/Dataset/ML_Indian_business data/Dataset/ML_Indian_dataset

 