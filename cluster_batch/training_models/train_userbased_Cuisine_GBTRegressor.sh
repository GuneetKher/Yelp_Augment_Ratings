echo "Preparing to train models"
echo "Training Random Forest Regression for seen review predictions"
spark-submit "$PWD/../../Modeling/user_star_prediction/training_user_GBTR.py" data/Dataset/ML_Indian_dataset models/unseen_cuisine_GBT_regressor