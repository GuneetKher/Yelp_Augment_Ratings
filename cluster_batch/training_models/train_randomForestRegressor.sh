echo "Preparing to train models"
echo "Training Random Forest Regression for seen review predictions"
spark-submit "$PWD/../../Modeling/review_star_prediction/training_RFR.py" data/Dataset/ReviewDataset models/random_forest_regressor