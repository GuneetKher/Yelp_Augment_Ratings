echo "Preparing to train models"
echo "Training Multinomial Logistic Regression Classification for seen review predictions"
spark-submit "$PWD/../../Modeling/review_star_prediction/training_MNLRC.py" data/Dataset/ReviewDataset models/Logistic_regressor