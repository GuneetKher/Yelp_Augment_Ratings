echo "Preparing to train models"
echo "Training Linear Regressor for seen review predictions"
spark-submit "$PWD/../../Modeling/review_star_prediction/training_LR.py" data/Dataset/ReviewDataset models/linear_regressor