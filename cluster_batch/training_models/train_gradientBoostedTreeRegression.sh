echo "Preparing to train models"
echo "Training GBT Regressor for seen review predictions"
spark-submit "$PWD/../../Modeling/review_star_prediction/training_GBTR.py" data/Dataset/ReviewDataset models/gbt_tree_regressor