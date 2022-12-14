echo "Preparing to train models"
echo "Training GBT Regressor for seen review predictions"
spark-submit "$PWD/../../Modeling/review_star_prediction/training_GBTR.py" $PWD/../../data/Dataset/ReviewDataset $PWD/../../models/gbt_tree_regressor