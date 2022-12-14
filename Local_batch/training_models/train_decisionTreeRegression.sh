echo "Preparing to train models"
echo "Training Decision Tree Regressor for seen review predictions"
spark-submit "$PWD/../../Modeling/review_star_prediction/training_DCTR.py" $PWD/../../data/Dataset/ReviewDataset $PWD/../../models/decision_tree_regressor