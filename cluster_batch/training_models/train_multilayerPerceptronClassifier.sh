echo "Preparing to train models"
echo "Training MLP classifier for seen review predictions"
spark-submit "$PWD/../../Modeling/review_star_prediction/training_MLPC.py" data/Dataset/ReviewDataset models/MLP_classifier