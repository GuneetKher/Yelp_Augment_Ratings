Please refer RUNNING.md for execution instructions 
# Folders and Files:
1) Analysis : <br />
- CategoryAnalysis : Analysis of the business categories <br />
category_average_rating.py : Finding the average rating of each business category <br />
top_category_by_user.py : Finding the top 100 preferred business category by users based on the reviews <br />
category_ranking.py : Finding the top business category using weighted average on category average rating  (category_average_rating.py) and user average rating (category_ranking.py) <br />
categoryAnalysis : Finding the frequency of different business categories <br />
 
- LocationAnalysis : Location based Analysis <br />
locationAnalysis.py : Finding state and city-wise total restaurant counts, their average star rating and avergage review counts <br />
top_restaurant_by_state_city.py : Sorting state and city-wise restaurants based on the number of reviews <br />
 
- reviewDistributionAnalysis/reviewAnalysis.py : Counting the frequency of review ratings <br />
 
- reviewTrendAnalysis/reviewTrendAnalysis.py : Finding the review count trend yearly and daily <br />
 
- trendsTogether/joinedTrendAnalysis.py : Joining review trend output (reviewTrendAnalysis.py) and user trend output (userTrendAnalysis) on year-wise and date-wise separately <br />
 
- UserAnalysis/userPercentAnalysis.py : Finding what percentage of users has given how many reviews <br />
 
- userTrendAnalysis/userTrendAnalysis.py : Finding the user count trend yearly and daily <br />

2) ML :- <br />
- PrepareForReviewBased: Consists of scripts that will create the dataset on which we will train models to predict review star ratings, based on existing review data.
- PrepareForUserBased: Consists of scripts that will create the dataset on which we will train GBT Model to predict review star ratings, based on user - business pairing. (without looking at review)
- PrepareForIndian: Consists of scripts that will create the dataset on which we will train GBT Model to predict review star ratings, based on user - business pairing, only for Indian cuisine businesses. (without looking at reviews)

3) Modeling :- <br />
- review_star_prediction: Consists of scripts that will train models for predicting review rating based on existing reviews
- user_star_prediction: Consists of scripts that will train models for predicting review ratings based on user-business pairings.

4) Pre_process_files : Preprocessing of the raw data <br />
   preprocessing_business_data.py : Selecting star rating, business_id and categories filtering by restaurants only from business data <br />
   preprocess_review_data.py : Selecting review rating, user_id, business_id, review_id, review and finding review length which is added as a new column from review data <br />
   preprocess_user_data.py : Selecting review count, average star and user_id from user data <br />
   data_join.py : Joining all the three preprocessed output files on user_id and business_id <br />

5) SentimentAnalysis/review_pyspark.py : Reading joined preprocessed data (data_join.py) and adding polarity and subjectivity of review as two new columns <br />

6) Tableau Visualization : Visualization of all the analysis based on categoreis, location, reviews and users
7) Local_batch : This folder contains shell scripts for running the project. (We did this so there is ease in testing and no confusion of output paths which will be read in later scripts as input)
8) cluster_batch : This folder contains shell scripts for running the project from the cluster gateway. Before running, please make sure to move the whole project folder to gateway and place the dataset in the hadoop cluster in a folder named "data".

# Problem Definition:
The Yelp academic dataset is a subset of business, user, and review data compiled by Yelp for educational purposes. As part of our project, we have performed a prediction of the star rating a user might give to a restaurant using various features. With this, we can recommend users to businesses who are more likely to give higher ratings to their business to help increase the overall business star rating. The star rating of a business has a lot of influence on customers’ decision to choose a restaurant. The better the rating of a restaurant, the more the footfall will be, which leads to better profits. 

The main objective of our project is to help businesses(restaurants) gain traction by approaching users who are more likely to provide them with a rating. In addition, we explore other trends that would help budding entrepreneurs to find a potentially profitable location for a new business to open.

# About the Dataset:
We have chosen the following 3 datasets out of the 5 provided by Yelp:
1. Business data
2. Review data
3. User data
The datasets which can be downloaded have 150 K businesses, 6.9M reviews and 1.9M users. And there are several businesses such as restaurants, home services and shopping. But we chose restaurant reviews for analysis and prediction primarily as they constitute about 35% of the total businesses.

# Methodology:
We are using PySpark to do the project because Python is a popular choice for statistical analysis and it supports most Spark features like Spark SQL, Dataframes and ML in a distributed environment. The Big Data Lab cluster has been used to run the code and get the results as the size of the data set is 8GB overall the cluster has more parallel processing power with larger memory. We were able to perform meaningful aggregations of data, and extract features needed for prediction and analysis.

The feature we are using in the prediction model requires sentiment analysis over the review text data. To get the sentiments (polarity and subjectivity), we used Python’s Natural Language Toolkit(NLTK) library. Since we don’t have any labeled data to get the polarity and subjectivity of the review text, we used the prebuilt model provided by the library to get the scores. These scores are incorporated as features in the model prediction to get better accuracy.
 
To build the prediction model, we used MLlib which is Apache Spark’s scalable machine learning library. The goal of MLlib is to make practical machine learning scalable and easy. We did various types of analysis on the dataset to understand the distribution of the dataset, find users and review trends etc. All this is being visualized using Tableau which is an analytics platform. It is a powerful and easy-to-use platform for data management, exploration and visualization.

# Analysis:
We are doing different types of analysis to understand how the dataset is distributed, and what are the trends over the last 15-20 years of the dataset. Below is the list of these.
1. Count the frequency of different types of categories present like restaurants, shopping, beauty & spa. This will be used to decide for which category we have more data. This helps us to decide which business will be used for prediction and further analysis.
2. Analyze state-wise and city-wise restaurant count, their average star rating and average review count. This will be useful to find the popular food hubs and how good they are based on their star rating.
3. Sorting state-wise and city-wise restaurants based on the number of reviews. This will help to know the most reviewed and popular restaurants which can be shown as recommendations to customers based on their location.
4. Analyze what percentage of users have given how many reviews. This helps to understand how often a user writes a review. We will be using a range instead of discrete values for the number of reviews for better visualization.
5. Analyze the count of different reviews rating present in the review dataset. This will help to understand the distribution of our reviews rating and whether the dataset is skewed for a particular type of rating.
6. Analyze the growth of the review and user trend over different years. On joining both the trends, it helps to understand whether the review count is increasing as the number of users joining is increasing. 
7. Analyze the restaurant categories based on the review count and business rating to find the most popular restaurant category. This helps to decide the kind of restaurant one can start. But the conundrum is, the more popular category will have more competition which might affect the profits, yet the chances of business success are more. If one chooses to open a restaurant least preferred, then we will be targeting a specific group of customers which could lead to unsatisfactory profits. Here the competition will be less but the chances of business failure are more. Hence we can suggest more preferred restaurant categories in areas with fewer restaurants of that particular category.

# Prediction:
Using Machine Learning we have first tried to identify if there is a learnable pattern within the historical reviews data by training a model which predicts the rating of an already existing review. The model uses several input features that we obtained after performing data manipulation on the publicly available Yelp dataset. Polarity refers to the sentiment [-1,1] and subjectivity refers to the degree of the review being a personal opinion [0,1]. Using these features, we predicted the target label rating by employing multiple ML models. <br />
The best scores were obtained using GBT-Regressor. One catch in the prediction was that it was a continuous variable, however, ratings are inherently a variable between [0,5] with steps of 0.5. To make our prediction appropriate, we ran our predictions through a layer of logic which scaled the values to acceptable values. However, from a business perspective, it does not help much to predict the rating of a review which already exists. So next we extracted user-specific details and business-specific details. Once we had specific user and business attributes, we joined these on review_id of our reviews dataset and trained a model which only used the user and business specific attributes to predict a possible rating between a matching of the two entities. And this model now could predict a review rating apriori to any review. The output from this model was then passed through our scaling layer to get the final output. 

# Problems:
1. We wanted to suggest newcomers with regions(cities) in different states where they have the potential of opening a restaurant. But the restaurants are mostly populated in the state of Pennsylvania (PA) and Florida (FL) which hinders our scope of going country-wide.
2. The target label for the prediction model has values within the set {0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.5, 4.0, 4.5, 5}; however, since we used a regression model, the predictions were a continuous variable (for eg. 4.323), which could not be accepted as a valid rating.

# Results:
Using the GBT-Regressor, we were able to map an existing review to a predicted review star with the best relative accuracy of all the models we tested. Further, after modeling the data around user-specific attributes and business-specific attributes, we created a dataset which allowed us to train a model which could make predictions about the potential review rating for a pair of user-business entities with an R2 score of  ̴ 0.43. We further improved this score to ̴ 0.48 by creating a dataset specific to one food category (‘Indian’) and training the model for one category.
