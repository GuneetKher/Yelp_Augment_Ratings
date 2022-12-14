# How to Run the Project

From the raw Yelp dataset, 3 files are being used for all the analysis and predictions:

1. yelp_academic_dataset_business
2. yelp_academic_dataset_review
3. yelp_academic_dataset_user

**NOTE: The dataset can be found on <a href="https://drive.google.com/drive/folders/1rdIo-IQc7CS0iMacEAIUgqpFCwtXXq7m">Google Drive</a> and also in the cluster gateway in gka89's account `/home/gka89` with a folder name "Dataset"**

# Commands to run on Local:
**If using windows, please run `sh ./prepare_script_files.sh` from the "Local_batch" directory before proceeding.**
### Preprocessing the dataset (ETL)-
![folder structure](/folder-structure.png "Folder Structure")
**NOTE:** Place the extracted dataset in the folder named "data" in the root directory of project. Such that the folder structure is like `GKS_BigData/data/Dataset` <br/>
**NOTE:** Run the following commands from within the "Local_batch" folder only. <br/>
1) Execute the shell script named pre_process.sh in the "Local_batch" folder as `sh ./pre_process.sh` <br />
Running this script will create the following new directories within the data folder: output_business, output_review, output_user and output_join. <br/>
Schema:-
<table>
<thead>
  <tr>
    <th>output_business</th>
    <th>output_user</th>
    <th>output_review</th>
    <th>output_join</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>business_star_rating</td>
    <td>review_count</td>
    <td>review_star_rating</td>
    <td>user_id</td>
  </tr>
  <tr>
    <td>business_ID</td>
    <td>avg_user_rating</td>
    <td>user_ID</td>
    <td>review_count</td>
  </tr>
  <tr>
    <td>categories</td>
    <td>user_ID</td>
    <td>business_ID</td>
    <td>avg_user_rating</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>review_ID</td>
    <td>business_star_rating</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>length_of_review_text</td>
    <td>length_of_review_text</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>review_text</td>
    <td>categories</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>review_star_rating</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>review_ID</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>business_ID</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>review_text</td>
  </tr>
</tbody>
</table>

### Creating Sentiment analysis Dataset-
**Make sure NLTK and TextBlob modules are installed**
**This script needs to run in local, as it requires some config which we unable to get working on cluster**
  **We have included the output of this step <a href="https://drive.google.com/drive/folders/1rdIo-IQc7CS0iMacEAIUgqpFCwtXXq7m">here (sentiments.zip)</a>, incase there are any issues executing on your machines. Please download and place this extracted file in `GKS_BigData/data/Dataset` directory if you downloaded it.**
1) Execute the shell script named "sentiment_analysis.sh" in the folder "Local_batch" as `sh ./sentiment_analysis.sh` <br />
Running this script will create the following dataset. <br />
Schema:-
<table>
<thead>
  <tr>
    <th>sentiments</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>review_id</td>
  </tr>
  <tr>
    <td>user_id</td>
  </tr>
  <tr>
    <td>review_text</td>
  </tr>
  <tr>
    <td>length_of_review_text</td>
  </tr>
  <tr>
    <td>polarity</td>
  </tr>
  <tr>
    <td>subjectivity</td>
  </tr>
</tbody>
</table>


### Creating Training Datasets-
1) Execute the shell script named "dataset_prep.sh" in the folder "Local_batch" as `sh ./dataset_prep.sh` <br />
Running this script will create the following datasets. <br />
Schema:-
<table>
<thead>
  <tr>
    <th>ReviewDataset</th>
    <th>ML_userbased_business</th>
    <th>ML_userbased_user</th>
    <th>ML_userbased_dataset</th>
    <th>ML_Indian_business</th>
    <th>ML_Indian_user</th>
    <th>ML_Indian_dataset</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>review_count</td>
    <td>b_business_id</td>
    <td>u_user_id</td>
    <td>user_id</td>
    <td>b_business_id</td>
    <td>u_user_id</td>
    <td>user_id</td>
  </tr>
  <tr>
    <td>avg_user_rating</td>
    <td>avg_business_review_len</td>
    <td>avg_user_review_len</td>
    <td>business_star_rating</td>
    <td>avg_business_review_len</td>
    <td>avg_user_review_len</td>
    <td>business_star_rating</td>
  </tr>
  <tr>
    <td>business_star_rating</td>
    <td>avg_business_polarity</td>
    <td>avg_user_rating</td>
    <td>categories</td>
    <td>avg_business_polarity</td>
    <td>avg_user_rating</td>
    <td>categories</td>
  </tr>
  <tr>
    <td>length_of_review_text</td>
    <td>avg_business_subjectivity</td>
    <td>avg_user_polarity</td>
    <td>review_star_rating</td>
    <td>avg_business_subjectivity</td>
    <td>avg_user_polarity</td>
    <td>review_star_rating</td>
  </tr>
  <tr>
    <td>categories</td>
    <td></td>
    <td>avg_user_subjectivity</td>
    <td>review_id</td>
    <td></td>
    <td>avg_user_subjectivity</td>
    <td>review_id</td>
  </tr>
  <tr>
    <td>review_star_rating</td>
    <td></td>
    <td></td>
    <td>business_id</td>
    <td></td>
    <td></td>
    <td>business_id</td>
  </tr>
  <tr>
    <td>review_id</td>
    <td></td>
    <td></td>
    <td>avg_user_review_len</td>
    <td></td>
    <td></td>
    <td>avg_user_review_len</td>
  </tr>
  <tr>
    <td>s_review_id</td>
    <td></td>
    <td></td>
    <td>avg_user_rating</td>
    <td></td>
    <td></td>
    <td>avg_user_rating</td>
  </tr>
  <tr>
    <td>polarity</td>
    <td></td>
    <td></td>
    <td>avg_user_polarity</td>
    <td></td>
    <td></td>
    <td>avg_user_polarity</td>
  </tr>
  <tr>
    <td>subjectivity</td>
    <td></td>
    <td></td>
    <td>avg_user_subjectivity</td>
    <td></td>
    <td></td>
    <td>avg_user_subjectivity</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>avg_business_review_len</td>
    <td></td>
    <td></td>
    <td>avg_business_review_len</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>avg_business_polarity</td>
    <td></td>
    <td></td>
    <td>avg_business_polarity</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>avg_business_subjectivity</td>
    <td></td>
    <td></td>
    <td>avg_business_subjectivity</td>
  </tr>
</tbody>
</table>

### Training Prediction Models-
1) We have placed several shell scripts in directory `Local_batch -> training_models` <br/>
You can run them in any order as they are independant. The files are named with the respective model they will train.
For eg. training GBT regressor for review based prediction from inside the "Local_batch/training_models" directory as <br/>
`sh ./train_gradientBoostedTreeRegression.sh` <br/>
Each script will save the respective model in a folder named "models" in the project root. <br/>
**Each script will display the validation scores at the end of execution**

### Commands for the execution of Data Analysis.
1) We have placed several analysis shell scripts in the directory `Local_batch -> analysis` <br/>
You can run them in any order as they are independant. The files are named with the respective analysis. The output of the data analysis (CSV) will be placed in their respective folders inside the "data/analysis" folder from the root of the project. <br/>
For eg. For CategoryAnalysis1, run this command from inside the "Local_batch/analysis" directory `sh ./CategoryAnalysis1.sh` <br/>
  This script will create a folder here `GKS_BIGDATA/data/Analysis/categoryAnalysis`

These CSV are then visualized in Tableau, you can find the visualizations <a href="https://public.tableau.com/app/profile/sathish.kumar.alagarsamy/viz/CityLocationAnalysis/CategoryAnalysis?publish=yes">here</a>


# Commands to run on Cluster:
### Preprocessing the dataset (ETL)- <br/>
**NOTE:** Place the extracted dataset in a folder named "data" in the hadoop cluster. Such that the folder structure is like `data/Dataset`.<br/>
**NOTE:** Move the whole project directory to the gateway using `scp -r {localpath to project} {cluster connection}`. <br/>
**NOTE:** Run the following commands from within the "cluster_batch" folder only. <br/>
1) Execute the shell script named pre_process.sh in the "cluster_batch" folder as `sh ./pre_process.sh` <br />
Running this script will create the following new directories within the data folder: output_business, output_review, output_user and output_join. <br/>
Schema:-
<table>
<thead>
  <tr>
    <th>output_business</th>
    <th>output_user</th>
    <th>output_review</th>
    <th>output_join</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>business_star_rating</td>
    <td>review_count</td>
    <td>review_star_rating</td>
    <td>user_id</td>
  </tr>
  <tr>
    <td>business_ID</td>
    <td>avg_user_rating</td>
    <td>user_ID</td>
    <td>review_count</td>
  </tr>
  <tr>
    <td>categories</td>
    <td>user_ID</td>
    <td>business_ID</td>
    <td>avg_user_rating</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>review_ID</td>
    <td>business_star_rating</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>length_of_review_text</td>
    <td>length_of_review_text</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td>review_text</td>
    <td>categories</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>review_star_rating</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>review_ID</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>business_ID</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>review_text</td>
  </tr>
</tbody>
</table>

### Creating Sentiment analysis Dataset-
**Make sure NLTK and TextBlob modules are installed**
**This script needs to run in local, as it requires some config which we unable to get working on cluster**
  **We have included the output of this step <a href="https://drive.google.com/drive/folders/1rdIo-IQc7CS0iMacEAIUgqpFCwtXXq7m">here (sentiments.zip)</a>, incase there are any issues executing on your machines. Please download and place this extracted file in `GKS_BigData/data/Dataset` directory if you downloaded it.**
1) Execute the shell script named "sentiment_analysis.sh" in the folder "cluster_batch" as `sh ./sentiment_analysis.sh` <br />
Running this script will create the following dataset. <br />
Schema:-
<table>
<thead>
  <tr>
    <th>sentiments</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>review_id</td>
  </tr>
  <tr>
    <td>user_id</td>
  </tr>
  <tr>
    <td>review_text</td>
  </tr>
  <tr>
    <td>length_of_review_text</td>
  </tr>
  <tr>
    <td>polarity</td>
  </tr>
  <tr>
    <td>subjectivity</td>
  </tr>
</tbody>
</table>


### Creating Training Datasets-
1) Execute the shell script named "dataset_prep.sh" in the folder "cluster_batch" as `sh ./dataset_prep.sh` <br />
Running this script will create the following datasets. <br />
Schema:-
<table>
<thead>
  <tr>
    <th>ReviewDataset</th>
    <th>ML_userbased_business</th>
    <th>ML_userbased_user</th>
    <th>ML_userbased_dataset</th>
    <th>ML_Indian_business</th>
    <th>ML_Indian_user</th>
    <th>ML_Indian_dataset</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>review_count</td>
    <td>b_business_id</td>
    <td>u_user_id</td>
    <td>user_id</td>
    <td>b_business_id</td>
    <td>u_user_id</td>
    <td>user_id</td>
  </tr>
  <tr>
    <td>avg_user_rating</td>
    <td>avg_business_review_len</td>
    <td>avg_user_review_len</td>
    <td>business_star_rating</td>
    <td>avg_business_review_len</td>
    <td>avg_user_review_len</td>
    <td>business_star_rating</td>
  </tr>
  <tr>
    <td>business_star_rating</td>
    <td>avg_business_polarity</td>
    <td>avg_user_rating</td>
    <td>categories</td>
    <td>avg_business_polarity</td>
    <td>avg_user_rating</td>
    <td>categories</td>
  </tr>
  <tr>
    <td>length_of_review_text</td>
    <td>avg_business_subjectivity</td>
    <td>avg_user_polarity</td>
    <td>review_star_rating</td>
    <td>avg_business_subjectivity</td>
    <td>avg_user_polarity</td>
    <td>review_star_rating</td>
  </tr>
  <tr>
    <td>categories</td>
    <td></td>
    <td>avg_user_subjectivity</td>
    <td>review_id</td>
    <td></td>
    <td>avg_user_subjectivity</td>
    <td>review_id</td>
  </tr>
  <tr>
    <td>review_star_rating</td>
    <td></td>
    <td></td>
    <td>business_id</td>
    <td></td>
    <td></td>
    <td>business_id</td>
  </tr>
  <tr>
    <td>review_id</td>
    <td></td>
    <td></td>
    <td>avg_user_review_len</td>
    <td></td>
    <td></td>
    <td>avg_user_review_len</td>
  </tr>
  <tr>
    <td>s_review_id</td>
    <td></td>
    <td></td>
    <td>avg_user_rating</td>
    <td></td>
    <td></td>
    <td>avg_user_rating</td>
  </tr>
  <tr>
    <td>polarity</td>
    <td></td>
    <td></td>
    <td>avg_user_polarity</td>
    <td></td>
    <td></td>
    <td>avg_user_polarity</td>
  </tr>
  <tr>
    <td>subjectivity</td>
    <td></td>
    <td></td>
    <td>avg_user_subjectivity</td>
    <td></td>
    <td></td>
    <td>avg_user_subjectivity</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>avg_business_review_len</td>
    <td></td>
    <td></td>
    <td>avg_business_review_len</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>avg_business_polarity</td>
    <td></td>
    <td></td>
    <td>avg_business_polarity</td>
  </tr>
  <tr>
    <td></td>
    <td></td>
    <td></td>
    <td>avg_business_subjectivity</td>
    <td></td>
    <td></td>
    <td>avg_business_subjectivity</td>
  </tr>
</tbody>
</table>

### Training Prediction Models-
1) We have placed several shell scripts in directory `cluster_batch -> training_models` <br/>
You can run them in any order as they are independant. The files are named with the respective model they will train.
For eg. training GBT regressor for review based prediction from inside the "cluster_batch/training_models" directory as <br/>
`sh ./train_gradientBoostedTreeRegression.sh` <br/>
Each script will save the respective model in a folder named "models" in the project root. <br/>
**Each script will display the validation scores at the end of execution**

### Commands for the execution of Data Analysis.
1) We have placed several analysis shell scripts in the directory `cluster_batch -> analysis` <br/>
You can run them in any order as they are independant. The files are named with the respective analysis. The output of the data analysis (CSV) will be placed in their respective folders inside the "data/analysis" folder from the root of the project. <br/>
For eg. For CategoryAnalysis1, run this command from inside the "cluster_batch/analysis" directory as `sh ./CategoryAnalysis1.sh` <br/>
  This script will create a folder here `GKS_BIGDATA/data/Analysis/categoryAnalysis`

These CSV are then visualized in Tableau, you can find the visualizations <a href="https://public.tableau.com/app/profile/sathish.kumar.alagarsamy/viz/CityLocationAnalysis/CategoryAnalysis?publish=yes">here</a>


**For removing project from gateway, use `rm -f -r {project path}`**
