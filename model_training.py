'''Linear Regression Model training example Jupyter Notebook'''
'''required pkgs: pysaprk, pandas, scikit-learn'''

import pyspark
import pandas as pd
import numpy as np
import joblib

from pyspark.sql.session import SparkSession
from pyspark.ml.classification import LogisticRegressionModel


spark = SparkSession.builder.appName('logistic_regression_model_training').getOrCreate()

df = spark.read.csv('datasets/transactions.csv', header=True, inferSchema=True)

df.show(2)
df.count() 

pd_df = df.sample(withReplacement=False, fraction=0.1, seed=42).toPandas()

# EXPLORATION OF DATA
pd_df.info()
print(pd_df.isFraud.value_counts()) # we have HIGHLY IMBALANCED dataset ( 2543410 non-fraud vs 3257 fraudulant values)
plt.figure(figsize=(2,4))
pd_df.isFraud.value_counts().plot(kind='bar')
pd_df['isFraud'].value_counts().plot(kind='pie', autopct='%1.1f%%') # SHOWS HOW IMBALANCE THE DATASET IS.

# INFORMATINO OF DATAFRAME, EXPLORING THE DATAFRAME
pd.options.display.float_format = '{:,.2f}'.format
# LEGIT
print('legit["amount"]: \n',pd_df[pd_df['isFraud'] == 0]['amount'].describe(),'\n')
# FRAUD
print('fraud["amount"]: \n', pd_df[pd_df['isFraud'] == 1]['amount'].describe())

'''NOW WE WILL DO Feature Engineering'''
'''FEATURES = transaction frequency, average transaction amount, or time-based features.'''

# ----------- TRANSACTION FREQUENCY -------------- 
# if high freq of trans then may be fraud, behavioural pattern (if person was using low trans but suddenly high then suspicious)
freq_orig = pd_df.nameOrig.value_counts().reset_index()
freq_orig.columns = ['nameOrig', 'orig_freq']
freq_dest = pd_df.nameDest.value_counts().reset_index()
freq_dest.columns = ['nameDest', 'dest_freq']

print(freq_orig)
print(freq_dest)

pd_df = pd_df.merge(freq_orig, on='nameOrig', how='left')
pd_df = pd_df.merge(freq_dest, on='nameDest', how='left')

pd_df['orig_freq'] = pd_df['orig_freq'].fillna(0)
pd_df['dest_freq'] = pd_df['dest_freq'].fillna(0)

pd_df.head(2) # WE ADDED orig_freq, dest_freq TO OUR DATAFRAME legit

# ---------- AVG(mean) TRANSACTION AMOUNT -------- feature
mean_orig = pd_df.groupby('nameOrig')['amount'].mean().reset_index()
mean_orig.columns = ['nameOrig', 'mean_orig']

mean_dest = pd_df.groupby('nameDest')['amount'].mean().reset_index()
mean_dest.columns = ['nameDest', 'mean_dest']

pd_df = pd_df.merge(mean_orig, on='nameOrig', how='left')
pd_df = pd_df.merge(mean_dest, on='nameDest', how='left')

pd_df['mean_orig'] = pd_df['mean_orig'].fillna(0)
pd_df['mean_dest'] = pd_df['mean_dest'].fillna(0)

pd_df.head(2)

#import seaborn as sns
'''IMPORTANT'''
# TAKING mean OF FRAUD AND LEGIT amount SECTION
# This difference is very important for us as it shows the difference between normal transaction and fraudulent transaction. This information is very important for our model.
# removed mean_orig and dest_orig as they might be redundant of amount column itself (values was coming same)
pd_df.groupby('isFraud')[['step', 'amount', 'oldbalanceOrg', 'newbalanceOrig', 'oldbalanceDest', 'newbalanceDest', 'orig_freq', 'dest_freq']].mean()

# CONVERT CATEGORICAL COLUMN type INTO NUMERIC ONE
pd_df = pd.get_dummies(pd_df, columns=['type'])
pd_df.head(2)

# DROP NOT REQUIRED COLUMNS
pd_df = pd_df.drop(columns=['nameOrig','nameDest','isFlaggedFraud'])
pd_df.head(1)

'''NOW OUR DATASET IS GOOD TO BE SAMPLED AND BUT NOT BALANCED YET'''
# first we will DIVIDE DATAFRAME INTO FRAUD AND NON FRAUD
legit = pd_df[pd_df['isFraud'] == 0]
fraud = pd_df[pd_df['isFraud'] == 1]
print(legit.shape)
print(fraud.shape)

# SAMPLE DATASET NOW FROM legit and make it equal to fraud df (which is 81 rows, with 0.01 fraction)
legit_sample = legit.sample(n=812)

# NOW CONCAT BOTH legit_sample and fraud dataframe
merged_df = pd.concat([legit_sample, fraud], axis=0)
print(merged_df.shape)

merged_df['isFraud'].value_counts()
merged_df.groupby('isFraud').mean()

# so this is now very similar to sampled_merged_df which is good, it tells that nature of df has not changed
# we got a good sample 👍👍👍
pd_df.groupby('isFraud').mean()

# NOW WE WILL SPLIT THE DF INTO features AND target, and feed to our machine learning model
X = merged_df.drop(columns='isFraud', axis=1)
y = merged_df['isFraud']

# NOW CONVERT THESE FEATURES AND LABELS INTO training DATA AND test DATA
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=2)

print('train test ytrain ytest df.shapes',X_train.shape, X_test.shape, y_train.shape, y_test.shape)

# NOW MODEL TRAINING, WE WILL BE USING TODAY logistic regression model. CAN USE DIFF MODEL & CHECK IF THEY GIVE BETTER ACCURACY SCORE.
# note: logistic regression model is generaly used for binary classification problems like this one
from sklearn.linear_model import LogisticRegression
model = LogisticRegression(max_iter=600)

# TRAINING THE LOGISTICREGRESSION MODEL WITH TRAINING DATA
model.fit(X_train, y_train)

print(lsv,'STEP 7: Check the accuracy',lsv)
# EVALUATE THE MODEL
 # accuracy score
  # accuracy on training data
from sklearn.metrics import accuracy_score
X_train_prediction = model.predict(X_train)
training_data_accuracy = accuracy_score(X_train_prediction, y_train)
print('Accuracy on training data: ',training_data_accuracy)

# accuracy score on test data
X_test_prediction = model.predict(X_test)
test_data_accuracy = accuracy_score(X_test_prediction, y_test)
print('Accuracy on test data: ',test_data_accuracy)

# WE HAVE AROUND 94-97 accuracy WHICH IS GOOD. IF THE DIFF BET training and test accuracy WAS MORE THEN OUR MODEL IS
# CONSIDERED underfitted or overfitted.

# saving the model
joblib.dump(model, 'logistic_regression_model.pkl')

