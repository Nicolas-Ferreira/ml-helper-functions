import pandas as pd
import numpy as np
import pickle

from sklearn.base import TransformerMixin
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline
from sklearn.metrics import f1_score, cohen_kappa_score, accuracy_score, recall_score, precision_score, roc_auc_score, confusion_matrix
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.model_selection import GridSearchCV, train_test_split, cross_val_score, RandomizedSearchCV
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression

from xgboost import XGBClassifier


def performance(y_true, y_pred, id_model):
    
    """

    Calculate performance metrics
    
    :param y_true: array with true values
    :param y_pred: array with predicted values
    :param id_model: model identifier
    :return: DataFrame with scores
    
    """

    # Calculate metrics
    accuracy = accuracy_score(y_true, y_pred)
    recall = recall_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred)
    kappa = cohen_kappa_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)
    auc = roc_auc_score(y_true, y_pred)

    model_performance_dict = {
        'accuracy': accuracy, 
        'recall': recall, 
        'precision': precision, 
        'kappa': kappa, 
        'f1': f1,
        'auc': auc
    }

    perf = pd.DataFrame(
        list(model_performance_dict.items()), 
        index=['a', 'b', 'c', 'd', 'e', 'f'],
        columns=['metric', 'score_' + id_model]
    )

    return perf



def logistic_reg(df, class_column, id_model):

    """

    Train Logistic Regression

    :param df: dataframe with features and class
    :param class_column: column name for class
    :param id_model: model identifier
    :return model_rlog: trained model
    :return performance_rlog: dataframe with perfomance metrics
    :return cm_log: confusion matrix

    """

    # Partition in training and testing
    x = df.loc[:, df.columns != class_column] #select all columns except the class
    y = df[class_column]

    x_train, x_test, y_train, y_test = train_test_split(
        x, 
        y, 
        stratify=y, 
        test_size=0.20, 
        random_state=1234
    )

    # Scaling
    scaler = StandardScaler().fit(x)
    x_train_sc = scaler.transform(x_train)
    x_test_sc  = scaler.transform(x_test)

    # Generate LR model
    log_model = LogisticRegression(solver='saga',
                                   penalty='elasticnet', # for elastic net penalty
                                   l1_ratio=0.5,
                                   class_weight='balanced', # class balance
                                   max_iter=5000)

    # Fitting
    model_rlog = log_model.fit(x_train_sc, y_train)

    # Testing
    y_pred = model_rlog.predict(x_test_sc)

    # Evaluating performance
    performance_rlog = performance(y_test, y_pred, id_model)

    # Confusion Matrix
    cm_log = confusion_matrix(y_test, y_pred)

    return model_rlog, performance_rlog, cm_log



def random_forest(df, class_column, id_model):

    """

    Train Random Forest Classifier

    :param df: dataframe with features and class
    :param class_column: column name for class
    :param id_model: model identifier
    :return model_rlog: trained model
    :return performance_rlog: dataframe with perfomance metrics
    :return cm_log: confusion matrix

    """

    # Partition in training and testing
    x = df.loc[:, df.columns != class_column] #select all columns except the class
    y = df[class_column]

    # Prepare hyperparameters to test
    random_grid = {
        'n_estimators': [250, 500, 1000, 1500, 2000], # Number of trees in random forest
        'max_features': ['auto'], # Number of features to consider at every split
        'max_depth': [10, 25, 50, 75, 100], # Maximum number of levels in tree
        'min_samples_split': [5, 10, 15, 20, 30, 40], # Minimum number of samples required to split a node
        'min_samples_leaf': [5, 10, 15, 20] # Minimum number of samples required at each leaf node
    } 

    # Create a base model to adjust
    rf = RandomForestClassifier()

    # Perform a Random Search of parameters (3 cross validation, 100 different combinations, use all available cores)
    rf_random = RandomizedSearchCV(
        estimator=rf, 
        scoring='f1',
        param_distributions=random_grid, 
        n_iter=100,
        v=3, 
        verbose=2, 
        random_state=42, 
        n_jobs=-1
    )
    
    # Fitting
    rf_random.fit(x_train, y_train)

    # Testing
    y_pred = rf_random.predict(x_test)

    # Evaluating performance
    performance_randomf = performance(y_test, y_pred, modelo)

    # Confusion Matrix
    cm_rf = confusion_matrix(y_test, y_pred)

    return rf_random, performance_randomf, cm_rf



def xgboost(df, class_column, id_model):

    """

    Train XGBoost Classifier

    :param df: dataframe with features and class
    :param class_column: column name for class
    :param id_model: model identifier
    :return model_rlog: trained model
    :return performance_rlog: dataframe with perfomance metrics
    :return cm_log: confusion matrix
    
    """

    # Partition in training and testing
    x = df.loc[:, df.columns != class_column] #select all columns except the class
    y = df[class_column]

    # prepare the hyperparameters to test
    random_grid_xgb = {
        # Parameters that we are going to tune.
        'learning_rate' : [0.01, 0.02, 0.04, 0.06, 0.08, 0.1],
        'n_estimators' : [500, 750, 1000, 1250, 1500],
        'max_depth':[5, 10, 20, 30],
        'subsample': [0.8, 0.9, 1],
        'colsample_bytree': [0.4, 0.6, 0.8, 1],
        'gamma': [0.1, 0.5, 0.7, 1]
    }

    xgb = XGBClassifier() # base model
    xgb_random = RandomizedSearchCV(
        estimator=xgb, 
        scoring='f1', 
        param_distributions=random_grid_xgb,
        n_iter=100, 
        cv=3, 
        verbose=2, 
        random_state=42, 
        n_jobs=-1
    )

    # Fitting
    xgb_random.fit(x_train, y_train)

    # Testing
    y_pred = xgb_random.predict(x_test)

    # Evaluating performance
    performance_xgb = performance(y_test, y_pred, modelo)

    # Confusion Matrix
    cm_xgb = confusion_matrix(y_test, y_pred)

    return xgb_random, performance_xgb, cm_xgb
