import pandas as pd
import numpy as np

annual_data_all = pd.read_csv("full_annual_data_all.csv")
annual_data_all["datadate"] = pd.to_datetime(annual_data_all["datadate"])
filtered_data = annual_data_all[annual_data_all["datadate"] == pd.Timestamp("2019-12-31")]
annual_metadata = ["datadate", "curcd", "curcdi", "isin", "fic", "loc", "conm"] #gvkey
filtered_data = filtered_data.drop(columns=annual_metadata)
nan_counts_per_column = filtered_data.isna().sum(axis=0)
columns_to_drop = nan_counts_per_column[nan_counts_per_column > 0].index # Potentially leave in more but drop rows
filtered_data = filtered_data.drop(columns=columns_to_drop)

gv_keys = (270979, 15598, 16447, 15724, 15513, 15512, 15804, 15545, 23087, 254578, 270248, 235656, 268159,
                    296091, 212641, 204825, 16349, 316604, 274774, 15603, 16281, 15677, 260840, 208752, 288153, 269546,
                    326143, 317629, 15773, 63759, 15647, 205699, 63546, 269809, 24826, 274282, 15862, 15914, 23595,
                    211471, 200731, 231496, 220426, 63120)
ar = (-17.17, -74.34, -71.35, -66.27, -38.72, -66.05, -60.44, -67.60, -47.25, -60.71, -39.68, -97.15, -38.46, -47.07, -47.40,
        -60.56, -73.49, -68.41, -48.82, -82.45, -39.90, -68.08, -49.43, -13.93, -58.12, -32.88, -46.48, -117.78, -56.92, -77.09,
        -93.22, -62.23, -76.19, -79.99, -23.39, -17.57, -75.16, -29.42, -51.28, -44.54, -60.25, -53.06, -50.57, -70.17)

df = pd.DataFrame(list(zip(gv_keys, ar)), columns=["gvkey", "OUTCOME"])

merged_df = pd.merge(filtered_data, df, how="left", on="gvkey")
merged_df = merged_df.drop(columns="gvkey")
merged_df = merged_df.dropna(subset=["OUTCOME"])
merged_df

merged_df["OUTCOME_LABEL"] = np.where(merged_df["OUTCOME"] >= -58, 0, 1)

from sklearn.model_selection import LeaveOneOut, cross_val_score
X = merged_df.drop("OUTCOME_LABEL", axis=1)
X = X.drop("OUTCOME", axis=1)
y = merged_df["OUTCOME_LABEL"]
loo = LeaveOneOut()

sum(y) / len(y)

from sklearn.linear_model import LogisticRegressionCV
log_reg = LogisticRegressionCV(penalty='l2', max_iter=1000, solver='liblinear')
log_reg_scores = cross_val_score(log_reg, X, y, cv=loo)
print(f'Logistic Regression CV scores: {log_reg_scores.mean()}')

from sklearn.ensemble import RandomForestClassifier
rf_clf = RandomForestClassifier(random_state=42)
rf_cv_scores = cross_val_score(rf_clf, X, y, cv=loo)
print(f'Cross-validation scores: {rf_cv_scores.mean()}')

from xgboost import XGBClassifier
xgb_clf = XGBClassifier(use_label_encoder=False, eval_metric='mlogloss')
xgb_cv_scores = cross_val_score(xgb_clf, X, y, cv=loo)
print(f'Cross-validation scores: {xgb_cv_scores.mean()}')


import matplotlib.pyplot as plt
xgb_clf.fit(X, y)
feature_importances = xgb_clf.feature_importances_
importance_series = pd.Series(feature_importances, index=X.columns)
importance_series.sort_values(ascending=False).plot(kind='bar', figsize=(12, 6))
plt.title('Feature Importance')
plt.xlabel('Features')
plt.ylabel('Importance Score')
plt.show()
"""

from sklearn.svm import SVC
from sklearn.model_selection import cross_val_score
svm_clf = SVC(probability=True, kernel="rbf")
svm_cv_scores = cross_val_score(svm_clf, X, y, cv=loo)
print(f'SVM Cross-validation scores: {svm_cv_scores.mean()}')

from sklearn.neighbors import KNeighborsClassifier
knn_clf = KNeighborsClassifier(n_neighbors=9) # use sqrt(n)
knn_cv_scores = cross_val_score(knn_clf, X, y, cv=loo)
print(f'KNN Cross-validation scores: {knn_cv_scores.mean()}')



from sklearn.inspection import permutation_importance
# Fit your kNN model
knn_clf.fit(X, y)
# Perform permutation importance
results = permutation_importance(knn_clf, X, y, scoring='accuracy')
# Get importance scores
importance_scores = results.importances_mean
# Visualize feature importance
plt.bar(range(X.shape[1]), importance_scores, tick_label=X.columns.tolist())
plt.xticks(rotation=90)
plt.xlabel('Feature Name')
plt.ylabel('Mean Decrease in Accuracy')
plt.title('Feature Importance')
plt.tight_layout()
plt.show()




from sklearn.linear_model import LogisticRegression
log_reg_clf = LogisticRegression(max_iter=1000)
log_reg_cv_scores = cross_val_score(log_reg_clf, X, y, cv=loo)
print(f'Logistic Regression Cross-validation scores: {log_reg_cv_scores.mean()}')

from sklearn.naive_bayes import GaussianNB
nb_clf = GaussianNB()
nb_cv_scores = cross_val_score(nb_clf, X, y, cv=loo)
print(f'Naive Bayes Cross-validation scores: {nb_cv_scores.mean()}')

from sklearn.neural_network import MLPClassifier
nn_clf = MLPClassifier(max_iter=1000)
nn_cv_scores = cross_val_score(nn_clf, X, y, cv=loo)
print(f'Neural Network Cross-validation scores: {nn_cv_scores.mean()}')
