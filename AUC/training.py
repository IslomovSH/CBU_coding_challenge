import warnings
warnings.filterwarnings('ignore')  # Clean output

import pyodbc
import pandas as pd
import numpy as np

from catboost import CatBoostClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score
import joblib

# 1. Connect to SQL Server and load data
conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=NODIRBEK;"
    "Database=coding_challenge;"
    "Trusted_Connection=yes;"
)
query = "SELECT * FROM mart.vw_loan_default_features;"

print("Connecting to database and loading data...")
with pyodbc.connect(conn_str) as conn:
    df = pd.read_sql(query, conn)
print(f"Loaded dataset with {len(df):,} rows and {df.shape[1]} columns.")

# 2. Prepare data
if "target_default_flag" in df.columns:
    target_col = "target_default_flag"
elif "default_flag" in df.columns:
    target_col = "default_flag"
else:
    raise ValueError("No target column found in data.")

df = df.dropna(subset=[target_col])
df[target_col] = df[target_col].astype(int)

id_cols = [col for col in ["customer_id", "application_id"] if col in df.columns]

y = df[target_col]
X = df.drop(columns=[target_col] + id_cols, errors="ignore")

print(f"Target column: {target_col} (classes: {y.nunique()})")

# 3. Detect feature types
numeric_cols = X.select_dtypes(include=["int64", "int32", "float64", "float32", "bool"]).columns.tolist()
categorical_cols = X.select_dtypes(include=["object", "category"]).columns.tolist()

print(f"Numeric features: {len(numeric_cols)}, Categorical features: {len(categorical_cols)}")

# 4. CatBoost config
catboost_params = {
    "iterations": 500,
    "depth": 6,
    "learning_rate": 0.1,
    "eval_metric": "AUC",
    "random_seed": 42,
    "verbose": False
}

# 5. Cross-validation
skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
fold_aucs = []

print("\nRunning 5-fold Stratified Cross-Validation...")
for fold, (train_idx, val_idx) in enumerate(skf.split(X, y), start=1):
    X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
    y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]

    model = CatBoostClassifier(**catboost_params)
    model.fit(X_train, y_train, cat_features=categorical_cols)

    val_pred = model.predict_proba(X_val)[:, 1]
    auc = roc_auc_score(y_val, val_pred)
    fold_aucs.append(auc)
    print(f"Fold {fold} AUC: {auc:.4f}")

print(f"Mean CV AUC: {np.mean(fold_aucs):.4f} (std: {np.std(fold_aucs):.4f})")

# 6. Final model on all data
print("\nTraining final model on all data...")
final_model = CatBoostClassifier(**catboost_params)
final_model.fit(X, y, cat_features=categorical_cols)

# 7. Predict for all rows
y_pred = final_model.predict_proba(X)[:, 1]

# 8. Create results.csv with customer_id, prob, default
result_df = pd.DataFrame({
    "customer_id": df["customer_id"],
    "prob": y_pred,
    "default": y
})

result_df = result_df[["customer_id", "prob", "default"]]
result_df.to_csv("results2.csv", index=False)
print("Saved results to results.csv")

# 9. Save the model
joblib.dump(final_model, "trained_model.cbm", compress=3)
print("Trained model saved to trained_model.cbm")
