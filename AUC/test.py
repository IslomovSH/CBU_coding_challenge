import pandas as pd
import joblib
import pyodbc
import warnings

warnings.filterwarnings("ignore")

# 1. Load 10K dataset from SQL
conn_str = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=NODIRBEK;"
    "Database=coding_challenge;"
    "Trusted_Connection=yes;"
)
query = "SELECT * FROM mart.loan_default_features"  # 10K data

print("Connecting to SQL Server...")
with pyodbc.connect(conn_str) as conn:
    df = pd.read_sql(query, conn)
print(f"Loaded {len(df):,} rows")

# 2. Load trained model
model = joblib.load("trained_model.cbm")
print("Model loaded.")

# 3. Prepare features
id_cols = [col for col in ["customer_id", "application_id"] if col in df.columns]
X = df.drop(columns=id_cols + ["default_flag", "target_default_flag"], errors="ignore")

# 4. Align features with model
expected = model.feature_names_
missing = [c for c in expected if c not in X.columns]
if missing:
    raise ValueError(f"Missing expected features: {missing}")
X = X[expected]  # ensure correct column order

# 5. Predict
print("Predicting...")
probs = model.predict_proba(X)[:, 1]
defaults = (probs >= 0.5).astype(int)

# 6. Output results
output = pd.DataFrame({
    "customer_id": df["customer_id"],
    "prob": probs,
    "default": defaults
})
output.to_csv("result.csv", index=False)
print("Saved predictions to predictions_10k.csv")
