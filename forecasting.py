"""
main.py

Requirements:
    pip install pandas scikit-learn matplotlib

Input:
    loan_default_features.csv  (export of mart.vw_loan_default_features)

Output:
    Prints ROC AUC on a hold-out test set
    Saves result.csv with predicted default probabilities for all applications
"""

import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, RocCurveDisplay
from sklearn.pipeline import make_pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier

import matplotlib.pyplot as plt


def add_feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Add a few simple, domain-based features if columns exist."""
    df = df.copy()

    # 1) Non-linear effect of debt_to_income_ratio
    if "debt_to_income_ratio" in df.columns:
        df["debt_to_income_ratio_sq"] = df["debt_to_income_ratio"] ** 2

    # 2) Interaction between credit score and recent inquiries
    if "credit_score" in df.columns and "recent_inquiry_count" in df.columns:
        df["score_inquiry_interaction"] = (
            df["credit_score"] * df["recent_inquiry_count"]
        )

    return df


def main():
    # -----------------------------
    # 1. Load data
    # -----------------------------
    csv_path = "loan_default_features.csv"
    df = pd.read_csv(csv_path)

    TARGET_COL = "target_default_flag"
    ID_COLS = ["customer_id", "application_id"]

    # Drop rows where target is missing (if any)
    df = df.dropna(subset=[TARGET_COL])

    # Basic info
    print("Data shape:", df.shape)
    print("Default rate:",
          df[TARGET_COL].mean().round(4),
          "(1 = default)")

    # -----------------------------
    # 2. Feature engineering
    # -----------------------------
    df = add_feature_engineering(df)

    # -----------------------------
    # 3. Split X / y
    # -----------------------------
    y = df[TARGET_COL]
    X = df.drop(columns=ID_COLS + [TARGET_COL])

    # One-hot encode categoricals
    cat_cols = X.select_dtypes(include=["object", "category"]).columns.tolist()
    X_encoded = pd.get_dummies(X, columns=cat_cols, drop_first=True)

    print("Number of features after encoding:", X_encoded.shape[1])

    # -----------------------------
    # 4. Train-test split
    # -----------------------------
    X_train, X_test, y_train, y_test = train_test_split(
        X_encoded,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    # -----------------------------
    # 5. Model: Imputer + RandomForest
    # -----------------------------
    model = make_pipeline(
        SimpleImputer(strategy="median"),
        RandomForestClassifier(
            n_estimators=400,
            max_depth=None,
            min_samples_split=5,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1
        )
    )

    model.fit(X_train, y_train)

    # -----------------------------
    # 6. Evaluation (AUC on test set)
    # -----------------------------
    y_proba_test = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_proba_test)
    print(f"Test ROC AUC: {auc:.4f}")

    # Optional: ROC curve
    RocCurveDisplay.from_predictions(y_test, y_proba_test)
    plt.title(f"ROC Curve (AUC = {auc:.3f})")
    plt.tight_layout()
    plt.show()

    # -----------------------------
    # 7. Fit on full data for final forecasting
    # -----------------------------
    model_full = make_pipeline(
        SimpleImputer(strategy="median"),
        RandomForestClassifier(
            n_estimators=400,
            max_depth=None,
            min_samples_split=5,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1
        )
    )
    model_full.fit(X_encoded, y)

    # Predict probabilities for ALL rows
    y_proba_all = model_full.predict_proba(X_encoded)[:, 1]

    # -----------------------------
    # 8. Save result.csv
    # -----------------------------
    result = df[ID_COLS].copy()
    result["pred_default_proba"] = y_proba_all

    # Optional: keep the original target for reference
    result["actual_default_flag"] = df[TARGET_COL].values

    result_path = "result.csv"
    result.to_csv(result_path, index=False)
    print(f"Saved forecasting results to: {result_path}")
    print("result.csv columns:", result.columns.tolist())


if __name__ == "__main__":
    main()
