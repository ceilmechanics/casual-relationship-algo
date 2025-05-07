import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
from sklearn.model_selection import train_test_split

def run_random_forest(csv_path):
    # Load and clean data
    df = pd.read_csv(csv_path)

    # Convert relevant columns to numeric
    df['dm1_cpu'] = pd.to_numeric(df['dm1_cpu'], errors='coerce')
    df['dm1_memory'] = pd.to_numeric(df['dm1_memory'], errors='coerce')
    df['dm1_mcr'] = pd.to_numeric(df['dm1_mcr'], errors='coerce')
    df['dm1_system_lag'] = pd.to_numeric(df['dm1_system_lag'], errors='coerce')
    df['dm1_mcr_lag'] = pd.to_numeric(df['dm1_mcr_lag'], errors='coerce')

    # Calculate system load
    df['dm1_system_load'] = 0.7 * df['dm1_cpu'] + 0.3 * df['dm1_memory']

    # Map execution_order to 0 (sequential) and 1 (concurrent)
    df['execution_order'] = df['execution_order'].map({'sequential': 0, 'concurrent': 1})

    # Drop rows with missing values in features or target
    df = df.dropna(subset=['dm1_system_load', 'dm1_mcr', 'execution_order'])

    if df['execution_order'].nunique() < 2:
        print("Only one class present in data. Cannot train a classifier.")
        return

    # Features and target
    X = df[['dm1_system_load', 'dm1_mcr']]
    y = df['execution_order']

    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, stratify=y, random_state=42)

    # Train Random Forest with class weight balancing
    model = RandomForestClassifier(class_weight='balanced', random_state=42)
    model.fit(X_train, y_train)

    # Predict
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]  # For ROC AUC

    # Evaluation
    print("\n🔍 Classification Report:")
    print(classification_report(y_test, y_pred, digits=4))

    print("📉 Confusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    auc_score = roc_auc_score(y_test, y_prob)
    print(f"\nROC AUC Score: {auc_score:.4f}")

if __name__ == "__main__":
    run_random_forest("output/contextual_MS_52394.csv")