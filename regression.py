import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression

def calculate_system_load(filtered):
    filtered['dm1_system_load'] = 0.7 * filtered['dm1_cpu'] + 0.3 * filtered['dm1_memory']
    return filtered

def regression_analysis(csv_path):
    print(f"Running regression analysis on {csv_path} ...")
    df = pd.read_csv(csv_path)

    df['dm1_cpu'] = pd.to_numeric(df['dm1_cpu'], errors='coerce')
    df['dm1_memory'] = pd.to_numeric(df['dm1_memory'], errors='coerce')
    df['dm1_mcr'] = pd.to_numeric(df['dm1_mcr'], errors='coerce')
    df['dm1_system_load'] = 0.7 * df['dm1_cpu'] + 0.3 * df['dm1_memory']
    # Convert lag columns and calculate their average
    df['dm1_system_lag'] = pd.to_numeric(df['dm1_system_lag'], errors='coerce')
    df['dm1_mcr_lag'] = pd.to_numeric(df['dm1_mcr_lag'], errors='coerce')
    # df['dm1_avg_lag'] = df[['dm1_system_lag', 'dm1_mcr_lag']].mean(axis=1)

    regression_data = df[df[['dm1_system_load', 'dm1_mcr']].notna().all(axis=1)].copy()

    if regression_data.empty:
        print("No valid rows for regression.")
        return

    if regression_data['execution_order'].dtype == object:
        y = pd.Categorical(regression_data['execution_order']).codes
    else:
        y = regression_data['execution_order'].values

    X = regression_data[['dm1_system_load', 'dm1_mcr']].values

    model = LogisticRegression()
    model.fit(X, y)

    print("\n===== Logistic Regression Results =====")
    print(f"Intercept: {model.intercept_[0]:.4f}")
    print(f"Coefficient for dm1_system_load: {model.coef_[0][0]:.4f}")
    print(f"Coefficient for dm1_mcr: {model.coef_[0][1]:.4e}")
    print(f"Accuracy (R² Score): {model.score(X, y):.4f}")

    # Odds ratios
    odds_ratios = np.exp(model.coef_[0])
    print(f"\nOdds Ratio for dm1_system_load: {odds_ratios[0]:.4f}")
    print(f"Odds Ratio for dm1_mcr: {odds_ratios[1]:.4f}")
    print("=======================================\n")

    # Print average and std for lag columns
    print(f"\nAvg dm1_system_lag: {regression_data['dm1_system_lag'].mean():.4f} ± {regression_data['dm1_system_lag'].std():.4f}")
    print(f"Avg dm1_mcr_lag: {regression_data['dm1_mcr_lag'].mean():.4f} ± {regression_data['dm1_mcr_lag'].std():.4f}")
    # print(f"Avg dm1_avg_lag: {regression_data['dm1_avg_lag'].mean():.4f} ± {regression_data['dm1_avg_lag'].std():.4f}")

if __name__ == "__main__":
    # Replace with your actual CSV path or pass it dynamically
    regression_analysis("output/contextual_MS_52394.csv")