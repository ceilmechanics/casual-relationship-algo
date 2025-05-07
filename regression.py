import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression

def calculate_system_load(filtered):
    filtered['dm1_system_load'] = 0.7 * filtered['dm1_cpu'] + 0.3 * filtered['dm1_memory']
    return filtered

import pandas as pd
import numpy as np
import statsmodels.api as sm

def regression_analysis_with_stats(csv_path):
    print(f"Running regression analysis on {csv_path} ...")
    df = pd.read_csv(csv_path)

    # Convert to numeric
    df['dm1_cpu'] = pd.to_numeric(df['dm1_cpu'], errors='coerce')
    df['dm1_memory'] = pd.to_numeric(df['dm1_memory'], errors='coerce')
    df['dm1_mcr'] = pd.to_numeric(df['dm1_mcr'], errors='coerce')
    df['dm1_system_load'] = 0.7 * df['dm1_cpu'] + 0.3 * df['dm1_memory']
    df['dm1_system_lag'] = pd.to_numeric(df['dm1_system_lag'], errors='coerce')
    df['dm1_mcr_lag'] = pd.to_numeric(df['dm1_mcr_lag'], errors='coerce')

    # Map execution_order
    execution_order_map = {'sequential': 0, 'concurrent': 1}
    df['execution_order'] = df['execution_order'].map(execution_order_map)

    regression_data = df.dropna(subset=['dm1_system_load', 'dm1_mcr', 'execution_order'])

    if regression_data['execution_order'].nunique() < 2:
        print("Only one class present. Skipping regression.")
        return

    y = regression_data['execution_order']
    X = regression_data[['dm1_system_load', 'dm1_mcr']]
    X = sm.add_constant(X)  # Add intercept

    model = sm.Logit(y, X)
    result = model.fit()

    print(result.summary())

    # Also print odds ratios and confidence intervals
    odds_ratios = np.exp(result.params)
    conf = result.conf_int()
    conf['OR_lower'] = np.exp(conf[0])
    conf['OR_upper'] = np.exp(conf[1])

    print("\nOdds Ratios with 95% CI:")
    print(pd.concat([odds_ratios, conf[['OR_lower', 'OR_upper']]], axis=1).rename(columns={0: 'OR'}))

    # Print average and std for lag columns
    print(f"\nAvg dm1_system_lag: {regression_data['dm1_system_lag'].mean():.4f} ± {regression_data['dm1_system_lag'].std():.4f}")
    print(f"Avg dm1_mcr_lag: {regression_data['dm1_mcr_lag'].mean():.4f} ± {regression_data['dm1_mcr_lag'].std():.4f}")
    # print(f"Avg dm1_avg_lag: {regression_data['dm1_avg_lag'].mean():.4f} ± {regression_data['dm1_avg_lag'].std():.4f}")

if __name__ == "__main__":
    # Replace with your actual CSV path or pass it dynamically
    regression_analysis_with_stats("output/contextual_MS_52394.csv")