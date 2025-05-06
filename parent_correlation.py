import pandas as pd
from scipy.stats import chi2_contingency

# Load the CSV file
df = pd.read_csv("sibling-for-analysis/sibling_MS_53745_MS_63670.csv")  # Replace with actual path

# Create contingency table: counts of execution_order per um
contingency = pd.crosstab(df['um'], df['execution_order'])

# Print the contingency table
print("Contingency Table:")
print(contingency)

# Run chi-square test
chi2, p, dof, expected = chi2_contingency(contingency)

# Print results
print(f"\nChi-square statistic: {chi2:.4f}")
print(f"Degrees of freedom: {dof}")
print(f"p-value: {p:.4e}")

# Interpret result
if p < 0.05:
    print("Conclusion: There is a statistically significant association between `um` and `execution_order`.")
else:
    print("Conclusion: No significant association between `um` and `execution_order`.")