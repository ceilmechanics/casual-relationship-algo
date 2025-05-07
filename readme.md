### Data Cleaning and Trace Statistics

We use **Casper** to correct inconsistencies in the [Alibaba Microservices Traces v2022](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2022).

#### **Data Scope**
- Only data from the **first hour** was downloaded.
- Casper was applied to the **first of 17 CSV files** in the `CallGraph` directory.
- The latest timestamp in this file is `179999`, indicating the analysis covers only the **first ~3 minutes** of trace data.

#### **Preprocessing**
Before running Casper, the following filters were applied:
- Removed rows where `rpctype != "rpc"` (non-RPC calls).
- Removed rows where `rt == 0` (calls with zero latency).

A total of **10,516,155 rows** were filtered out, accounting for **78.88%** of the original data.

#### **Post-Casper Rebuilding**
After applying Casper, the total number of reconstructed traces is **786,614**.

---

### Sibling Identification

Sibling service pairs are identified from MSCallGraph data—i.e., services called by the same upstream microservice (`um`) within a single trace—and classified as either concurrent or sequential.

```
TOTAL SUMMARY:
   • Unique sibling pairs: 22,117
   • Total records: 1,420,971
   • Parallel executions: 220,280 (15.5%)
   • Sequential executions: 1,200,691 (84.5%)

📁 OUTPUT LOCATION: output/siblings/
```

Each file `sibling_<dm1>_<dm2>.csv` follows this schema:
```
| traceid | rpcid | um | uminstanceid | dm1 | dminstanceid1 | dm1_start_time | dm2 | dminstanceid2 | dm2_start_time | execution_order |
```
---

### Sibling Pair Categorization

We process all sibling CSVs in `output/siblings/` and categorize them into:
- **Parallel**: All records have `execution_order == 'concurrent'`
- **Unknown**: Mixed execution patterns with < 1000 observations
- **Uncertain**: Mixed patterns with ≥ 1000 observations

#### Parallel Category
Written to: `output/res/parallel.csv`  
Entries: **1556**

| um       | dm1      | dm2      | num_observations |
|----------|----------|----------|------------------|
| MS_64512 | MS_70124 | MS_19439 | 7                |
| MS_51975 | MS_32628 | MS_37363 | 5                |

#### Unknown Category
Written to: `output/res/unknown.csv`  
Entries: **19591**

| um       | dm1      | dm2      | num_seq | num_parallel | num_observations |
|----------|----------|----------|---------|---------------|------------------|
| MS_66647 | MS_45042 | MS_64565 | 8       | 0             | 8                |

#### Uncertain Category
Remaining files are copied to `output/res/uncertain/`.

Example:
```
File: sibling_MS_15934_MS_51006.csv
Observations: 59133
Note: Unknown `um` prevents chi-square test; logistic regression still applicable.
```

---

### 🔍 Find Top 5 Largest Sibling Files

Run `find_top5.py` to list the largest sibling CSVs by observation count.

#### 🏆 Top 5 Largest Files (Mixed Execution, Known Services)

| Rank | File Name                                | Rows  | Concurrent | Sequential | Path                                               |
|------|------------------------------------------|-------|------------|------------|----------------------------------------------------|
| 1    | sibling_MS_53745_MS_63670.csv            | 47835 | 0          | 47835      | `output/res/uncertain/sibling_MS_53745_MS_63670.csv` |
| 2    | sibling_MS_23205_MS_63670.csv            | 30237 | 0          | 30237      | `output/res/uncertain/sibling_MS_23205_MS_63670.csv` |
| 3    | sibling_MS_29680_MS_63670.csv            | 28560 | 0          | 28560      | `output/res/uncertain/sibling_MS_29680_MS_63670.csv` |
| 4    | sibling_MS_40123_MS_59532.csv            | 17254 | 28         | 17226      | `output/res/uncertain/sibling_MS_40123_MS_59532.csv` |
| 5    | sibling_MS_12704_MS_26234.csv            | 14030 | 5          | 14025      | `output/res/uncertain/sibling_MS_12704_MS_26234.csv` |

#### 🔄 Top 5 Largest Mixed Execution Files

| Rank | File Name                                | Rows  | Concurrent | Sequential | Path                                               |
|------|------------------------------------------|-------|------------|------------|----------------------------------------------------|
| 1    | sibling_MS_40123_MS_59532.csv            | 17254 | 28         | 17226      | `output/res/uncertain/sibling_MS_40123_MS_59532.csv` |
| 2    | sibling_MS_12704_MS_26234.csv            | 14030 | 5          | 14025      | `output/res/uncertain/sibling_MS_12704_MS_26234.csv` |
| 3    | sibling_MS_30541_MS_3533.csv             | 11776 | 760        | 11016      | `output/res/uncertain/sibling_MS_30541_MS_3533.csv` |
| 4    | sibling_MS_23205_MS_53745.csv            | 11694 | 5          | 11689      | `output/res/uncertain/sibling_MS_23205_MS_53745.csv` |
| 5    | sibling_MS_29680_MS_53745.csv            | 11014 | 6          | 11008      | `output/res/uncertain/sibling_MS_29680_MS_53745.csv` |

---

### 📊 Summary Statistics

| Metric                             | Count |
|------------------------------------|-------|
| Total CSV files found              | 171   |
| Without `(?)` or `unknown`         | 147   |
| With mixed execution patterns      | 86    |
| With `(?)` or `unknown`            | 24    |
| Largest file overall               | `sibling_MS_53745_MS_63670.csv` (47835 rows) |
| Largest mixed execution file       | `sibling_MS_40123_MS_59532.csv` (17254 rows) |

---

### Selected Services for Further Analysis

#### 1. Sequential Only
```
File: sibling_MS_53745_MS_63670.csv
Rows: 47835 (Concurrent: 0, Sequential: 47835)
```

#### 2. Mixed (Mostly Sequential)
```
File: sibling_MS_40123_MS_59532.csv
Rows: 17254 (Concurrent: 28, Sequential: 17226)
```

#### 3. Mixed (Balanced)
```
File: sibling_MS_30541_MS_3533.csv
Rows: 11776 (Concurrent: 760, Sequential: 11016)
```

---

### Chi-Square Test Results

To test whether `execution_order` is associated with `um`, we conducted chi-square tests.

**Sample: sibling_MS_30541_MS_3533.csv**
- Chi-square statistic: 3.0633
- Degrees of freedom: 2
- p-value: 0.2162  
**Conclusion**: No significant association.

**Sample: contextual_MS_29421.csv**
- Chi-square statistic: 2176.9108
- Degrees of freedom: 12
- p-value: 0.0000  
**Conclusion**: Significant association.

**Sample: sibling_MS_53745_MS_63670.csv**
- Chi-square statistic: 0.0000
- Degrees of freedom: 0
- p-value: 1.0000  
**Conclusion**: No association.

---

### Contextual Feature Extraction

To enrich each sibling record with context (system load, memory, MCR):
1. Extract `dm1`, `dm2` and their start times.
2. Align timestamps to 60-second intervals.
3. Use `metrics_index` and `mcr_index` to fetch:
   - CPU & memory from `MSMetrics`
   - MCR from `MSRTMCR`
4. Expand search ±5 intervals if no match.
5. Calculate time lag.
6. Save enriched results to a new CSV.

Note: Some metrics are averaged across instances (when `msinstanceid` differs but `msname` matches).

---

### Logistic Regression Results

Performed on grouped traces by `um` using contextual features.

#### `contextual_MS_9638.csv`
```
Intercept: 0.6718
dm1_system_load coef: 0.1075  | Odds Ratio: 1.1135
dm1_mcr coef: 1.4389e-07      | Odds Ratio: 1.0000
Accuracy (R²): 0.6658
Avg system lag: 28717.63 ± 17620.49 ms
Avg MCR lag: 28717.63 ± 17620.49 ms
```

#### `contextual_MS_29421.csv`
```
Intercept: 5.4375
dm1_system_load coef: 0.0071  | Odds Ratio: 1.0072
dm1_mcr coef: -7.2571e-08     | Odds Ratio: 1.0000
Accuracy (R²): 0.9956
Avg system lag: 29371.53 ± 18974.62 ms
Avg MCR lag: 29371.53 ± 18974.62 ms
```

#### `contextual_MS_52394.csv`
```
Intercept: 3.5229
dm1_system_load coef: 0.5590  | Odds Ratio: 1.7489
dm1_mcr coef: 7.3447e-07      | Odds Ratio: 1.0000
Accuracy (R²): 0.9738
Avg system lag: 31618.36 ± 16830.22 ms
Avg MCR lag: 31618.36 ± 16830.22 ms
```