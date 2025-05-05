### Data Cleaning

We use **Casper** to fix inconsistencies in the Alibaba trace datasets.

- Only the **first hour** of data was downloaded.
- Casper was run on the **first of the 17 CSV files** provided in the `CallGraph` directory.
- During preprocessing, we **filtered out rows** where:
  - `rpctype != "rpc"` (non-RPC calls are excluded)
  - `rt == 0` (responses with zero latency are removed)


```
# traces processed in this file: 6615
cp_affected_traces:  4992
dl_affected_traces:  45962
unaffected_traces 736069

Filtered out 10516155 rows where rpctype is not "rpc" OR rt is 0 (78.88%)

Global stats! 
total: 786614  unaffected:  93.57435794430306 % 
 CP:  0.6346187583745013 % 
 DL:  5.843018303767795
```

### **Sibling Identification**

Run `sibling_identifier.py` to identify all sibling service pairs.  
Each sibling pair will be saved as a separate CSV file in the `output/siblings/` directory: `output/siblings/sibling_<dm1>_<dm2>.csv`

Here, `<dm1>` and `<dm2>` are downstream microservices (child services) that share the same upstream parent (`um`).

Each `sibling_<dm1>_<dm2>.csv` file follows the schema below:
| traceid | rpcid | um | uminstanceid | dm1 | dminstanceid1 | dm1_start_time | dm2 | dminstanceid2 | dm2_start_time | execution_order |
|---------|-------|----|---------------|-----|----------------|----------------|-----|----------------|----------------|------------------|

---

Next, run `csv_filter.py` to filter out rows in the `MSMetrics`, `MSRTMCR`, and `NodeMetrics` directories where the `timestamp` exceeds a given cutoff. This ensures all contextual data aligns with the subset of traces used.

Example command:
```bash
python3 csv_filter.py clusterdata/cluster-trace-microservices-v2022/data 239999 --output output/contextual
```
This will write the filtered metrics to the `output/contextual/` folder.

```
output/contextual
├── MSMetrics
│   └── MSMetrics_0.tar.gz
├── MSRTMCR
│   ├── MSRTMCR_0.tar.gz
│   └── MSRTMCR_1.tar.gz
└── NodeMetrics
    └── NodeMetrics_0.tar.gz

3 directories, 4 files
```

---


### **Sibling Pair Categorization**

For each sibling pair:

- If it contains **only parallel (concurrent)** execution patterns, it is written to:

  ```
  output/res/parallel.csv
  ```

- If it contains **mixed execution patterns** but has **fewer than 1000 total observations**, it is categorized as **unknown** due to insufficient data for statistical inference:

  ```
  output/res/unknown.csv
  ```

The rest sibling.csv is copied to the `output/res/uncertain` folder.
```
Created output/res/parallel.csv with 1556 entries  
Created output/res/unknown.csv with 19591 entries 

Largest uncertain service: sibling_MS_15934_MS_51006.csv
Number of observations: 59133

Note: This pair contains an unknown `um`, so the parent chi-square test cannot be performed.  
However, logistic regression can still be run using system load and call rates.
```

---

### 🔍 **Find Top 5 Largest Sibling Files**

Run `find_top5.py` to list the largest sibling files by observation count.

---

#### 🏆 **Top 5 Largest CSV Files (Sequential Only, No Unknowns):**

| Rank | File Name                                | Rows  | Concurrent | Sequential | Path                                               |
|------|------------------------------------------|-------|------------|------------|----------------------------------------------------|
| 1    | sibling_MS_53745_MS_63670.csv            | 47835 | 0          | 47835      | `output/res/uncertain/sibling_MS_53745_MS_63670.csv` |
| 2    | sibling_MS_23205_MS_63670.csv            | 30237 | 0          | 30237      | `output/res/uncertain/sibling_MS_23205_MS_63670.csv` |
| 3    | sibling_MS_29680_MS_63670.csv            | 28560 | 0          | 28560      | `output/res/uncertain/sibling_MS_29680_MS_63670.csv` |
| 4    | sibling_MS_40123_MS_59532.csv            | 17254 | 28         | 17226      | `output/res/uncertain/sibling_MS_40123_MS_59532.csv` |
| 5    | sibling_MS_12704_MS_26234.csv            | 14030 | 5          | 14025      | `output/res/uncertain/sibling_MS_12704_MS_26234.csv` |

---

#### 🔄 **Top 5 Largest CSV Files with Mixed Execution Patterns:**

| Rank | File Name                                | Rows  | Concurrent | Sequential | Path                                               |
|------|------------------------------------------|-------|------------|------------|----------------------------------------------------|
| 1    | sibling_MS_40123_MS_59532.csv            | 17254 | 28         | 17226      | `output/res/uncertain/sibling_MS_40123_MS_59532.csv` |
| 2    | sibling_MS_12704_MS_26234.csv            | 14030 | 5          | 14025      | `output/res/uncertain/sibling_MS_12704_MS_26234.csv` |
| 3    | sibling_MS_30541_MS_3533.csv             | 11776 | 760        | 11016      | `output/res/uncertain/sibling_MS_30541_MS_3533.csv` |
| 4    | sibling_MS_23205_MS_53745.csv            | 11694 | 5          | 11689      | `output/res/uncertain/sibling_MS_23205_MS_53745.csv` |
| 5    | sibling_MS_29680_MS_53745.csv            | 11014 | 6          | 11008      | `output/res/uncertain/sibling_MS_29680_MS_53745.csv` |

---

### 📊 **Summary Statistics**

| Metric                                 | Count |
|----------------------------------------|-------|
| Total CSV files found                  | 171   |
| Files without `(?)` or `unknown`       | 147   |
| Files with mixed execution patterns    | 86    |
| Files with `(?)` or `unknown`          | 24    |
| Largest file overall                   | `sibling_MS_53745_MS_63670.csv` (47835 rows) |
| Largest mixed execution file           | `sibling_MS_40123_MS_59532.csv` (17254 rows) |
