# Microservices Trace Analysis - Causal Relationship Inference

This document presents our analysis of microservice execution patterns based on the Alibaba Microservices Traces dataset, with a focus on inferring causal relationships between sibling services.

## Dataset Overview

We utilized **Casper** to correct inconsistencies in the [Alibaba Microservices Traces v2022](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2022).
- Only data from the **first hour** was downloaded.
- Casper was applied to the **first of 17 CSV files** in the `CallGraph` directory.
- The latest timestamp in this file is `179999`, indicating the analysis covers only the **first ~3 minutes** of trace data.

## Preprocessing

We applied the following filters to clean the dataset:
- Removed non-RPC calls (`rpctype != "rpc"`)
- Removed zero-latency calls (`rt == 0`)

These filters excluded 10,516,155 rows, representing 78.88% of the original dataset.

## Analysis Overview

After processing, we identified:
- **786,614** total traces
- **22,117** unique sibling service pairs
- **1,420,971** total records across all sibling pairs

## Execution Pattern Classification

Services were classified into execution patterns:
- **Parallel executions**: 220,280 (15.5%)
- **Sequential executions**: 1,200,691 (84.5%)

## Sibling Service Analysis
We identified microservice pairs that share the same upstream caller within a single trace and classified their execution patterns as either concurrent or sequential.


###  Schema Definition
Each `sibling_<dm1>_<dm2>.csv` file contains the following fields:

| Field | Description |
|-------|-------------|
| traceid | Unique trace identifier |
| rpcid | RPC call identifier |
| um | Upstream microservice |
| uminstanceid | Instance ID of upstream service |
| dm1 | First downstream microservice |
| dminstanceid1 | Instance ID of first downstream service |
| dm1_start_time | Start timestamp of first downstream call |
| dm2 | Second downstream microservice |
| dminstanceid2 | Instance ID of second downstream service |
| dm2_start_time | Start timestamp of second downstream call |
| execution_order | Classification (concurrent/sequential) |

### Categorization Results

#### Parallel Pairs
- **Definition**: Sibling pairs with exclusively concurrent execution patterns
- **Count**: 1,556 entries
- **Output**: `output/res/parallel.csv`

**Example entries:**
| um | dm1 | dm2 | num_observations |
|----|-----|-----|------------------|
| MS_64512 | MS_70124 | MS_19439 | 7 |
| MS_51975 | MS_32628 | MS_37363 | 5 |
| MS_2780 | MS_58932 | MS_69982 | 1 |

#### Unknown Pairs
- **Definition**: Pairs with mixed execution patterns but fewer than 1,000 observations
- **Count**: 19,591 entries
- **Output**: `output/res/unknown.csv`

**Example entries:**
| um | dm1 | dm2 | num_seq | num_parallel | num_observations |
|----|-----|-----|---------|--------------|------------------|
| MS_66647 | MS_45042 | MS_64565 | 8 | 0 | 8 |
| MS_71956 | MS_34796 | MS_56926 | 6 | 14 | 20 |
| MS_70114 | MS_24612 | MS_28020 | 3 | 0 | 3 |

#### Uncertain Pairs
- **Definition**: Remaining sibling pairs with sufficient observations for analysis
- **Count**: 171 CSV files
- **Output**: `output/res/uncertain/`
- **Notable**: 147 files without `(?)` or `unknown` markers, 86 files with mixed execution patterns

#### Top 5 sibling pairs in uncertain folder

**Top 5 sibling pairs by observation count**
- excluding unknown or (?) services
- may or may not have mixed execution patterns
  
| Rank | File Name | Total Rows | Concurrent | Sequential |
|------|-----------|------------|------------|------------|
| 1 | sibling_MS_53745_MS_63670.csv | 47,835 | 0 | 47,835 |
| 2 | sibling_MS_23205_MS_63670.csv | 30,237 | 0 | 30,237 |
| 3 | sibling_MS_29680_MS_63670.csv | 28,560 | 0 | 28,560 |
| 4 | sibling_MS_40123_MS_59532.csv | 17,254 | 28 | 17,226 |
| 5 | sibling_MS_12704_MS_26234.csv | 14,030 | 5 | 14,025 |

**Top files with mixed execution patterns:**
- excluding unknown or (?) services
- must have mixed (concurrent + sequential) execution patterns 
  
| Rank | File Name | Total Rows | Concurrent | Sequential |
|------|-----------|------------|------------|------------|
| 1 | sibling_MS_40123_MS_59532.csv | 17,254 | 28 | 17,226 |
| 2 | sibling_MS_12704_MS_26234.csv | 14,030 | 5 | 14,025 |
| 3 | sibling_MS_30541_MS_3533.csv | 11,776 | 760 | 11,016 |
| 4 | sibling_MS_23205_MS_53745.csv | 11,694 | 5 | 11,689 |
| 5 | sibling_MS_29680_MS_53745.csv | 11,014 | 6 | 11,008 |

## Selected Services for Further Analysis

We selected three representative services for detailed statistical analysis:

### 1. Sequential-Only Pattern
- **File**: `sibling_MS_53745_MS_63670.csv`
- **Observations**: 47,835 (0 concurrent, 47,835 sequential)

### 2. Mixed Pattern with Minimal Concurrency
- **File**: `sibling_MS_40123_MS_59532.csv`
- **Observations**: 17,254 (28 concurrent, 17,226 sequential)

### 3. Mixed Pattern with Significant Concurrency
- **File**: `sibling_MS_30541_MS_3533.csv`
- **Observations**: 11,776 (760 concurrent, 11,016 sequential)

## Statistical Analysis

### Chi-Square Test Results

#### Sequential-Only (sibling_MS_53745_MS_63670.csv)
```
Contingency Table:
execution_order  sequential
um                         
MS_63670              47835

Chi-square statistic: 0.0000
Degrees of freedom: 0
p-value: 1.0000e+00
Conclusion: No significant association between `um` and `execution_order`.
```

#### Mixed Execution Patterns With Minimal Concurrency(sibling_MS_40123_MS_59532.csv)
```
Contingency Table:
execution_order  concurrent  sequential
um                                     
MS_13853                  0        1698
MS_25766                  0           1
MS_5897                  28       15527

Chi-square statistic: 3.0633
Degrees of freedom: 2
p-value: 2.1618e-01
Conclusion: No significant association between `um` and `execution_order`.
```

#### Mixed Execution with Strong Concurrency Signals (sibling_MS_30541_MS_3533.csv)
```
Contingency Table:
execution_order  concurrent  sequential
um                                     
MS_27473                  4           4
MS_27503                  2           6
MS_29421                  4         910
MS_31205                  0         260
MS_34969                  0           2
MS_40387                  0           6
MS_45073                  0          22
MS_46619                  0           2
MS_52394                232        8627
MS_52588                  0           1
MS_60503                  4         151
MS_65288                  0           1
MS_9638                 514        1024

Chi-square statistic: 2176.9108
Degrees of freedom: 12
p-value: 0.0000e+00
Conclusion: There is a statistically significant association between `um` and `execution_order`.
```

As it showed significant association between upstream services and execution patterns, we performed logistic regression to identify factors influencing execution order.

We grouped traces by upstream microservice (um) and ran regression analysis on:
- contextual_MS_29421
- contextual_MS_52394
- contextual_MS_9638

##### MS_9638 Results
```
Running regression analysis on output/contextual_MS_9638.csv ...
===== Logistic Regression Results =====
Intercept: 0.6718
Coefficient for dm1_system_load: 0.1075
Coefficient for dm1_mcr: 1.4389e-07
Accuracy (R² Score): 0.6658

Odds Ratio for dm1_system_load: 1.1135
Odds Ratio for dm1_mcr: 1.0000
=======================================
Avg dm1_system_lag: 28717.6313 ± 17620.4911
Avg dm1_mcr_lag: 28717.6313 ± 17620.4911
```

For the sibling pair (MS_30541, MS_3533), the regression suggests a likely causal dependency when invoked by the parent service MS_9638.

##### MS_29421 Results
```
Running regression analysis on output/contextual_MS_29421.csv ...
===== Logistic Regression Results =====
Intercept: 5.4375
Coefficient for dm1_system_load: 0.0071
Coefficient for dm1_mcr: -7.2571e-08
Accuracy (R² Score): 0.9956

Odds Ratio for dm1_system_load: 1.0072
Odds Ratio for dm1_mcr: 1.0000
=======================================
Avg dm1_system_lag: 29371.5295 ± 18974.6237
Avg dm1_mcr_lag: 29371.5295 ± 18974.6237
```
For the sibling pair (MS_30541, MS_3533), the regression suggests a likely causal dependency when invoked by the parent service MS_29421.


##### MS_52394 Results
```
Running regression analysis on output/contextual_MS_52394.csv ...
===== Logistic Regression Results =====
Intercept: 3.5229
Coefficient for dm1_system_load: 0.5590
Coefficient for dm1_mcr: 7.3447e-07
Accuracy (R² Score): 0.9738

Odds Ratio for dm1_system_load: 1.7489
Odds Ratio for dm1_mcr: 1.0000
=======================================
Avg dm1_system_lag: 31618.3564 ± 16830.2153
Avg dm1_mcr_lag: 31618.3564 ± 16830.2153
```

A positive coefficient of 0.5590 for dm1_system_load and an odds ratio of 1.7489 means that as system load increases, the likelihood of concurrent execution increases — which is consistent with the idea of resource contention causing overlaps in execution, rather than intentional sequentiality.

A coefficient near zero and odds ratio ≈ 1.0000 implies no meaningful influence from dm1_mcr on execution order.

For the sibling pair (MS_30541, MS_3533), the regression suggests that concurrent execution under the parent service MS_52394 is more likely driven by resource contention (e.g., system load), rather than a causal or sequential dependency between the two services.


## Appendix: Contextual Information Extraction

### Methodology
Given project scope limitations, we extracted only:
- System load (CPU, memory)
- Call rates (MCR) of sibling services

The process matched service names and timestamps using pre-built indexes:
1. Extracted microservice names (dm1, dm2) and start times
2. Aligned timestamps to the nearest 60-second interval
3. Used pre-built indexes (metrics_index and mcr_index) to:
   - Search for CPU and memory utilization from MSMetrics
   - Search for MCR values from MSRTMCR
   - Expanded search ±5 intervals when exact matches were unavailable
4. Calculated time lag between matched records and original timestamps
5. Returned contextual values (CPU, memory, MCR, lags) for each downstream microservice
6. Saved enriched results to new CSV files

Note: Due to data limitations, some contextual information is approximated:
- CPU and memory metrics may not represent the exact same instance (msinstanceid differs)
- Values represent averages across multiple containers of the same microservice
- Time lags exist between recorded events and contextual metrics