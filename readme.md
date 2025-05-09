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
