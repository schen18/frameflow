# FrameFlow

Unified data transformation framework for Polars lazyframes and PySpark with JSON-based pipeline orchestration. This enables interoperability and ease of migration as the same JSON pipeline config to be used in Python as well as PySpark jobs.

In addition, data transformations are defined via JSON configs instead of being hardcoded, thus ensuring consistency in data transformations and insulating code from dataset / schema changes (as JSON configs just need to be updated and no code needs to be rewritten).

## Features

- **Consistent API**: Same function names and parameters across Polars and PySpark
- **JSON Pipelines**: Define complex transformations declaratively
- **Expression Parsing**: Use `col('name')` instead of `pl.col()` or `F.col()` in JSON
- **GCS Support**: Read/write from Google Cloud Storage
- **Type Safety**: Full type hints and documentation

## Installation

```bash
# Install with Polars support
pip install frameflow[polars]

# Install with PySpark support
pip install frameflow[pyspark]

# Install with both
pip install frameflow[all]

# Install with GCS support
pip install frameflow[polars,gcs]
```

## Quick Start

### Using Polars

```python
from frameflow.polars import parq_read, cols_fillna, df_filter, parq_write

# Read data
df = parq_read('data.parquet')

# Transform
df = cols_fillna(df, ['amount'], strategy='zero')
df = df_filter(df, pl.col('amount') > 100)

# Write
parq_write(df, 'output.parquet')
```

### Using PySpark

```python
from frameflow.pyspark import parq_read, cols_fillna, df_filter, parq_write
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("myapp").getOrCreate()

# Read data
df = parq_read('data.parquet')

# Transform
df = cols_fillna(df, ['amount'], strategy='zero')
df = df_filter(df, F.col('amount') > 100)

# Write
parq_write(df, 'output.parquet')
```

### Using JSON Pipelines

```python
from frameflow.polars.pipeline import run_pipeline

# Define pipeline
spec = {
    "input": "data.csv",
    "steps": [
        {
            "operation": "cols_fillna",
            "params": {"cols": "amount", "strategy": "zero"}
        },
        {
            "operation": "df_filter",
            "params": {"condition": "col('amount') > 100"}
        },
        {
            "operation": "cols_collect_to_array",
            "params": {
                "group_by": "customer_id",
                "collect_cols": {
                    "date": "all_dates",
                    "amount": "all_amounts"
                }
            }
        }
    ],
    "output": "output.parquet"
}

# Execute
run_pipeline(spec)
```

### GCS Support

```python
from frameflow.polars import parq_read, parq_write

# Read from GCS
df = parq_read('gs://my-bucket/data.parquet')

# Write to GCS
parq_write(df, 'gs://my-bucket/output.parquet')
```

## Available Operations

### File I/O
- `parq_read`, `parq_write`
- `csv_read`, `csv_write`
- `json_read`, `json_write`
- `json_load`, `json_dump`

### Column Operations
- `cols_drop`, `cols_select`, `cols_fillna`, `cols_rename`
- `cols_split`, `cols_split_to_array`, `cols_concat`
- `cols_todate`, `cols_tobool`, `cols_toint`, `cols_tofloat`, `cols_tostr`
- `cols_date_diff`, `cols_collect_to_array`

### DataFrame Operations
- `df_filter`, `df_groupby`, `df_join`, `df_sort`, `df_unique`
- `df_with_columns`, `df_pivot`, `df_unpivot`, `df_sql`
- `df_sample`, `df_head`, `df_tail`, `df_collect`, `df_describe`

### PySpark Window Operations
- `df_window_rank`, `df_window_row_number`
- `df_window_lag`, `df_window_lead`, `df_window_cumsum`

## Expression Syntax in JSON

```json
{
  "operation": "df_filter",
  "params": {
    "condition": "col('amount') > 100"
  }
}

{
  "operation": "df_with_columns",
  "params": {
    "exprs": {
      "total": "col('price') * col('quantity')",
      "year": "year(col('date'))"
    }
  }
}
```

# Version 1.5.0
Updated pipeline modules to accept DataFrames directly as input. Here's what changed and how to use it:

## Key Changes:

### 1. **Accept DataFrame/LazyFrame as Input**

Both pipeline classes now accept:

- File paths (string)
- File configuration (dict)
- **DataFrame/LazyFrame objects directly** ← NEW!

### 2. **Flexible Validation**

If you pass a DataFrame as input, the `steps` field becomes optional (defaults to empty list).

## Usage Examples:

### **Polars - Transform Existing DataFrame**

python

```python
import polars as pl
from dataops.polars.pipeline import run_pipeline

# Create or load a DataFrame
df = pl.DataFrame({
    'customer_id': [1, 1, 2, 2, 3],
    'amount': [100, 200, 150, None, 300],
    'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-02', '2024-01-01']
})

# Define transformations using JSON spec
spec = {
    'input': df,  # Pass DataFrame directly!
    'steps': [
        {
            'operation': 'cols_fillna',
            'params': {'cols': 'amount', 'strategy': 'zero'}
        },
        {
            'operation': 'cols_todate',
            'params': {'cols': 'date'}
        },
        {
            'operation': 'df_filter',
            'params': {'condition': "col('amount') > 0"}
        },
        {
            'operation': 'cols_collect_to_array',
            'params': {
                'group_by': 'customer_id',
                'collect_cols': {
                    'date': 'all_dates',
                    'amount': 'all_amounts'
                }
            }
        }
    ],
    'output': 'output.parquet'  # Optional
}

# Execute pipeline
result = run_pipeline(spec, return_df=True)
print(result)
```

### **PySpark - Transform Existing DataFrame**

python

```python
from pyspark.sql import SparkSession
from dataops.pyspark.pipeline import run_pipeline

spark = SparkSession.builder.appName("myapp").getOrCreate()

# Create or load a DataFrame
df = spark.createDataFrame([
    (1, 100, '2024-01-01'),
    (1, 200, '2024-01-02'),
    (2, 150, '2024-01-01'),
    (2, None, '2024-01-02'),
    (3, 300, '2024-01-01')
], ['customer_id', 'amount', 'date'])

# Define transformations using JSON spec
spec = {
    'input': df,  # Pass DataFrame directly!
    'steps': [
        {
            'operation': 'cols_fillna',
            'params': {'cols': 'amount', 'strategy': 'zero'}
        },
        {
            'operation': 'cols_todate',
            'params': {'cols': 'date', 'format': 'yyyy-MM-dd'}
        },
        {
            'operation': 'df_filter',
            'params': {'condition': "col('amount') > 0"}
        },
        {
            'operation': 'cols_collect_to_array',
            'params': {
                'group_by': 'customer_id',
                'collect_cols': {
                    'date': 'all_dates',
                    'amount': 'all_amounts'
                }
            }
        }
    ]
}

# Execute pipeline
result = run_pipeline(spec, return_df=True)
result.show()
```

## Benefits:

✅ **Reuse transformation specs** - Same JSON for file-based or in-memory data  
✅ **Consistent transformations** - Guarantee same logic across sources  
✅ **Mix and match** - Load from file, transform, pass to another pipeline  
✅ **Testing friendly** - Easy to test transformations on sample DataFrames  
✅ **Composable** - Chain multiple pipelines together

## Example: Chaining Pipelines

python

```python
# Pipeline 1: Load and clean
spec1 = {
    'input': 'raw_data.csv',
    'steps': [
        {'operation': 'cols_fillna', 'params': {'cols': 'amount', 'strategy': 'zero'}},
        {'operation': 'cols_todate', 'params': {'cols': 'date'}}
    ]
}
df_clean = run_pipeline(spec1, return_df=True)

# Pipeline 2: Aggregate (reuse spec for consistency)
spec2 = {
    'input': df_clean,  # Use output from previous pipeline
    'steps': [
        {
            'operation': 'cols_collect_to_array',
            'params': {
                'group_by': 'customer_id',
                'collect_cols': {'date': 'dates', 'amount': 'amounts'}
            }
        }
    ],
    'output': 'aggregated.parquet'
}
run_pipeline(spec2)
```

Now JSON specs work identically whether data comes from files or DataFrames!

## Documentation

Full documentation available at: https://github.com/schen18/frameflow

## License

MIT License