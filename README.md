# FrameFlow

Unified data transformation framework for Polars and PySpark with JSON-based pipeline orchestration.

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

## Documentation

Full documentation available at: https://github.com/schen18/frameflow

## License

MIT License
'''