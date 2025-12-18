"""
FrameFlow PySpark DataFrame abstraction module for standardized dataframe operations.

This module provides a consistent interface for common dataframe transformations
using PySpark DataFrames (which use lazy evaluation by design). All functions 
follow the pattern:
    function(df: DataFrame, **parameters) -> DataFrame

GCS Support:
    For GCS operations, configure your SparkSession with GCS connector:
    spark = SparkSession.builder \
        .config("spark.jars", "gcs-connector-hadoop3-latest.jar") \
        .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("fs.gs.auth.service.account.enable", "true") \
        .config("google.cloud.auth.service.account.json.keyfile", "/path/to/key.json") \
        .getOrCreate()

Author: Claude
Version: 1.0.0
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *
from typing import Union, List, Dict, Any, Optional
from pathlib import Path
import os


# ============================================================================
# FILE I/O OPERATIONS
# ============================================================================

def _get_spark() -> SparkSession:
    """Get or create SparkSession."""
    return SparkSession.builder.getOrCreate()


def parq_read(path: Union[str, Path], **kwargs) -> DataFrame:
    """Read Parquet file(s) as DataFrame from local or GCS.
    
    Args:
        path: File path, glob pattern, or GCS URI (gs://bucket/path/file.parquet)
        **kwargs: Additional arguments passed to spark.read.parquet
        
    Returns:
        DataFrame
        
    Examples:
        # Local file
        df = parq_read('data.parquet')
        
        # GCS
        df = parq_read('gs://my-bucket/data.parquet')
        
        # Multiple files
        df = parq_read('gs://my-bucket/data/*.parquet')
    """
    spark = _get_spark()
    return spark.read.parquet(str(path), **kwargs)


def parq_write(df: DataFrame, path: Union[str, Path], mode: str = 'overwrite', **kwargs) -> None:
    """Write DataFrame to Parquet file on local or GCS.
    
    Args:
        df: Input DataFrame
        path: Output file path or GCS URI (gs://bucket/path/file.parquet)
        mode: Write mode - 'overwrite', 'append', 'ignore', 'error'
        **kwargs: Additional arguments passed to write.parquet
        
    Examples:
        # Local file
        parq_write(df, 'output.parquet')
        
        # GCS with partitioning
        parq_write(df, 'gs://my-bucket/output.parquet', 
                   partitionBy=['year', 'month'])
    """
    df.write.mode(mode).parquet(str(path), **kwargs)


def csv_read(path: Union[str, Path], header: bool = True, inferSchema: bool = True, **kwargs) -> DataFrame:
    """Read CSV file(s) as DataFrame from local or GCS.
    
    Args:
        path: File path, glob pattern, or GCS URI (gs://bucket/path/file.csv)
        header: Whether first row is header
        inferSchema: Whether to infer column types
        **kwargs: Additional arguments passed to spark.read.csv
        
    Returns:
        DataFrame
        
    Examples:
        # Local file
        df = csv_read('data.csv')
        
        # GCS with custom delimiter
        df = csv_read('gs://my-bucket/data.csv', sep='|')
    """
    spark = _get_spark()
    return spark.read.csv(str(path), header=header, inferSchema=inferSchema, **kwargs)


def csv_write(df: DataFrame, path: Union[str, Path], mode: str = 'overwrite', 
              header: bool = True, **kwargs) -> None:
    """Write DataFrame to CSV file on local or GCS.
    
    Args:
        df: Input DataFrame
        path: Output file path or GCS URI (gs://bucket/path/file.csv)
        mode: Write mode - 'overwrite', 'append', 'ignore', 'error'
        header: Whether to write header row
        **kwargs: Additional arguments passed to write.csv
        
    Examples:
        # Local file
        csv_write(df, 'output.csv')
        
        # GCS
        csv_write(df, 'gs://my-bucket/output.csv')
    """
    df.write.mode(mode).csv(str(path), header=header, **kwargs)


def json_read(path: Union[str, Path], **kwargs) -> DataFrame:
    """Read JSON file(s) as DataFrame from local or GCS.
    
    Args:
        path: File path or GCS URI (gs://bucket/path/file.json)
        **kwargs: Additional arguments passed to spark.read.json
        
    Returns:
        DataFrame
        
    Examples:
        # Local file
        df = json_read('data.json')
        
        # GCS
        df = json_read('gs://my-bucket/data.json')
    """
    spark = _get_spark()
    return spark.read.json(str(path), **kwargs)


def json_write(df: DataFrame, path: Union[str, Path], mode: str = 'overwrite', **kwargs) -> None:
    """Write DataFrame to JSON file on local or GCS.
    
    Args:
        df: Input DataFrame
        path: Output file path or GCS URI (gs://bucket/path/file.json)
        mode: Write mode - 'overwrite', 'append', 'ignore', 'error'
        **kwargs: Additional arguments passed to write.json
        
    Examples:
        # Local file
        json_write(df, 'output.json')
        
        # GCS
        json_write(df, 'gs://my-bucket/output.json')
    """
    df.write.mode(mode).json(str(path), **kwargs)


def json_load(path: Union[str, Path]) -> Union[Dict, List]:
    """Load JSON file to Python dict/list from local or GCS.
    
    Args:
        path: File path or GCS URI (gs://bucket/path/file.json)
        
    Returns:
        Python dict or list depending on JSON structure
        
    Examples:
        # Local file
        data = json_load('config.json')
        
        # GCS
        data = json_load('gs://my-bucket/config.json')
    """
    import json
    
    path = str(path)
    
    if path.startswith('gs://') or path.startswith('gcs://'):
        try:
            import fsspec
        except ImportError:
            raise ImportError("fsspec is required for GCS operations. Install with: pip install fsspec gcsfs")
        
        with fsspec.open(path, 'r') as f:
            return json.load(f)
    else:
        with open(path, 'r') as f:
            return json.load(f)


def json_dump(obj: Union[Dict, List], path: Union[str, Path], **kwargs) -> None:
    """Write Python dict/list to JSON file on local or GCS.
    
    Args:
        obj: Python dict or list to write
        path: Output file path or GCS URI (gs://bucket/path/file.json)
        **kwargs: Additional arguments passed to json.dump
        
    Examples:
        # Local file
        data = {'key': 'value'}
        json_dump(data, 'output.json', indent=2)
        
        # GCS
        json_dump(data, 'gs://my-bucket/output.json', indent=2)
    """
    import json
    
    path = str(path)
    
    if path.startswith('gs://') or path.startswith('gcs://'):
        try:
            import fsspec
        except ImportError:
            raise ImportError("fsspec is required for GCS operations. Install with: pip install fsspec gcsfs")
        
        with fsspec.open(path, 'w') as f:
            json.dump(obj, f, **kwargs)
    else:
        with open(path, 'w') as f:
            json.dump(obj, f, **kwargs)


# ============================================================================
# COLUMN OPERATIONS
# ============================================================================

def cols_drop(df: DataFrame, cols: Union[str, List[str]]) -> DataFrame:
    """Drop one or more columns.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to drop
        
    Returns:
        DataFrame with columns dropped
    """
    cols = [cols] if isinstance(cols, str) else cols
    return df.drop(*cols)


def cols_select(df: DataFrame, cols: Union[str, List[str]]) -> DataFrame:
    """Select one or more columns.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to select
        
    Returns:
        DataFrame with selected columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    return df.select(*cols)


def cols_fillna(df: DataFrame, cols: Union[str, List[str]], value: Any = None, 
                strategy: Optional[str] = None) -> DataFrame:
    """Fill null values in columns.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to fill
        value: Value to fill nulls with (if strategy is None)
        strategy: Fill strategy - 'forward', 'backward', 'mean', 'min', 'max', 'zero', 'one'
        
    Returns:
        DataFrame with nulls filled
    """
    cols = [cols] if isinstance(cols, str) else cols
    
    if strategy == 'forward':
        # Forward fill using Window function
        window = Window.orderBy(F.monotonically_increasing_id()).rowsBetween(Window.unboundedPreceding, 0)
        for col in cols:
            df = df.withColumn(col, F.last(col, ignorenulls=True).over(window))
        return df
    elif strategy == 'backward':
        # Backward fill using Window function
        window = Window.orderBy(F.monotonically_increasing_id()).rowsBetween(0, Window.unboundedFollowing)
        for col in cols:
            df = df.withColumn(col, F.first(col, ignorenulls=True).over(window))
        return df
    elif strategy == 'mean':
        for col in cols:
            mean_val = df.select(F.mean(col)).first()[0]
            df = df.fillna({col: mean_val})
        return df
    elif strategy == 'min':
        for col in cols:
            min_val = df.select(F.min(col)).first()[0]
            df = df.fillna({col: min_val})
        return df
    elif strategy == 'max':
        for col in cols:
            max_val = df.select(F.max(col)).first()[0]
            df = df.fillna({col: max_val})
        return df
    elif strategy == 'zero':
        return df.fillna({col: 0 for col in cols})
    elif strategy == 'one':
        return df.fillna({col: 1 for col in cols})
    else:
        return df.fillna({col: value for col in cols})


def cols_rename(df: DataFrame, mapping: Dict[str, str]) -> DataFrame:
    """Rename columns using a mapping dictionary.
    
    Args:
        df: Input DataFrame
        mapping: Dictionary mapping old names to new names
        
    Returns:
        DataFrame with renamed columns
    """
    for old_name, new_name in mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    return df


def cols_split(df: DataFrame, col: str, separator: str, 
               new_cols: Optional[List[str]] = None) -> DataFrame:
    """Split a string column into multiple columns.
    
    Args:
        df: Input DataFrame
        col: Column to split
        separator: String separator
        new_cols: Names for new columns (if None, uses col_1, col_2, etc.)
        
    Returns:
        DataFrame with split columns
    """
    split_col = F.split(F.col(col), separator)
    
    if new_cols is None:
        # Auto-generate column names
        for i in range(10):  # Arbitrary limit
            df = df.withColumn(f"{col}_{i+1}", split_col.getItem(i))
    else:
        for i, new_col in enumerate(new_cols):
            df = df.withColumn(new_col, split_col.getItem(i))
    
    return df


def cols_split_to_array(df: DataFrame, col: str, separator: str, 
                        new_col: Optional[str] = None) -> DataFrame:
    """Split a string column into an array column.
    
    Args:
        df: Input DataFrame
        col: Column to split
        separator: String separator
        new_col: Name for new array column (if None, uses original col name)
        
    Returns:
        DataFrame with array column
        
    Example:
        df with col "tags" = "python,data,ml"
        -> df with col "tags" = ["python", "data", "ml"]
    """
    new_col = new_col or col
    return df.withColumn(new_col, F.split(F.col(col), separator))


def cols_concat(df: DataFrame, cols: List[str], new_col: str, 
                separator: str = "") -> DataFrame:
    """Concatenate multiple string columns into one.
    
    Args:
        df: Input DataFrame
        cols: Columns to concatenate
        new_col: Name of new concatenated column
        separator: String separator between values
        
    Returns:
        DataFrame with concatenated column
    """
    if separator:
        concat_expr = F.concat_ws(separator, *[F.col(c) for c in cols])
    else:
        concat_expr = F.concat(*[F.col(c) for c in cols])
    
    return df.withColumn(new_col, concat_expr)


def cols_todate(df: DataFrame, cols: Union[str, List[str]], 
                format: Optional[str] = None) -> DataFrame:
    """Convert columns to date/timestamp type.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to convert
        format: Date format string (if None, auto-infers)
        
    Returns:
        DataFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    
    for col in cols:
        if format:
            df = df.withColumn(col, F.to_timestamp(F.col(col), format))
        else:
            df = df.withColumn(col, F.to_timestamp(F.col(col)))
    
    return df


def cols_tobool(df: DataFrame, cols: Union[str, List[str]]) -> DataFrame:
    """Convert columns to boolean type.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to convert
        
    Returns:
        DataFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    for col in cols:
        df = df.withColumn(col, F.col(col).cast(BooleanType()))
    return df


def cols_toint(df: DataFrame, cols: Union[str, List[str]]) -> DataFrame:
    """Convert columns to integer type.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to convert
        
    Returns:
        DataFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    for col in cols:
        df = df.withColumn(col, F.col(col).cast(IntegerType()))
    return df


def cols_tofloat(df: DataFrame, cols: Union[str, List[str]]) -> DataFrame:
    """Convert columns to float type.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to convert
        
    Returns:
        DataFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    for col in cols:
        df = df.withColumn(col, F.col(col).cast(DoubleType()))
    return df


def cols_tostr(df: DataFrame, cols: Union[str, List[str]]) -> DataFrame:
    """Convert columns to string type.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to convert
        
    Returns:
        DataFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    for col in cols:
        df = df.withColumn(col, F.col(col).cast(StringType()))
    return df


def cols_date_diff(df: DataFrame, col1: str, col2: str, new_col: str, 
                   unit: str = 'days') -> DataFrame:
    """Calculate difference between two date/timestamp columns.
    
    Args:
        df: Input DataFrame
        col1: First date column (subtracted from)
        col2: Second date column (subtracted)
        new_col: Name for new column with difference
        unit: Unit of difference - 'days', 'hours', 'minutes', 'seconds'
        
    Returns:
        DataFrame with new difference column
        
    Example:
        Calculate days between dates: cols_date_diff(df, 'end_date', 'start_date', 'duration', 'days')
    """
    if unit == 'days':
        diff_expr = F.datediff(F.col(col1), F.col(col2))
    elif unit == 'hours':
        diff_expr = (F.unix_timestamp(F.col(col1)) - F.unix_timestamp(F.col(col2))) / 3600
    elif unit == 'minutes':
        diff_expr = (F.unix_timestamp(F.col(col1)) - F.unix_timestamp(F.col(col2))) / 60
    elif unit == 'seconds':
        diff_expr = F.unix_timestamp(F.col(col1)) - F.unix_timestamp(F.col(col2))
    else:
        raise ValueError(f"Invalid unit: {unit}. Supported: days, hours, minutes, seconds")
    
    return df.withColumn(new_col, diff_expr)


def cols_collect_to_array(df: DataFrame, group_by: Union[str, List[str]], 
                          collect_cols: Union[str, List[str], Dict[str, str]],
                          sort_by: Optional[Union[str, List[str]]] = None,
                          sort_descending: bool = False) -> DataFrame:
    """Collect column values into array columns grouped by key column(s).
    
    Args:
        df: Input DataFrame
        group_by: Column(s) to group by (e.g., 'customer_id')
        collect_cols: Column(s) to collect into arrays. Can be:
            - Single column name: collected as-is
            - List of columns: each collected separately
            - Dict mapping old names to new array column names
        sort_by: Column(s) to sort by before collecting (maintains order in arrays)
        sort_descending: Sort order for sort_by
        
    Returns:
        DataFrame with one row per group and array columns
        
    Example:
        # Multiple transactions per customer -> one row per customer with arrays
        df = cols_collect_to_array(
            df, 
            group_by='customer_id',
            collect_cols={
                'transaction_date': 'all_dates',
                'transaction_type': 'all_types', 
                'amount': 'all_amounts'
            },
            sort_by='transaction_date'
        )
    """
    group_by = [group_by] if isinstance(group_by, str) else group_by
    
    # Sort first if specified
    if sort_by is not None:
        sort_by = [sort_by] if isinstance(sort_by, str) else sort_by
        df = df.orderBy(*[F.col(c).desc() if sort_descending else F.col(c) for c in sort_by])
    
    # Build aggregation expressions
    agg_exprs = []
    
    if isinstance(collect_cols, dict):
        # Dict mapping: old_name -> new_name
        for old_col, new_col in collect_cols.items():
            agg_exprs.append(F.collect_list(old_col).alias(new_col))
    else:
        # Single column or list of columns
        collect_cols = [collect_cols] if isinstance(collect_cols, str) else collect_cols
        for col in collect_cols:
            agg_exprs.append(F.collect_list(col).alias(col))
    
    return df.groupBy(*group_by).agg(*agg_exprs)


# ============================================================================
# DATAFRAME OPERATIONS
# ============================================================================

def df_filter(df: DataFrame, condition: Union[str, Any]) -> DataFrame:
    """Filter rows based on condition.
    
    Args:
        df: Input DataFrame
        condition: Filter condition (PySpark column expression or SQL string)
        
    Returns:
        Filtered DataFrame
    """
    return df.filter(condition)


def df_groupby(df: DataFrame, by: Union[str, List[str]], 
               agg: Dict[str, Union[str, List[str]]]) -> DataFrame:
    """Group by columns and aggregate.
    
    Args:
        df: Input DataFrame
        by: Column(s) to group by
        agg: Dictionary mapping column names to aggregation functions
             e.g., {'col1': 'sum', 'col2': ['mean', 'max']}
        
    Returns:
        Aggregated DataFrame
    """
    by = [by] if isinstance(by, str) else by
    
    agg_exprs = []
    for col, funcs in agg.items():
        funcs = [funcs] if isinstance(funcs, str) else funcs
        for func in funcs:
            if func == 'sum':
                agg_exprs.append(F.sum(col).alias(f"{col}_{func}"))
            elif func == 'mean' or func == 'avg':
                agg_exprs.append(F.mean(col).alias(f"{col}_{func}"))
            elif func == 'min':
                agg_exprs.append(F.min(col).alias(f"{col}_{func}"))
            elif func == 'max':
                agg_exprs.append(F.max(col).alias(f"{col}_{func}"))
            elif func == 'count':
                agg_exprs.append(F.count(col).alias(f"{col}_{func}"))
            elif func == 'std' or func == 'stddev':
                agg_exprs.append(F.stddev(col).alias(f"{col}_{func}"))
            elif func == 'var' or func == 'variance':
                agg_exprs.append(F.variance(col).alias(f"{col}_{func}"))
            elif func == 'first':
                agg_exprs.append(F.first(col).alias(f"{col}_{func}"))
            elif func == 'last':
                agg_exprs.append(F.last(col).alias(f"{col}_{func}"))
            elif func == 'median':
                agg_exprs.append(F.expr(f"percentile_approx({col}, 0.5)").alias(f"{col}_{func}"))
            elif func == 'n_unique' or func == 'countDistinct':
                agg_exprs.append(F.countDistinct(col).alias(f"{col}_{func}"))
    
    return df.groupBy(*by).agg(*agg_exprs)


def df_join(df: DataFrame, other: DataFrame, on: Union[str, List[str]], 
            how: str = 'inner', suffix: str = '_right') -> DataFrame:
    """Join two DataFrames.
    
    Args:
        df: Left DataFrame
        other: Right DataFrame
        on: Column(s) to join on
        how: Join type - 'inner', 'left', 'right', 'outer', 'cross', 'semi', 'anti'
        suffix: Suffix for overlapping column names from right frame
        
    Returns:
        Joined DataFrame
    """
    on = [on] if isinstance(on, str) else on
    
    # Handle duplicate column names
    if suffix:
        # Rename overlapping columns in right DataFrame
        overlapping = set(df.columns).intersection(set(other.columns)) - set(on)
        for col in overlapping:
            other = other.withColumnRenamed(col, f"{col}{suffix}")
    
    return df.join(other, on=on, how=how)


def df_sort(df: DataFrame, by: Union[str, List[str]], 
            descending: Union[bool, List[bool]] = False) -> DataFrame:
    """Sort DataFrame by column(s).
    
    Args:
        df: Input DataFrame
        by: Column(s) to sort by
        descending: Sort order (can be per-column if list)
        
    Returns:
        Sorted DataFrame
    """
    by = [by] if isinstance(by, str) else by
    
    if isinstance(descending, bool):
        descending = [descending] * len(by)
    
    sort_cols = [F.col(c).desc() if desc else F.col(c) for c, desc in zip(by, descending)]
    return df.orderBy(*sort_cols)


def df_unique(df: DataFrame, subset: Optional[Union[str, List[str]]] = None) -> DataFrame:
    """Remove duplicate rows.
    
    Args:
        df: Input DataFrame
        subset: Column(s) to consider for duplicates (None = all columns)
        
    Returns:
        DataFrame with duplicates removed
    """
    if subset is not None:
        subset = [subset] if isinstance(subset, str) else subset
        return df.dropDuplicates(subset)
    return df.dropDuplicates()


def df_with_columns(df: DataFrame, exprs: Dict[str, Any]) -> DataFrame:
    """Add or modify columns using expressions.
    
    Args:
        df: Input DataFrame
        exprs: Dictionary mapping new column names to PySpark column expressions
        
    Returns:
        DataFrame with new/modified columns
    """
    for name, expr in exprs.items():
        df = df.withColumn(name, expr)
    return df


def df_pivot(df: DataFrame, index: Union[str, List[str]], 
             columns: str, values: Union[str, List[str]], 
             aggregate_function: str = 'first') -> DataFrame:
    """Pivot DataFrame from long to wide format.
    
    Args:
        df: Input DataFrame
        index: Column(s) to use as index
        columns: Column to pivot into new columns
        values: Column(s) containing values
        aggregate_function: How to aggregate duplicates
        
    Returns:
        Pivoted DataFrame
    """
    index = [index] if isinstance(index, str) else index
    values = [values] if isinstance(values, str) else values
    
    # Map function names to PySpark functions
    agg_map = {
        'first': 'first',
        'sum': 'sum',
        'mean': 'avg',
        'avg': 'avg',
        'min': 'min',
        'max': 'max',
        'count': 'count'
    }
    
    agg_func = agg_map.get(aggregate_function, 'first')
    
    pivot_df = df.groupBy(*index).pivot(columns)
    
    # Apply aggregation to each value column
    agg_exprs = []
    for val_col in values:
        agg_exprs.append(getattr(F, agg_func)(val_col))
    
    return pivot_df.agg(*agg_exprs)


def df_unpivot(df: DataFrame, id_vars: Union[str, List[str]], 
               value_vars: Optional[Union[str, List[str]]] = None,
               variable_name: str = 'variable', 
               value_name: str = 'value') -> DataFrame:
    """Unpivot DataFrame from wide to long format.
    
    Args:
        df: Input DataFrame
        id_vars: Column(s) to keep as identifiers
        value_vars: Column(s) to unpivot (None = all except id_vars)
        variable_name: Name for the variable column
        value_name: Name for the value column
        
    Returns:
        Unpivoted DataFrame
    """
    id_vars = [id_vars] if isinstance(id_vars, str) else id_vars
    
    if value_vars is None:
        value_vars = [c for c in df.columns if c not in id_vars]
    else:
        value_vars = [value_vars] if isinstance(value_vars, str) else value_vars
    
    # Use stack function for unpivot
    expr = f"stack({len(value_vars)}"
    for col in value_vars:
        expr += f", '{col}', `{col}`"
    expr += f") as ({variable_name}, {value_name})"
    
    return df.select(*id_vars, F.expr(expr))


def df_sql(query: str, **frames: DataFrame) -> DataFrame:
    """Execute SQL query on DataFrames.
    
    Args:
        query: SQL query string
        **frames: Named DataFrames to use in query (name=dataframe)
        
    Returns:
        Result DataFrame
        
    Example:
        result = df_sql(
            "SELECT * FROM df1 JOIN df2 ON df1.id = df2.id",
            df1=dataframe1,
            df2=dataframe2
        )
    """
    spark = _get_spark()
    
    # Register DataFrames as temporary views
    for name, frame in frames.items():
        frame.createOrReplaceTempView(name)
    
    return spark.sql(query)


def df_sample(df: DataFrame, fraction: Optional[float] = None, 
              n: Optional[int] = None,
              with_replacement: bool = False, 
              seed: Optional[int] = None) -> DataFrame:
    """Sample rows from DataFrame.
    
    Args:
        df: Input DataFrame
        fraction: Fraction of rows to sample (0.0 to 1.0)
        n: Number of rows to sample
        with_replacement: Whether to sample with replacement
        seed: Random seed for reproducibility
        
    Returns:
        Sampled DataFrame
    """
    if n is not None:
        # Sample exact number of rows
        total_count = df.count()
        fraction = n / total_count
        return df.sample(withReplacement=with_replacement, fraction=fraction, seed=seed)
    elif fraction is not None:
        return df.sample(withReplacement=with_replacement, fraction=fraction, seed=seed)
    else:
        raise ValueError("Either 'n' or 'fraction' must be specified")


def df_head(df: DataFrame, n: int = 5) -> List:
    """Get first n rows.
    
    Args:
        df: Input DataFrame
        n: Number of rows to return
        
    Returns:
        List of Row objects
    """
    return df.head(n)


def df_tail(df: DataFrame, n: int = 5) -> List:
    """Get last n rows.
    
    Args:
        df: Input DataFrame
        n: Number of rows to return
        
    Returns:
        List of Row objects
    """
    return df.tail(n)


def df_collect(df: DataFrame):
    """Collect DataFrame (trigger execution and return as list).
    
    Args:
        df: Input DataFrame
        
    Returns:
        List of Row objects
    """
    return df.collect()


def df_show(df: DataFrame, n: int = 20, truncate: bool = True) -> None:
    """Display DataFrame.
    
    Args:
        df: Input DataFrame
        n: Number of rows to show
        truncate: Whether to truncate long values
    """
    df.show(n=n, truncate=truncate)


def df_describe(df: DataFrame) -> DataFrame:
    """Get summary statistics.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with summary statistics
    """
    return df.describe()


def df_count(df: DataFrame) -> int:
    """Count rows in DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Number of rows
    """
    return df.count()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def df_schema(df: DataFrame) -> StructType:
    """Get schema of DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        StructType schema object
    """
    return df.schema


def df_print_schema(df: DataFrame) -> None:
    """Print schema of DataFrame.
    
    Args:
        df: Input DataFrame
    """
    df.printSchema()


def df_columns(df: DataFrame) -> List[str]:
    """Get column names of DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        List of column names
    """
    return df.columns


def df_dtypes(df: DataFrame) -> List[tuple]:
    """Get column names and types.
    
    Args:
        df: Input DataFrame
        
    Returns:
        List of (column_name, data_type) tuples
    """
    return df.dtypes


def df_cache(df: DataFrame) -> DataFrame:
    """Cache DataFrame in memory.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Cached DataFrame
    """
    return df.cache()


def df_persist(df: DataFrame, storage_level: str = 'MEMORY_AND_DISK') -> DataFrame:
    """Persist DataFrame with specified storage level.
    
    Args:
        df: Input DataFrame
        storage_level: Storage level - 'MEMORY_ONLY', 'MEMORY_AND_DISK', 
                      'DISK_ONLY', 'MEMORY_ONLY_2', 'MEMORY_AND_DISK_2'
        
    Returns:
        Persisted DataFrame
    """
    from pyspark import StorageLevel
    
    level_map = {
        'MEMORY_ONLY': StorageLevel.MEMORY_ONLY,
        'MEMORY_AND_DISK': StorageLevel.MEMORY_AND_DISK,
        'DISK_ONLY': StorageLevel.DISK_ONLY,
        'MEMORY_ONLY_2': StorageLevel.MEMORY_ONLY_2,
        'MEMORY_AND_DISK_2': StorageLevel.MEMORY_AND_DISK_2,
    }
    
    level = level_map.get(storage_level, StorageLevel.MEMORY_AND_DISK)
    return df.persist(level)


def df_unpersist(df: DataFrame) -> DataFrame:
    """Unpersist (uncache) DataFrame.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Unpersisted DataFrame
    """
    return df.unpersist()


def df_repartition(df: DataFrame, num_partitions: int, 
                   cols: Optional[Union[str, List[str]]] = None) -> DataFrame:
    """Repartition DataFrame.
    
    Args:
        df: Input DataFrame
        num_partitions: Number of partitions
        cols: Optional column(s) to partition by
        
    Returns:
        Repartitioned DataFrame
    """
    if cols is not None:
        cols = [cols] if isinstance(cols, str) else cols
        return df.repartition(num_partitions, *cols)
    return df.repartition(num_partitions)


def df_coalesce(df: DataFrame, num_partitions: int) -> DataFrame:
    """Coalesce DataFrame to fewer partitions.
    
    Args:
        df: Input DataFrame
        num_partitions: Target number of partitions
        
    Returns:
        Coalesced DataFrame
    """
    return df.coalesce(num_partitions)


# ============================================================================
# ADDITIONAL COLUMN OPERATIONS
# ============================================================================

def cols_add(df: DataFrame, col_name: str, expr: Any) -> DataFrame:
    """Add a new column with an expression.
    
    Args:
        df: Input DataFrame
        col_name: Name of new column
        expr: PySpark column expression
        
    Returns:
        DataFrame with new column
    """
    return df.withColumn(col_name, expr)


def cols_replace(df: DataFrame, col: str, to_replace: Dict[Any, Any]) -> DataFrame:
    """Replace values in a column.
    
    Args:
        df: Input DataFrame
        col: Column name
        to_replace: Dictionary mapping old values to new values
        
    Returns:
        DataFrame with replaced values
    """
    from functools import reduce
    
    replace_expr = F.col(col)
    for old_val, new_val in to_replace.items():
        replace_expr = F.when(F.col(col) == old_val, new_val).otherwise(replace_expr)
    
    return df.withColumn(col, replace_expr)


def cols_upper(df: DataFrame, cols: Union[str, List[str]]) -> DataFrame:
    """Convert string columns to uppercase.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to convert
        
    Returns:
        DataFrame with uppercase columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    for col in cols:
        df = df.withColumn(col, F.upper(F.col(col)))
    return df


def cols_lower(df: DataFrame, cols: Union[str, List[str]]) -> DataFrame:
    """Convert string columns to lowercase.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to convert
        
    Returns:
        DataFrame with lowercase columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    for col in cols:
        df = df.withColumn(col, F.lower(F.col(col)))
    return df


def cols_trim(df: DataFrame, cols: Union[str, List[str]]) -> DataFrame:
    """Trim whitespace from string columns.
    
    Args:
        df: Input DataFrame
        cols: Column name(s) to trim
        
    Returns:
        DataFrame with trimmed columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    for col in cols:
        df = df.withColumn(col, F.trim(F.col(col)))
    return df


def cols_substring(df: DataFrame, col: str, new_col: str, 
                   start: int, length: int) -> DataFrame:
    """Extract substring from a column.
    
    Args:
        df: Input DataFrame
        col: Source column name
        new_col: New column name for substring
        start: Starting position (1-indexed)
        length: Length of substring
        
    Returns:
        DataFrame with substring column
    """
    return df.withColumn(new_col, F.substring(F.col(col), start, length))


def cols_regexp_extract(df: DataFrame, col: str, new_col: str, 
                        pattern: str, group_idx: int = 0) -> DataFrame:
    """Extract text matching a regex pattern.
    
    Args:
        df: Input DataFrame
        col: Source column name
        new_col: New column name for extracted text
        pattern: Regular expression pattern
        group_idx: Regex group index to extract
        
    Returns:
        DataFrame with extracted text column
    """
    return df.withColumn(new_col, F.regexp_extract(F.col(col), pattern, group_idx))


def cols_regexp_replace(df: DataFrame, col: str, pattern: str, 
                        replacement: str) -> DataFrame:
    """Replace text matching a regex pattern.
    
    Args:
        df: Input DataFrame
        col: Column name to modify
        pattern: Regular expression pattern
        replacement: Replacement string
        
    Returns:
        DataFrame with replaced text
    """
    return df.withColumn(col, F.regexp_replace(F.col(col), pattern, replacement))


# ============================================================================
# WINDOW OPERATIONS
# ============================================================================

def df_window_rank(df: DataFrame, partition_by: Union[str, List[str]], 
                   order_by: Union[str, List[str]], 
                   new_col: str = 'rank',
                   descending: bool = False) -> DataFrame:
    """Add rank column using window function.
    
    Args:
        df: Input DataFrame
        partition_by: Column(s) to partition by
        order_by: Column(s) to order by
        new_col: Name for rank column
        descending: Sort order
        
    Returns:
        DataFrame with rank column
    """
    partition_by = [partition_by] if isinstance(partition_by, str) else partition_by
    order_by = [order_by] if isinstance(order_by, str) else order_by
    
    window_spec = Window.partitionBy(*partition_by).orderBy(
        *[F.col(c).desc() if descending else F.col(c) for c in order_by]
    )
    
    return df.withColumn(new_col, F.rank().over(window_spec))


def df_window_row_number(df: DataFrame, partition_by: Union[str, List[str]], 
                         order_by: Union[str, List[str]], 
                         new_col: str = 'row_number',
                         descending: bool = False) -> DataFrame:
    """Add row number column using window function.
    
    Args:
        df: Input DataFrame
        partition_by: Column(s) to partition by
        order_by: Column(s) to order by
        new_col: Name for row number column
        descending: Sort order
        
    Returns:
        DataFrame with row number column
    """
    partition_by = [partition_by] if isinstance(partition_by, str) else partition_by
    order_by = [order_by] if isinstance(order_by, str) else order_by
    
    window_spec = Window.partitionBy(*partition_by).orderBy(
        *[F.col(c).desc() if descending else F.col(c) for c in order_by]
    )
    
    return df.withColumn(new_col, F.row_number().over(window_spec))


def df_window_lag(df: DataFrame, col: str, new_col: str,
                  partition_by: Union[str, List[str]], 
                  order_by: Union[str, List[str]],
                  offset: int = 1,
                  default: Any = None) -> DataFrame:
    """Add lag column using window function.
    
    Args:
        df: Input DataFrame
        col: Column to lag
        new_col: Name for lag column
        partition_by: Column(s) to partition by
        order_by: Column(s) to order by
        offset: Number of rows to lag
        default: Default value for nulls
        
    Returns:
        DataFrame with lag column
    """
    partition_by = [partition_by] if isinstance(partition_by, str) else partition_by
    order_by = [order_by] if isinstance(order_by, str) else order_by
    
    window_spec = Window.partitionBy(*partition_by).orderBy(*order_by)
    
    return df.withColumn(new_col, F.lag(F.col(col), offset, default).over(window_spec))


def df_window_lead(df: DataFrame, col: str, new_col: str,
                   partition_by: Union[str, List[str]], 
                   order_by: Union[str, List[str]],
                   offset: int = 1,
                   default: Any = None) -> DataFrame:
    """Add lead column using window function.
    
    Args:
        df: Input DataFrame
        col: Column to lead
        new_col: Name for lead column
        partition_by: Column(s) to partition by
        order_by: Column(s) to order by
        offset: Number of rows to lead
        default: Default value for nulls
        
    Returns:
        DataFrame with lead column
    """
    partition_by = [partition_by] if isinstance(partition_by, str) else partition_by
    order_by = [order_by] if isinstance(order_by, str) else order_by
    
    window_spec = Window.partitionBy(*partition_by).orderBy(*order_by)
    
    return df.withColumn(new_col, F.lead(F.col(col), offset, default).over(window_spec))


def df_window_cumsum(df: DataFrame, col: str, new_col: str,
                     partition_by: Union[str, List[str]], 
                     order_by: Union[str, List[str]]) -> DataFrame:
    """Add cumulative sum column using window function.
    
    Args:
        df: Input DataFrame
        col: Column to sum
        new_col: Name for cumsum column
        partition_by: Column(s) to partition by
        order_by: Column(s) to order by
        
    Returns:
        DataFrame with cumsum column
    """
    partition_by = [partition_by] if isinstance(partition_by, str) else partition_by
    order_by = [order_by] if isinstance(order_by, str) else order_by
    
    window_spec = Window.partitionBy(*partition_by).orderBy(*order_by)\
                       .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    return df.withColumn(new_col, F.sum(F.col(col)).over(window_spec))


# ============================================================================
# SPECIAL OPERATIONS
# ============================================================================

def df_explode(df: DataFrame, col: str, new_col: Optional[str] = None) -> DataFrame:
    """Explode array column into multiple rows.
    
    Args:
        df: Input DataFrame
        col: Array column to explode
        new_col: Name for exploded column (if None, uses original name)
        
    Returns:
        DataFrame with exploded rows
    """
    new_col = new_col or col
    return df.withColumn(new_col, F.explode(F.col(col)))


def df_union(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """Union two DataFrames.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        
    Returns:
        Unioned DataFrame
    """
    return df1.union(df2)


def df_union_by_name(df1: DataFrame, df2: DataFrame, 
                     allow_missing: bool = False) -> DataFrame:
    """Union two DataFrames by column names.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        allow_missing: Allow missing columns
        
    Returns:
        Unioned DataFrame
    """
    return df1.unionByName(df2, allowMissingColumns=allow_missing)


def df_intersect(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """Get intersection of two DataFrames.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        
    Returns:
        Intersected DataFrame
    """
    return df1.intersect(df2)


def df_subtract(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """Subtract df2 from df1 (rows in df1 but not in df2).
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame to subtract
        
    Returns:
        Subtracted DataFrame
    """
    return df1.subtract(df2)


def df_crossjoin(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """Cross join two DataFrames.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        
    Returns:
        Cross joined DataFrame
    """
    return df1.crossJoin(df2)