"""
FrameFlow Polars LazyFrame abstraction module for standardized dataframe operations.

This module provides a consistent interface for common dataframe transformations
using Polars LazyFrames. All functions follow the pattern:
    function(df: LazyFrame, **parameters) -> LazyFrame

GCS Support:
    For GCS operations, you need to set up credentials:
    - Set GOOGLE_APPLICATION_CREDENTIALS environment variable, OR
    - Pass storage_options with credentials, OR
    - Use gcloud CLI authentication

Author: Claude
Version: 1.1.0
"""

import polars as pl
from polars import LazyFrame
from typing import Union, List, Dict, Any, Optional
from pathlib import Path
import os


# ============================================================================
# FILE I/O OPERATIONS
# ============================================================================

def _get_storage_options(path: str, storage_options: Optional[Dict] = None) -> Optional[Dict]:
    """Helper to get storage options for cloud paths.
    
    Args:
        path: File path (local or cloud)
        storage_options: Optional storage options dict
        
    Returns:
        Storage options dict or None for local paths
    """
    if path.startswith('gs://') or path.startswith('gcs://'):
        if storage_options is None:
            # Try to use GOOGLE_APPLICATION_CREDENTIALS from environment
            creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if creds_path:
                return {'google_application_credentials': creds_path}
            # If no explicit credentials, Polars will try default authentication
            return {}
        return storage_options
    return storage_options


def parq_read(path: Union[str, Path], storage_options: Optional[Dict] = None, **kwargs) -> LazyFrame:
    """Read Parquet file(s) as LazyFrame from local or GCS.
    
    Args:
        path: File path, glob pattern, or GCS URI (gs://bucket/path/file.parquet)
        storage_options: For GCS, dict with credentials or auth options:
            - 'google_application_credentials': path to JSON key file
            - 'token': OAuth2 token string
            Or None to use GOOGLE_APPLICATION_CREDENTIALS env var
        **kwargs: Additional arguments passed to pl.scan_parquet
        
    Returns:
        LazyFrame
        
    Examples:
        # Local file
        df = parq_read('data.parquet')
        
        # GCS with env credentials
        df = parq_read('gs://my-bucket/data.parquet')
        
        # GCS with explicit credentials
        df = parq_read('gs://my-bucket/data.parquet', 
                       storage_options={'google_application_credentials': 'key.json'})
    """
    path = str(path)
    storage_opts = _get_storage_options(path, storage_options)
    return pl.scan_parquet(path, storage_options=storage_opts, **kwargs)


def parq_write(df: LazyFrame, path: Union[str, Path], 
               storage_options: Optional[Dict] = None, **kwargs) -> None:
    """Write LazyFrame to Parquet file on local or GCS.
    
    Args:
        df: Input LazyFrame
        path: Output file path or GCS URI (gs://bucket/path/file.parquet)
        storage_options: For GCS, dict with credentials or auth options
        **kwargs: Additional arguments passed to write_parquet
        
    Examples:
        # Local file
        parq_write(df, 'output.parquet')
        
        # GCS
        parq_write(df, 'gs://my-bucket/output.parquet')
    """
    path = str(path)
    storage_opts = _get_storage_options(path, storage_options)
    df.collect().write_parquet(path, storage_options=storage_opts, **kwargs)


def csv_read(path: Union[str, Path], storage_options: Optional[Dict] = None, **kwargs) -> LazyFrame:
    """Read CSV file(s) as LazyFrame from local or GCS.
    
    Args:
        path: File path, glob pattern, or GCS URI (gs://bucket/path/file.csv)
        storage_options: For GCS, dict with credentials or auth options
        **kwargs: Additional arguments passed to pl.scan_csv
        
    Returns:
        LazyFrame
        
    Examples:
        # Local file
        df = csv_read('data.csv')
        
        # GCS
        df = csv_read('gs://my-bucket/data.csv')
    """
    path = str(path)
    storage_opts = _get_storage_options(path, storage_options)
    return pl.scan_csv(path, storage_options=storage_opts, **kwargs)


def csv_write(df: LazyFrame, path: Union[str, Path], 
              storage_options: Optional[Dict] = None, **kwargs) -> None:
    """Write LazyFrame to CSV file on local or GCS.
    
    Args:
        df: Input LazyFrame
        path: Output file path or GCS URI (gs://bucket/path/file.csv)
        storage_options: For GCS, dict with credentials or auth options
        **kwargs: Additional arguments passed to write_csv
        
    Examples:
        # Local file
        csv_write(df, 'output.csv')
        
        # GCS
        csv_write(df, 'gs://my-bucket/output.csv')
    """
    path = str(path)
    storage_opts = _get_storage_options(path, storage_options)
    df.collect().write_csv(path, storage_options=storage_opts, **kwargs)


def json_read(path: Union[str, Path], storage_options: Optional[Dict] = None, **kwargs) -> LazyFrame:
    """Read JSON file(s) as LazyFrame from local or GCS.
    
    Args:
        path: File path or GCS URI (gs://bucket/path/file.json)
        storage_options: For GCS, dict with credentials or auth options
        **kwargs: Additional arguments passed to pl.scan_ndjson
        
    Returns:
        LazyFrame
        
    Examples:
        # Local file
        df = json_read('data.json')
        
        # GCS
        df = json_read('gs://my-bucket/data.json')
    """
    path = str(path)
    storage_opts = _get_storage_options(path, storage_options)
    return pl.scan_ndjson(path, storage_options=storage_opts, **kwargs)


def json_write(df: LazyFrame, path: Union[str, Path], 
               storage_options: Optional[Dict] = None, **kwargs) -> None:
    """Write LazyFrame to JSON file on local or GCS.
    
    Args:
        df: Input LazyFrame
        path: Output file path or GCS URI (gs://bucket/path/file.json)
        storage_options: For GCS, dict with credentials or auth options
        **kwargs: Additional arguments passed to write_json
        
    Examples:
        # Local file
        json_write(df, 'output.json')
        
        # GCS
        json_write(df, 'gs://my-bucket/output.json')
    """
    path = str(path)
    storage_opts = _get_storage_options(path, storage_options)
    df.collect().write_json(path, storage_options=storage_opts, **kwargs)


def json_load(path: Union[str, Path], storage_options: Optional[Dict] = None) -> Union[Dict, List]:
    """Load JSON file to Python dict/list (like json.load) from local or GCS.
    
    Args:
        path: File path or GCS URI (gs://bucket/path/file.json)
        storage_options: For GCS, dict with credentials or auth options
        
    Returns:
        Python dict or list depending on JSON structure
        
    Examples:
        # Local file
        data = json_load('config.json')
        
        # GCS
        data = json_load('gs://my-bucket/config.json')
        
        # Access nested data
        value = data['key']['nested_key']
    """
    import json
    
    path = str(path)
    
    if path.startswith('gs://') or path.startswith('gcs://'):
        # For GCS, use fsspec to read the file
        try:
            import fsspec
        except ImportError:
            raise ImportError("fsspec is required for GCS operations. Install with: pip install fsspec gcsfs")
        
        storage_opts = _get_storage_options(path, storage_options)
        
        # Open and read file from GCS
        with fsspec.open(path, 'r', **storage_opts) as f:
            return json.load(f)
    else:
        # Local file
        with open(path, 'r') as f:
            return json.load(f)


def json_dump(obj: Union[Dict, List], path: Union[str, Path], 
              storage_options: Optional[Dict] = None, **kwargs) -> None:
    """Write Python dict/list to JSON file (like json.dump) on local or GCS.
    
    Args:
        obj: Python dict or list to write
        path: Output file path or GCS URI (gs://bucket/path/file.json)
        storage_options: For GCS, dict with credentials or auth options
        **kwargs: Additional arguments passed to json.dump (indent, sort_keys, etc.)
        
    Examples:
        # Local file
        data = {'key': 'value', 'nested': {'a': 1}}
        json_dump(data, 'output.json', indent=2)
        
        # GCS
        json_dump(data, 'gs://my-bucket/output.json', indent=2)
    """
    import json
    
    path = str(path)
    
    if path.startswith('gs://') or path.startswith('gcs://'):
        # For GCS, use fsspec to write the file
        try:
            import fsspec
        except ImportError:
            raise ImportError("fsspec is required for GCS operations. Install with: pip install fsspec gcsfs")
        
        storage_opts = _get_storage_options(path, storage_options)
        
        # Open and write file to GCS
        with fsspec.open(path, 'w', **storage_opts) as f:
            json.dump(obj, f, **kwargs)
    else:
        # Local file
        with open(path, 'w') as f:
            json.dump(obj, f, **kwargs)


# ============================================================================
# COLUMN OPERATIONS
# ============================================================================

def cols_drop(df: LazyFrame, cols: Union[str, List[str]]) -> LazyFrame:
    """Drop one or more columns.
    
    Args:
        df: Input LazyFrame
        cols: Column name(s) to drop
        
    Returns:
        LazyFrame with columns dropped
    """
    cols = [cols] if isinstance(cols, str) else cols
    return df.drop(cols)


def cols_select(df: LazyFrame, cols: Union[str, List[str]]) -> LazyFrame:
    """Select one or more columns.
    
    Args:
        df: Input LazyFrame
        cols: Column name(s) to select
        
    Returns:
        LazyFrame with selected columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    return df.select(cols)


def cols_fillna(df: LazyFrame, cols: Union[str, List[str]], value: Any = None, 
                strategy: Optional[str] = None) -> LazyFrame:
    """Fill null values in columns.
    
    Args:
        df: Input LazyFrame
        cols: Column name(s) to fill
        value: Value to fill nulls with (if strategy is None)
        strategy: Fill strategy - 'forward', 'backward', 'mean', 'min', 'max', 'zero', 'one'
        
    Returns:
        LazyFrame with nulls filled
    """
    cols = [cols] if isinstance(cols, str) else cols
    
    if strategy == 'forward':
        return df.with_columns([pl.col(c).forward_fill() for c in cols])
    elif strategy == 'backward':
        return df.with_columns([pl.col(c).backward_fill() for c in cols])
    elif strategy == 'mean':
        return df.with_columns([pl.col(c).fill_null(pl.col(c).mean()) for c in cols])
    elif strategy == 'min':
        return df.with_columns([pl.col(c).fill_null(pl.col(c).min()) for c in cols])
    elif strategy == 'max':
        return df.with_columns([pl.col(c).fill_null(pl.col(c).max()) for c in cols])
    elif strategy == 'zero':
        return df.with_columns([pl.col(c).fill_null(0) for c in cols])
    elif strategy == 'one':
        return df.with_columns([pl.col(c).fill_null(1) for c in cols])
    else:
        return df.with_columns([pl.col(c).fill_null(value) for c in cols])


def cols_rename(df: LazyFrame, mapping: Dict[str, str]) -> LazyFrame:
    """Rename columns using a mapping dictionary.
    
    Args:
        df: Input LazyFrame
        mapping: Dictionary mapping old names to new names
        
    Returns:
        LazyFrame with renamed columns
    """
    return df.rename(mapping)


def cols_split(df: LazyFrame, col: str, separator: str, 
               new_cols: Optional[List[str]] = None) -> LazyFrame:
    """Split a string column into multiple columns.
    
    Args:
        df: Input LazyFrame
        col: Column to split
        separator: String separator
        new_cols: Names for new columns (if None, uses col_1, col_2, etc.)
        
    Returns:
        LazyFrame with split columns
    """
    split_expr = pl.col(col).str.split(separator)
    
    if new_cols is None:
        # Auto-generate column names
        return df.with_columns([
            split_expr.list.get(i).alias(f"{col}_{i+1}") 
            for i in range(10)  # Arbitrary limit
        ])
    else:
        return df.with_columns([
            split_expr.list.get(i).alias(new_cols[i]) 
            for i in range(len(new_cols))
        ])


def cols_split_to_array(df: LazyFrame, col: str, separator: str, 
                        new_col: Optional[str] = None) -> LazyFrame:
    """Split a string column into an array/list column.
    
    Args:
        df: Input LazyFrame
        col: Column to split
        separator: String separator
        new_col: Name for new array column (if None, uses original col name)
        
    Returns:
        LazyFrame with array column
        
    Example:
        df with col "tags" = "python,data,ml"
        -> df with col "tags" = ["python", "data", "ml"]
    """
    new_col = new_col or col
    return df.with_columns(
        pl.col(col).str.split(separator).alias(new_col)
    )


def cols_concat(df: LazyFrame, cols: List[str], new_col: str, 
                separator: str = "") -> LazyFrame:
    """Concatenate multiple string columns into one.
    
    Args:
        df: Input LazyFrame
        cols: Columns to concatenate
        new_col: Name of new concatenated column
        separator: String separator between values
        
    Returns:
        LazyFrame with concatenated column
    """
    if separator:
        concat_expr = pl.concat_str([pl.col(c) for c in cols], separator=separator)
    else:
        concat_expr = pl.concat_str([pl.col(c) for c in cols])
    
    return df.with_columns(concat_expr.alias(new_col))


def cols_todate(df: LazyFrame, cols: Union[str, List[str]], 
                format: Optional[str] = None) -> LazyFrame:
    """Convert columns to date/datetime type.
    
    Args:
        df: Input LazyFrame
        cols: Column name(s) to convert
        format: Date format string (if None, auto-infers)
        
    Returns:
        LazyFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    
    if format:
        return df.with_columns([
            pl.col(c).str.to_datetime(format) for c in cols
        ])
    else:
        return df.with_columns([
            pl.col(c).str.to_datetime() for c in cols
        ])


def cols_tobool(df: LazyFrame, cols: Union[str, List[str]]) -> LazyFrame:
    """Convert columns to boolean type.
    
    Args:
        df: Input LazyFrame
        cols: Column name(s) to convert
        
    Returns:
        LazyFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    return df.with_columns([pl.col(c).cast(pl.Boolean) for c in cols])


def cols_toint(df: LazyFrame, cols: Union[str, List[str]], 
               strict: bool = True) -> LazyFrame:
    """Convert columns to integer type.
    
    Args:
        df: Input LazyFrame
        cols: Column name(s) to convert
        strict: If False, allows nulls for non-convertible values
        
    Returns:
        LazyFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    return df.with_columns([pl.col(c).cast(pl.Int64, strict=strict) for c in cols])


def cols_tofloat(df: LazyFrame, cols: Union[str, List[str]], 
                 strict: bool = True) -> LazyFrame:
    """Convert columns to float type.
    
    Args:
        df: Input LazyFrame
        cols: Column name(s) to convert
        strict: If False, allows nulls for non-convertible values
        
    Returns:
        LazyFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    return df.with_columns([pl.col(c).cast(pl.Float64, strict=strict) for c in cols])


def cols_tostr(df: LazyFrame, cols: Union[str, List[str]]) -> LazyFrame:
    """Convert columns to string type.
    
    Args:
        df: Input LazyFrame
        cols: Column name(s) to convert
        
    Returns:
        LazyFrame with converted columns
    """
    cols = [cols] if isinstance(cols, str) else cols
    return df.with_columns([pl.col(c).cast(pl.Utf8) for c in cols])


def cols_date_diff(df: LazyFrame, col1: str, col2: str, new_col: str, 
                   unit: str = 'days') -> LazyFrame:
    """Calculate difference between two date/datetime columns.
    
    Args:
        df: Input LazyFrame
        col1: First date column (subtracted from)
        col2: Second date column (subtracted)
        new_col: Name for new column with difference
        unit: Unit of difference - 'days', 'hours', 'minutes', 'seconds', 
              'milliseconds', 'microseconds', 'nanoseconds'
        
    Returns:
        LazyFrame with new difference column
        
    Example:
        Calculate days between dates: cols_date_diff(df, 'end_date', 'start_date', 'duration', 'days')
    """
    diff_expr = pl.col(col1) - pl.col(col2)
    
    if unit == 'days':
        result_expr = diff_expr.dt.total_days()
    elif unit == 'hours':
        result_expr = diff_expr.dt.total_hours()
    elif unit == 'minutes':
        result_expr = diff_expr.dt.total_minutes()
    elif unit == 'seconds':
        result_expr = diff_expr.dt.total_seconds()
    elif unit == 'milliseconds':
        result_expr = diff_expr.dt.total_milliseconds()
    elif unit == 'microseconds':
        result_expr = diff_expr.dt.total_microseconds()
    elif unit == 'nanoseconds':
        result_expr = diff_expr.dt.total_nanoseconds()
    else:
        raise ValueError(f"Invalid unit: {unit}")
    
    return df.with_columns(result_expr.alias(new_col))


def cols_collect_to_array(df: LazyFrame, group_by: Union[str, List[str]], 
                          collect_cols: Union[str, List[str], Dict[str, str]],
                          sort_by: Optional[Union[str, List[str]]] = None,
                          sort_descending: bool = False) -> LazyFrame:
    """Collect column values into array columns grouped by key column(s).
    
    Args:
        df: Input LazyFrame
        group_by: Column(s) to group by (e.g., 'customer_id')
        collect_cols: Column(s) to collect into arrays. Can be:
            - Single column name: collected as-is
            - List of columns: each collected separately
            - Dict mapping old names to new array column names
        sort_by: Column(s) to sort by before collecting (maintains order in arrays)
        sort_descending: Sort order for sort_by
        
    Returns:
        LazyFrame with one row per group and array columns
        
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
        df = df.sort(sort_by, descending=sort_descending)
    
    # Build aggregation expressions
    agg_exprs = []
    
    if isinstance(collect_cols, dict):
        # Dict mapping: old_name -> new_name
        for old_col, new_col in collect_cols.items():
            agg_exprs.append(pl.col(old_col).alias(new_col))
    else:
        # Single column or list of columns
        collect_cols = [collect_cols] if isinstance(collect_cols, str) else collect_cols
        for col in collect_cols:
            agg_exprs.append(pl.col(col))
    
    return df.group_by(group_by).agg(agg_exprs)


# ============================================================================
# DATAFRAME OPERATIONS
# ============================================================================

def df_filter(df: LazyFrame, condition: Union[str, pl.Expr]) -> LazyFrame:
    """Filter rows based on condition.
    
    Args:
        df: Input LazyFrame
        condition: Filter condition (Polars expression or SQL-like string)
        
    Returns:
        Filtered LazyFrame
    """
    if isinstance(condition, str):
        # Parse simple string conditions
        return df.filter(pl.Expr.deserialize(condition, format='json'))
    return df.filter(condition)


def df_groupby(df: LazyFrame, by: Union[str, List[str]], 
               agg: Dict[str, Union[str, List[str]]]) -> LazyFrame:
    """Group by columns and aggregate.
    
    Args:
        df: Input LazyFrame
        by: Column(s) to group by
        agg: Dictionary mapping column names to aggregation functions
             e.g., {'col1': 'sum', 'col2': ['mean', 'max']}
        
    Returns:
        Aggregated LazyFrame
    """
    by = [by] if isinstance(by, str) else by
    
    agg_exprs = []
    for col, funcs in agg.items():
        funcs = [funcs] if isinstance(funcs, str) else funcs
        for func in funcs:
            if func == 'sum':
                agg_exprs.append(pl.col(col).sum().alias(f"{col}_{func}"))
            elif func == 'mean':
                agg_exprs.append(pl.col(col).mean().alias(f"{col}_{func}"))
            elif func == 'min':
                agg_exprs.append(pl.col(col).min().alias(f"{col}_{func}"))
            elif func == 'max':
                agg_exprs.append(pl.col(col).max().alias(f"{col}_{func}"))
            elif func == 'count':
                agg_exprs.append(pl.col(col).count().alias(f"{col}_{func}"))
            elif func == 'std':
                agg_exprs.append(pl.col(col).std().alias(f"{col}_{func}"))
            elif func == 'var':
                agg_exprs.append(pl.col(col).var().alias(f"{col}_{func}"))
            elif func == 'first':
                agg_exprs.append(pl.col(col).first().alias(f"{col}_{func}"))
            elif func == 'last':
                agg_exprs.append(pl.col(col).last().alias(f"{col}_{func}"))
            elif func == 'median':
                agg_exprs.append(pl.col(col).median().alias(f"{col}_{func}"))
            elif func == 'n_unique':
                agg_exprs.append(pl.col(col).n_unique().alias(f"{col}_{func}"))
    
    return df.group_by(by).agg(agg_exprs)


def df_join(df: LazyFrame, other: LazyFrame, on: Union[str, List[str]], 
            how: str = 'inner', suffix: str = '_right') -> LazyFrame:
    """Join two LazyFrames.
    
    Args:
        df: Left LazyFrame
        other: Right LazyFrame
        on: Column(s) to join on
        how: Join type - 'inner', 'left', 'outer', 'cross', 'semi', 'anti'
        suffix: Suffix for overlapping column names from right frame
        
    Returns:
        Joined LazyFrame
    """
    on = [on] if isinstance(on, str) else on
    return df.join(other, on=on, how=how, suffix=suffix)


def df_sort(df: LazyFrame, by: Union[str, List[str]], 
            descending: Union[bool, List[bool]] = False) -> LazyFrame:
    """Sort LazyFrame by column(s).
    
    Args:
        df: Input LazyFrame
        by: Column(s) to sort by
        descending: Sort order (can be per-column if list)
        
    Returns:
        Sorted LazyFrame
    """
    by = [by] if isinstance(by, str) else by
    return df.sort(by, descending=descending)


def df_unique(df: LazyFrame, subset: Optional[Union[str, List[str]]] = None, 
              keep: str = 'first') -> LazyFrame:
    """Remove duplicate rows.
    
    Args:
        df: Input LazyFrame
        subset: Column(s) to consider for duplicates (None = all columns)
        keep: Which duplicate to keep - 'first', 'last', 'none'
        
    Returns:
        LazyFrame with duplicates removed
    """
    if subset is not None:
        subset = [subset] if isinstance(subset, str) else subset
    return df.unique(subset=subset, keep=keep)


def df_with_columns(df: LazyFrame, exprs: Dict[str, pl.Expr]) -> LazyFrame:
    """Add or modify columns using expressions.
    
    Args:
        df: Input LazyFrame
        exprs: Dictionary mapping new column names to Polars expressions
        
    Returns:
        LazyFrame with new/modified columns
    """
    col_exprs = [expr.alias(name) for name, expr in exprs.items()]
    return df.with_columns(col_exprs)


def df_pivot(df: LazyFrame, index: Union[str, List[str]], 
             columns: str, values: Union[str, List[str]], 
             aggregate_function: str = 'first') -> LazyFrame:
    """Pivot LazyFrame from long to wide format.
    
    Args:
        df: Input LazyFrame
        index: Column(s) to use as index
        columns: Column to pivot into new columns
        values: Column(s) containing values
        aggregate_function: How to aggregate duplicates
        
    Returns:
        Pivoted LazyFrame
    """
    index = [index] if isinstance(index, str) else index
    values = [values] if isinstance(values, str) else values
    
    return df.pivot(
        index=index,
        columns=columns,
        values=values,
        aggregate_function=aggregate_function
    )


def df_unpivot(df: LazyFrame, id_vars: Union[str, List[str]], 
               value_vars: Optional[Union[str, List[str]]] = None,
               variable_name: str = 'variable', 
               value_name: str = 'value') -> LazyFrame:
    """Unpivot LazyFrame from wide to long format.
    
    Args:
        df: Input LazyFrame
        id_vars: Column(s) to keep as identifiers
        value_vars: Column(s) to unpivot (None = all except id_vars)
        variable_name: Name for the variable column
        value_name: Name for the value column
        
    Returns:
        Unpivoted LazyFrame
    """
    id_vars = [id_vars] if isinstance(id_vars, str) else id_vars
    if value_vars is not None:
        value_vars = [value_vars] if isinstance(value_vars, str) else value_vars
    
    return df.unpivot(
        index=id_vars,
        on=value_vars,
        variable_name=variable_name,
        value_name=value_name
    )


def df_sql(query: str, **frames: LazyFrame) -> LazyFrame:
    """Execute SQL query on LazyFrames.
    
    Args:
        query: SQL query string
        **frames: Named LazyFrames to use in query (name=lazyframe)
        
    Returns:
        Result LazyFrame
        
    Example:
        result = df_sql(
            "SELECT * FROM df1 JOIN df2 ON df1.id = df2.id",
            df1=lazyframe1,
            df2=lazyframe2
        )
    """
    # Register frames in SQL context
    ctx = pl.SQLContext()
    for name, frame in frames.items():
        ctx.register(name, frame)
    
    return ctx.execute(query)


def df_sample(df: LazyFrame, n: Optional[int] = None, 
              fraction: Optional[float] = None, 
              with_replacement: bool = False, 
              seed: Optional[int] = None) -> LazyFrame:
    """Sample rows from LazyFrame.
    
    Args:
        df: Input LazyFrame
        n: Number of rows to sample
        fraction: Fraction of rows to sample (0.0 to 1.0)
        with_replacement: Whether to sample with replacement
        seed: Random seed for reproducibility
        
    Returns:
        Sampled LazyFrame
    """
    if n is not None:
        return df.sample(n=n, with_replacement=with_replacement, seed=seed)
    elif fraction is not None:
        return df.sample(fraction=fraction, with_replacement=with_replacement, seed=seed)
    else:
        raise ValueError("Either 'n' or 'fraction' must be specified")


def df_head(df: LazyFrame, n: int = 5) -> pl.DataFrame:
    """Get first n rows (collects LazyFrame).
    
    Args:
        df: Input LazyFrame
        n: Number of rows to return
        
    Returns:
        DataFrame with first n rows
    """
    return df.head(n).collect()


def df_tail(df: LazyFrame, n: int = 5) -> pl.DataFrame:
    """Get last n rows (collects LazyFrame).
    
    Args:
        df: Input LazyFrame
        n: Number of rows to return
        
    Returns:
        DataFrame with last n rows
    """
    return df.tail(n).collect()


def df_collect(df: LazyFrame) -> pl.DataFrame:
    """Execute LazyFrame and return DataFrame.
    
    Args:
        df: Input LazyFrame
        
    Returns:
        Collected DataFrame
    """
    return df.collect()


def df_describe(df: LazyFrame) -> pl.DataFrame:
    """Get summary statistics (collects LazyFrame).
    
    Args:
        df: Input LazyFrame
        
    Returns:
        DataFrame with summary statistics
    """
    return df.collect().describe()


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def df_schema(df: LazyFrame) -> Dict[str, Any]:
    """Get schema of LazyFrame.
    
    Args:
        df: Input LazyFrame
        
    Returns:
        Dictionary mapping column names to data types
    """
    return df.schema


def df_columns(df: LazyFrame) -> List[str]:
    """Get column names of LazyFrame.
    
    Args:
        df: Input LazyFrame
        
    Returns:
        List of column names
    """
    return df.columns


def df_shape(df: LazyFrame) -> tuple:
    """Get shape of LazyFrame (collects to count rows).
    
    Args:
        df: Input LazyFrame
        
    Returns:
        Tuple of (n_rows, n_columns)
    """
    collected = df.collect()
    return collected.shape