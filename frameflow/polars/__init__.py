"""
FrameFlow Polars module.

Standardized operations for Polars LazyFrames with JSON pipeline support.
"""

from .ops import (
    # File I/O
    parq_read, parq_write,
    csv_read, csv_write,
    json_read, json_write,
    json_load, json_dump,
    
    # Column operations
    cols_drop, cols_select, cols_fillna,
    cols_rename, cols_split, cols_split_to_array, cols_concat,
    cols_todate, cols_tobool, cols_toint, cols_tofloat, cols_tostr,
    cols_date_diff, cols_collect_to_array,
    
    # DataFrame operations
    df_filter, df_groupby, df_join, df_sort, df_unique,
    df_with_columns, df_pivot, df_unpivot, df_sql,
    df_sample, df_head, df_tail, df_collect, df_describe,
    
    # Utilities
    df_schema, df_columns, df_shape,
)

from .pipeline import (
    ExpressionParser,
    PolarsPipeline,
    run_pipeline,
    create_pipeline_template,
)

__all__ = [
    # File I/O
    'parq_read', 'parq_write',
    'csv_read', 'csv_write',
    'json_read', 'json_write',
    'json_load', 'json_dump',
    
    # Column operations
    'cols_drop', 'cols_select', 'cols_fillna',
    'cols_rename', 'cols_split', 'cols_split_to_array', 'cols_concat',
    'cols_todate', 'cols_tobool', 'cols_toint', 'cols_tofloat', 'cols_tostr',
    'cols_date_diff', 'cols_collect_to_array',
    
    # DataFrame operations
    'df_filter', 'df_groupby', 'df_join', 'df_sort', 'df_unique',
    'df_with_columns', 'df_pivot', 'df_unpivot', 'df_sql',
    'df_sample', 'df_head', 'df_tail', 'df_collect', 'df_describe',
    
    # Utilities
    'df_schema', 'df_columns', 'df_shape',
    
    # Pipeline
    'ExpressionParser', 'PolarsPipeline', 'run_pipeline', 'create_pipeline_template',
]
