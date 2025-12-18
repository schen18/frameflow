"""
FrameFlow PySpark module.

Standardized operations for PySpark DataFrames with JSON pipeline support.
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
    cols_add, cols_replace,
    cols_upper, cols_lower, cols_trim, cols_substring,
    cols_regexp_extract, cols_regexp_replace,
    
    # DataFrame operations
    df_filter, df_groupby, df_join, df_sort, df_unique,
    df_with_columns, df_pivot, df_unpivot, df_sql,
    df_sample, df_head, df_tail, df_show, df_collect, df_describe, df_count,
    df_union, df_union_by_name, df_intersect, df_subtract, df_crossjoin,
    df_explode,
    
    # Window operations
    df_window_rank, df_window_row_number,
    df_window_lag, df_window_lead, df_window_cumsum,
    
    # Utilities
    df_schema, df_print_schema, df_columns, df_dtypes,
    df_cache, df_persist, df_unpersist,
    df_repartition, df_coalesce,
)

from .pipeline import (
    # Expression wrappers
    col, lit, when, coalesce, greatest, least,
    array, struct, concat, concat_ws,
    upper, lower, length, trim,
    abs, round, floor, ceil, sqrt, exp, log,
    current_date, current_timestamp,
    to_date, to_timestamp, date_add, date_sub,
    year, month, day, hour, minute, second,
    
    # Pipeline
    ExpressionParser,
    PySparkPipeline,
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
    'cols_add', 'cols_replace',
    'cols_upper', 'cols_lower', 'cols_trim', 'cols_substring',
    'cols_regexp_extract', 'cols_regexp_replace',
    
    # DataFrame operations
    'df_filter', 'df_groupby', 'df_join', 'df_sort', 'df_unique',
    'df_with_columns', 'df_pivot', 'df_unpivot', 'df_sql',
    'df_sample', 'df_head', 'df_tail', 'df_show', 'df_collect', 'df_describe', 'df_count',
    'df_union', 'df_union_by_name', 'df_intersect', 'df_subtract', 'df_crossjoin',
    'df_explode',
    
    # Window operations
    'df_window_rank', 'df_window_row_number',
    'df_window_lag', 'df_window_lead', 'df_window_cumsum',
    
    # Utilities
    'df_schema', 'df_print_schema', 'df_columns', 'df_dtypes',
    'df_cache', 'df_persist', 'df_unpersist',
    'df_repartition', 'df_coalesce',
    
    # Expression wrappers
    'col', 'lit', 'when', 'coalesce', 'greatest', 'least',
    'array', 'struct', 'concat', 'concat_ws',
    'upper', 'lower', 'length', 'trim',
    'abs', 'round', 'floor', 'ceil', 'sqrt', 'exp', 'log',
    'current_date', 'current_timestamp',
    'to_date', 'to_timestamp', 'date_add', 'date_sub',
    'year', 'month', 'day', 'hour', 'minute', 'second',
    
    # Pipeline
    'ExpressionParser', 'PySparkPipeline', 'run_pipeline', 'create_pipeline_template',
]
