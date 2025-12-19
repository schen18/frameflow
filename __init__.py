"""
FrameFlow - Unified DataFrame transformation framework.

This package provides a consistent API for data transformations across
Polars and PySpark, with JSON-based pipeline orchestration.

Example usage:
    # Polars
    from frameflow.polars import parq_read, cols_fillna, df_filter
    from frameflow.polars.pipeline import run_pipeline
    
    # PySpark
    from frameflow.pyspark import parq_read, cols_fillna, df_filter
    from frameflow.pyspark.pipeline import run_pipeline
"""

__version__ = "1.5.0"
__author__ = "Stephen Chen"
__license__ = "MIT"

# Try to import both frameworks, but don't fail if one is missing
_polars_available = False
_pyspark_available = False

try:
    import polars
    _polars_available = True
except ImportError:
    pass

try:
    import pyspark
    _pyspark_available = True
except ImportError:
    pass


def check_dependencies():
    """Check which frameworks are available."""
    status = {
        'polars': _polars_available,
        'pyspark': _pyspark_available
    }
    
    print("FrameFlow Framework Status:")
    print(f"  Polars:  {'✓ Available' if status['polars'] else '✗ Not installed'}")
    print(f"  PySpark: {'✓ Available' if status['pyspark'] else '✗ Not installed'}")
    
    if not any(status.values()):
        print("\nWarning: Neither Polars nor PySpark is installed!")
        print("Install with: pip install frameflow[polars] or pip install frameflow[pyspark]")
    
    return status


__all__ = [
    '__version__',
    '__author__',
    '__license__',
    'check_dependencies',
]
