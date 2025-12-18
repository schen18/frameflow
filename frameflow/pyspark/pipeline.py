"""
FrameFlow PySpark Pipeline Orchestrator - JSON-driven data transformation wrapper.

This module enables declarative data transformations using JSON specifications
with a consistent API to the Polars pipeline module. Provides expression parsing
to use col('name') instead of F.col('name') in JSON specs.

Author: Claude
Version: 1.0.0
"""

import json
import re
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import *

# Import all functions from the pyspark_ops module
# Assuming the module is named pyspark_ops and is importable
try:
    from pyspark_ops import *
except ImportError:
    print("Warning: pyspark_ops module not found. Make sure it's in your Python path.")


# ============================================================================
# EXPRESSION WRAPPER FUNCTIONS
# ============================================================================

def col(name: str):
    """Wrapper for F.col() - reference a column.
    
    Args:
        name: Column name
        
    Returns:
        PySpark Column expression
    """
    return F.col(name)


def lit(value: Any):
    """Wrapper for F.lit() - create a literal value.
    
    Args:
        value: Literal value
        
    Returns:
        PySpark Column expression
    """
    return F.lit(value)


def when(condition, value):
    """Wrapper for F.when() - conditional expression.
    
    Args:
        condition: Condition expression
        value: Value if condition is true
        
    Returns:
        PySpark Column expression
    """
    return F.when(condition, value)


def coalesce(*cols):
    """Wrapper for F.coalesce() - return first non-null value.
    
    Args:
        *cols: Column expressions
        
    Returns:
        PySpark Column expression
    """
    return F.coalesce(*cols)


def greatest(*cols):
    """Wrapper for F.greatest() - return greatest value.
    
    Args:
        *cols: Column expressions
        
    Returns:
        PySpark Column expression
    """
    return F.greatest(*cols)


def least(*cols):
    """Wrapper for F.least() - return least value.
    
    Args:
        *cols: Column expressions
        
    Returns:
        PySpark Column expression
    """
    return F.least(*cols)


def array(*cols):
    """Wrapper for F.array() - create array from columns.
    
    Args:
        *cols: Column expressions
        
    Returns:
        PySpark Column expression
    """
    return F.array(*cols)


def struct(*cols):
    """Wrapper for F.struct() - create struct from columns.
    
    Args:
        *cols: Column expressions
        
    Returns:
        PySpark Column expression
    """
    return F.struct(*cols)


def concat(*cols):
    """Wrapper for F.concat() - concatenate columns.
    
    Args:
        *cols: Column expressions
        
    Returns:
        PySpark Column expression
    """
    return F.concat(*cols)


def concat_ws(sep: str, *cols):
    """Wrapper for F.concat_ws() - concatenate with separator.
    
    Args:
        sep: Separator string
        *cols: Column expressions
        
    Returns:
        PySpark Column expression
    """
    return F.concat_ws(sep, *cols)


def upper(col_expr):
    """Wrapper for F.upper() - convert to uppercase.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.upper(col_expr)


def lower(col_expr):
    """Wrapper for F.lower() - convert to lowercase.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.lower(col_expr)


def length(col_expr):
    """Wrapper for F.length() - get string length.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.length(col_expr)


def trim(col_expr):
    """Wrapper for F.trim() - trim whitespace.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.trim(col_expr)


def abs(col_expr):
    """Wrapper for F.abs() - absolute value.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.abs(col_expr)


def round(col_expr, scale: int = 0):
    """Wrapper for F.round() - round to scale decimal places.
    
    Args:
        col_expr: Column expression
        scale: Number of decimal places
        
    Returns:
        PySpark Column expression
    """
    return F.round(col_expr, scale)


def floor(col_expr):
    """Wrapper for F.floor() - round down.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.floor(col_expr)


def ceil(col_expr):
    """Wrapper for F.ceil() - round up.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.ceil(col_expr)


def sqrt(col_expr):
    """Wrapper for F.sqrt() - square root.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.sqrt(col_expr)


def exp(col_expr):
    """Wrapper for F.exp() - exponential.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.exp(col_expr)


def log(col_expr):
    """Wrapper for F.log() - natural logarithm.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.log(col_expr)


def current_date():
    """Wrapper for F.current_date() - get current date.
    
    Returns:
        PySpark Column expression
    """
    return F.current_date()


def current_timestamp():
    """Wrapper for F.current_timestamp() - get current timestamp.
    
    Returns:
        PySpark Column expression
    """
    return F.current_timestamp()


def to_date(col_expr, format: Optional[str] = None):
    """Wrapper for F.to_date() - convert to date.
    
    Args:
        col_expr: Column expression
        format: Date format string
        
    Returns:
        PySpark Column expression
    """
    if format:
        return F.to_date(col_expr, format)
    return F.to_date(col_expr)


def to_timestamp(col_expr, format: Optional[str] = None):
    """Wrapper for F.to_timestamp() - convert to timestamp.
    
    Args:
        col_expr: Column expression
        format: Timestamp format string
        
    Returns:
        PySpark Column expression
    """
    if format:
        return F.to_timestamp(col_expr, format)
    return F.to_timestamp(col_expr)


def date_add(col_expr, days: int):
    """Wrapper for F.date_add() - add days to date.
    
    Args:
        col_expr: Column expression
        days: Number of days to add
        
    Returns:
        PySpark Column expression
    """
    return F.date_add(col_expr, days)


def date_sub(col_expr, days: int):
    """Wrapper for F.date_sub() - subtract days from date.
    
    Args:
        col_expr: Column expression
        days: Number of days to subtract
        
    Returns:
        PySpark Column expression
    """
    return F.date_sub(col_expr, days)


def year(col_expr):
    """Wrapper for F.year() - extract year.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.year(col_expr)


def month(col_expr):
    """Wrapper for F.month() - extract month.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.month(col_expr)


def day(col_expr):
    """Wrapper for F.day() - extract day.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.day(col_expr)


def hour(col_expr):
    """Wrapper for F.hour() - extract hour.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.hour(col_expr)


def minute(col_expr):
    """Wrapper for F.minute() - extract minute.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.minute(col_expr)


def second(col_expr):
    """Wrapper for F.second() - extract second.
    
    Args:
        col_expr: Column expression
        
    Returns:
        PySpark Column expression
    """
    return F.second(col_expr)


# ============================================================================
# EXPRESSION PARSER
# ============================================================================

class ExpressionParser:
    """Parse string expressions into PySpark expressions."""
    
    @staticmethod
    def parse(expr_str: str):
        """Parse a string expression into a PySpark expression.
        
        Supports:
        - col('name') -> F.col('name')
        - lit(value) -> F.lit(value)
        - Chained operations: col('x').isNull()
        - Binary operations: col('x') + col('y')
        - Comparisons: col('x') > 10
        - Logical operations: (col('x') > 10) & (col('y') < 20)
        - String methods: col('x').contains('pattern')
        
        Args:
            expr_str: String expression to parse
            
        Returns:
            PySpark Column expression
        """
        # Remove extra whitespace
        expr_str = expr_str.strip()
        
        # Create safe namespace with wrapper functions and F module
        namespace = {
            'F': F,
            'col': col,
            'lit': lit,
            'when': when,
            'coalesce': coalesce,
            'greatest': greatest,
            'least': least,
            'array': array,
            'struct': struct,
            'concat': concat,
            'concat_ws': concat_ws,
            'upper': upper,
            'lower': lower,
            'length': length,
            'trim': trim,
            'abs': abs,
            'round': round,
            'floor': floor,
            'ceil': ceil,
            'sqrt': sqrt,
            'exp': exp,
            'log': log,
            'current_date': current_date,
            'current_timestamp': current_timestamp,
            'to_date': to_date,
            'to_timestamp': to_timestamp,
            'date_add': date_add,
            'date_sub': date_sub,
            'year': year,
            'month': month,
            'day': day,
            'hour': hour,
            'minute': minute,
            'second': second,
            # Common Python values
            'True': True,
            'False': False,
            'None': None,
        }
        
        try:
            return eval(expr_str, {"__builtins__": {}}, namespace)
        except Exception as e:
            raise ValueError(f"Failed to parse expression '{expr_str}': {str(e)}")
    
    @staticmethod
    def parse_condition(condition: Union[str, Any]):
        """Parse a condition (handles both string and Column expressions).
        
        Args:
            condition: String condition or PySpark Column expression
            
        Returns:
            PySpark Column expression
        """
        if isinstance(condition, str):
            return ExpressionParser.parse(condition)
        return condition
    
    @staticmethod
    def parse_expressions(exprs: Union[str, List[str], Dict[str, str]]) -> Union[Any, List[Any], Dict[str, Any]]:
        """Parse one or multiple expressions.
        
        Args:
            exprs: Single expression, list of expressions, or dict of expressions
            
        Returns:
            Parsed PySpark expression(s)
        """
        if isinstance(exprs, str):
            return ExpressionParser.parse(exprs)
        elif isinstance(exprs, list):
            return [ExpressionParser.parse(e) if isinstance(e, str) else e for e in exprs]
        elif isinstance(exprs, dict):
            return {k: ExpressionParser.parse(v) if isinstance(v, str) else v for k, v in exprs.items()}
        return exprs


# ============================================================================
# PIPELINE CLASS
# ============================================================================

class PySparkPipeline:
    """Execute data transformations based on JSON specifications."""
    
    def __init__(self, spec: Union[Dict, str, Path]):
        """Initialize pipeline with specification.
        
        Args:
            spec: Pipeline specification as dict, JSON string, or path to JSON file
        """
        if isinstance(spec, (str, Path)):
            spec_path = str(spec)
            if spec_path.endswith('.json'):
                # Load from file (supports GCS)
                self.spec = json_load(spec_path)
            else:
                # Parse as JSON string
                self.spec = json.loads(spec_path)
        else:
            self.spec = spec
        
        self.df = None
        self._validate_spec()
    
    def _validate_spec(self):
        """Validate the pipeline specification."""
        if 'input' not in self.spec:
            raise ValueError("Specification must contain 'input' field")
        
        if 'steps' not in self.spec:
            raise ValueError("Specification must contain 'steps' field")
        
        if not isinstance(self.spec['steps'], list):
            raise ValueError("'steps' must be a list")
    
    def _load_input(self) -> DataFrame:
        """Load input data based on specification."""
        input_config = self.spec['input']
        
        if isinstance(input_config, str):
            # Simple path string - infer format from extension
            path = input_config
            if path.endswith('.parquet'):
                return parq_read(path)
            elif path.endswith('.csv'):
                return csv_read(path)
            elif path.endswith('.json'):
                return json_read(path)
            else:
                raise ValueError(f"Cannot infer format from path: {path}")
        
        elif isinstance(input_config, dict):
            # Detailed configuration
            path = input_config.get('path')
            format_type = input_config.get('format', self._infer_format(path))
            read_options = input_config.get('options', {})
            
            if format_type == 'parquet':
                return parq_read(path, **read_options)
            elif format_type == 'csv':
                return csv_read(path, **read_options)
            elif format_type == 'json':
                return json_read(path, **read_options)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
        
        else:
            raise ValueError("'input' must be a string path or dict configuration")
    
    def _infer_format(self, path: str) -> str:
        """Infer file format from path extension."""
        if path.endswith('.parquet'):
            return 'parquet'
        elif path.endswith('.csv'):
            return 'csv'
        elif path.endswith('.json'):
            return 'json'
        else:
            raise ValueError(f"Cannot infer format from path: {path}")
    
    def _execute_step(self, df: DataFrame, step: Dict) -> DataFrame:
        """Execute a single transformation step."""
        operation = step.get('operation')
        params = step.get('params', {})
        
        if not operation:
            raise ValueError("Each step must have an 'operation' field")
        
        # Parse string expressions in params
        parsed_params = self._parse_params(params)
        
        # Get the function from globals (imported from pyspark_ops)
        func = globals().get(operation)
        
        if func is None:
            raise ValueError(f"Unknown operation: {operation}")
        
        # Execute the operation
        try:
            return func(df, **parsed_params)
        except Exception as e:
            raise RuntimeError(f"Error executing operation '{operation}': {str(e)}")
    
    def _parse_params(self, params: Dict) -> Dict:
        """Parse parameters, converting string expressions to PySpark expressions.
        
        Args:
            params: Dictionary of parameters
            
        Returns:
            Dictionary with parsed expressions
        """
        parsed = {}
        
        for key, value in params.items():
            # Check if this parameter should be parsed as an expression
            if self._should_parse_expression(key, value):
                parsed[key] = ExpressionParser.parse_condition(value)
            elif isinstance(value, dict):
                # Recursively parse nested dicts (like in df_with_columns)
                parsed[key] = {k: ExpressionParser.parse_condition(v) if isinstance(v, str) and self._looks_like_expression(v) else v 
                              for k, v in value.items()}
            elif isinstance(value, list):
                # Parse list items if they look like expressions
                parsed[key] = [ExpressionParser.parse_condition(v) if isinstance(v, str) and self._looks_like_expression(v) else v 
                              for v in value]
            else:
                parsed[key] = value
        
        return parsed
    
    def _should_parse_expression(self, param_name: str, value: Any) -> bool:
        """Determine if a parameter should be parsed as an expression.
        
        Args:
            param_name: Name of the parameter
            value: Value of the parameter
            
        Returns:
            True if should be parsed as expression
        """
        # Parameters that should be parsed as expressions
        expression_params = {'condition', 'expr', 'predicate', 'filter'}
        
        if param_name in expression_params and isinstance(value, str):
            return True
        
        # Check if value looks like an expression
        if isinstance(value, str) and self._looks_like_expression(value):
            return True
        
        return False
    
    def _looks_like_expression(self, value: str) -> bool:
        """Check if a string looks like a PySpark expression.
        
        Args:
            value: String to check
            
        Returns:
            True if looks like an expression
        """
        # Check for common expression patterns
        expression_patterns = [
            r'\bcol\(',
            r'\blit\(',
            r'\bF\.',
            r'\bwhen\(',
            r'\bupper\(',
            r'\blower\(',
            r'\bconcat\(',
            r'\byear\(',
            r'\bmonth\(',
            r'\bday\(',
        ]
        
        return any(re.search(pattern, value) for pattern in expression_patterns)
    
    def _write_output(self, df: DataFrame):
        """Write output data based on specification."""
        if 'output' not in self.spec:
            return  # No output specified
        
        output_config = self.spec['output']
        
        if isinstance(output_config, str):
            # Simple path string - infer format from extension
            path = output_config
            if path.endswith('.parquet'):
                parq_write(df, path)
            elif path.endswith('.csv'):
                csv_write(df, path)
            elif path.endswith('.json'):
                json_write(df, path)
            else:
                raise ValueError(f"Cannot infer format from path: {path}")
        
        elif isinstance(output_config, dict):
            # Detailed configuration
            path = output_config.get('path')
            format_type = output_config.get('format', self._infer_format(path))
            write_options = output_config.get('options', {})
            
            if format_type == 'parquet':
                parq_write(df, path, **write_options)
            elif format_type == 'csv':
                csv_write(df, path, **write_options)
            elif format_type == 'json':
                json_write(df, path, **write_options)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
        
        else:
            raise ValueError("'output' must be a string path or dict configuration")
    
    def execute(self, return_df: bool = False) -> Optional[DataFrame]:
        """Execute the entire pipeline.
        
        Args:
            return_df: If True, return the final DataFrame
            
        Returns:
            DataFrame if return_df is True, otherwise None
        """
        # Load input data
        print(f"Loading input from: {self.spec['input']}")
        self.df = self._load_input()
        
        # Execute transformation steps
        print(f"Executing {len(self.spec['steps'])} transformation steps...")
        for i, step in enumerate(self.spec['steps'], 1):
            operation = step.get('operation')
            print(f"  Step {i}: {operation}")
            self.df = self._execute_step(self.df, step)
        
        # Write output if specified
        if 'output' in self.spec:
            print(f"Writing output to: {self.spec['output']}")
            self._write_output(self.df)
        
        print("Pipeline execution complete!")
        
        if return_df:
            return self.df
        
        return None
    
    def dry_run(self) -> Dict[str, Any]:
        """Perform a dry run to validate the pipeline without executing.
        
        Returns:
            Dictionary with pipeline information
        """
        info = {
            'input': self.spec['input'],
            'steps': [step.get('operation') for step in self.spec['steps']],
            'output': self.spec.get('output'),
            'total_steps': len(self.spec['steps'])
        }
        
        print("Dry run - Pipeline configuration:")
        print(f"  Input: {info['input']}")
        print(f"  Steps ({info['total_steps']}):")
        for i, op in enumerate(info['steps'], 1):
            print(f"    {i}. {op}")
        print(f"  Output: {info['output']}")
        
        return info


def run_pipeline(spec: Union[Dict, str, Path], return_df: bool = False) -> Optional[DataFrame]:
    """Convenience function to run a pipeline from specification.
    
    Args:
        spec: Pipeline specification (dict, JSON string, or path to JSON file)
        return_df: If True, return the final DataFrame
        
    Returns:
        DataFrame if return_df is True, otherwise None
        
    Example:
        # From file
        run_pipeline('pipeline.json')
        
        # From dict
        spec = {
            'input': 'data.csv',
            'steps': [
                {'operation': 'cols_drop', 'params': {'cols': ['col1', 'col2']}},
                {'operation': 'df_filter', 'params': {'condition': "col('amount') > 100"}}
            ],
            'output': 'output.parquet'
        }
        run_pipeline(spec)
    """
    pipeline = PySparkPipeline(spec)
    return pipeline.execute(return_df=return_df)


def create_pipeline_template(output_path: str = 'pyspark_pipeline_template.json'):
    """Create a template pipeline JSON file with examples.
    
    Args:
        output_path: Path where template should be saved
    """
    template = {
        "description": "PySpark pipeline template with example transformations",
        "input": {
            "path": "gs://my-bucket/input.csv",
            "format": "csv",
            "options": {
                "header": True,
                "inferSchema": True
            }
        },
        "steps": [
            {
                "operation": "cols_drop",
                "params": {
                    "cols": ["unnecessary_col1", "unnecessary_col2"]
                }
            },
            {
                "operation": "cols_rename",
                "params": {
                    "mapping": {
                        "old_name": "new_name",
                        "id": "customer_id"
                    }
                }
            },
            {
                "operation": "cols_fillna",
                "params": {
                    "cols": ["amount", "quantity"],
                    "strategy": "zero"
                }
            },
            {
                "operation": "cols_toint",
                "params": {
                    "cols": ["quantity"]
                }
            },
            {
                "operation": "cols_todate",
                "params": {
                    "cols": "transaction_date",
                    "format": "yyyy-MM-dd"
                }
            },
            {
                "operation": "df_filter",
                "params": {
                    "condition": "col('amount') > 0"
                }
            },
            {
                "operation": "df_with_columns",
                "params": {
                    "exprs": {
                        "total": "col('quantity') * col('price')",
                        "profit": "col('revenue') - col('cost')"
                    }
                }
            },
            {
                "operation": "df_sort",
                "params": {
                    "by": "transaction_date",
                    "descending": False
                }
            },
            {
                "operation": "cols_collect_to_array",
                "params": {
                    "group_by": "customer_id",
                    "collect_cols": {
                        "transaction_date": "all_dates",
                        "amount": "all_amounts"
                    },
                    "sort_by": "transaction_date"
                }
            }
        ],
        "output": {
            "path": "gs://my-bucket/output.parquet",
            "format": "parquet",
            "options": {
                "mode": "overwrite",
                "partitionBy": ["year", "month"]
            }
        }
    }
    
    with open(output_path, 'w') as f:
        json.dump(template, f, indent=2)
    
    print(f"Template created at: {output_path}")


# Example usage
if __name__ == "__main__":
    # Example 1: Simple pipeline with expression parsing
    simple_spec = {
        "input": "data.csv",
        "steps": [
            {
                "operation": "cols_drop",
                "params": {"cols": ["col1", "col2"]}
            },
            {
                "operation": "df_filter",
                "params": {"condition": "col('amount') > 100"}
            },
            {
                "operation": "cols_fillna",
                "params": {"cols": "col3", "strategy": "mean"}
            }
        ],
        "output": "output.parquet"
    }
    
    # Example 2: Complex pipeline with expressions
    complex_spec = {
        "input": {
            "path": "gs://my-bucket/transactions.csv",
            "format": "csv"
        },
        "steps": [
            {
                "operation": "cols_rename",
                "params": {
                    "mapping": {"id": "transaction_id", "date": "transaction_date"}
                }
            },
            {
                "operation": "cols_todate",
                "params": {"cols": "transaction_date", "format": "yyyy-MM-dd"}
            },
            {
                "operation": "df_filter",
                "params": {
                    "condition": "(col('amount') > 0) & (col('status') == 'completed')"
                }
            },
            {
                "operation": "df_with_columns",
                "params": {
                    "exprs": {
                        "total_cost": "col('quantity') * col('unit_price')",
                        "profit_margin": "(col('revenue') - col('cost')) / col('revenue')",
                        "year": "year(col('transaction_date'))",
                        "month": "month(col('transaction_date'))"
                    }
                }
            },
            {
                "operation": "cols_collect_to_array",
                "params": {
                    "group_by": "customer_id",
                    "collect_cols": {
                        "transaction_date": "dates",
                        "amount": "amounts"
                    }
                }
            }
        ],
        "output": {
            "path": "gs://my-bucket/customer_summary.parquet",
            "format": "parquet"
        }
    }
    
    # Example 3: Expression examples
    expression_examples = {
        "input": "sales.csv",
        "steps": [
            # Simple comparison
            {
                "operation": "df_filter",
                "params": {"condition": "col('sales') > 1000"}
            },
            # Complex logical condition
            {
                "operation": "df_filter",
                "params": {
                    "condition": "((col('region') == 'West') & (col('sales') > 500)) | (col('vip') == True)"
                }
            },
            # String operations
            {
                "operation": "df_filter",
                "params": {
                    "condition": "col('name').contains('Inc')"
                }
            },
            # Date operations
            {
                "operation": "df_filter",
                "params": {
                    "condition": "col('date') > to_date(lit('2024-01-01'))"
                }
            },
            # Null checks
            {
                "operation": "df_filter",
                "params": {
                    "condition": "col('email').isNotNull()"
                }
            },
            # Creating new columns with expressions
            {
                "operation": "df_with_columns",
                "params": {
                    "exprs": {
                        "total": "col('price') * col('quantity')",
                        "discount_price": "col('price') * 0.9",
                        "full_name": "concat_ws(' ', col('first_name'), col('last_name'))",
                        "is_premium": "col('amount') > 1000",
                        "year_month": "concat(year(col('date')), lit('-'), month(col('date')))"
                    }
                }
            }
        ],
        "output": "filtered_sales.parquet"
    }
    
    # Create template
    create_pipeline_template()
    
    print("\nExpression Parser Features:")
    print("✓ col('name') - Column reference")
    print("✓ lit(value) - Literal value")
    print("✓ col('x') > 10 - Comparisons")
    print("✓ (col('x') > 10) & (col('y') < 20) - Logical operations")
    print("✓ col('price') * col('quantity') - Arithmetic")
    print("✓ col('text').contains('pattern') - String operations")
    print("✓ col('date') > to_date(lit('2024-01-01')) - Date operations")
    print("✓ col('value').isNull() / isNotNull() - Null checks")
    print("✓ year(col('date')), month(col('date')) - Date extraction")
    print("✓ upper(col('name')), lower(col('name')) - String functions")
    print("✓ concat_ws(' ', col('a'), col('b')) - Concatenation")
    print("\nExample usage:")
    print("1. run_pipeline('pipeline.json')")
    print("2. run_pipeline(spec_dict)")
    print("3. pipeline = PySparkPipeline('pipeline.json')")
    print("   pipeline.dry_run()")
    print("   pipeline.execute()")