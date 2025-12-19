"""
FrameFlow Polars Pipeline Orchestrator - JSON-driven data transformation wrapper.

This module enables declarative data transformations using JSON specifications.
Define your entire data pipeline in JSON and execute it programmatically.

Author: Stephen Chen
Version: 1.5.0
"""

import json
import re
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import polars as pl
from polars import LazyFrame

# Import all functions from the polars_lazy_ops module
# Assuming the module is named polars_lazy_ops and is importable
try:
    from polars_lazy_ops import *
except ImportError:
    print("Warning: polars_lazy_ops module not found. Make sure it's in your Python path.")


class ExpressionParser:
    """Parse string expressions into Polars expressions."""
    
    @staticmethod
    def parse(expr_str: str) -> pl.Expr:
        """Parse a string expression into a Polars expression.
        
        Supports:
        - col('name') -> pl.col('name')
        - lit(value) -> pl.lit(value)
        - Chained operations: col('x').sum()
        - Binary operations: col('x') + col('y')
        - Comparisons: col('x') > 10
        - Logical operations: (col('x') > 10) & (col('y') < 20)
        
        Args:
            expr_str: String expression to parse
            
        Returns:
            Polars expression
        """
        # Remove extra whitespace
        expr_str = expr_str.strip()
        
        # Replace shorthand functions with pl. prefix
        replacements = {
            r'\bcol\(': 'pl.col(',
            r'\blit\(': 'pl.lit(',
            r'\ball\(\)': 'pl.all()',
            r'\bwhen\(': 'pl.when(',
        }
        
        for pattern, replacement in replacements.items():
            expr_str = re.sub(pattern, replacement, expr_str)
        
        # Evaluate the expression in a safe namespace
        namespace = {
            'pl': pl,
            # Add common Python operations
            'True': True,
            'False': False,
            'None': None,
        }
        
        try:
            return eval(expr_str, {"__builtins__": {}}, namespace)
        except Exception as e:
            raise ValueError(f"Failed to parse expression '{expr_str}': {str(e)}")
    
    @staticmethod
    def parse_condition(condition: Union[str, pl.Expr]) -> pl.Expr:
        """Parse a condition (handles both string and Expr).
        
        Args:
            condition: String condition or Polars expression
            
        Returns:
            Polars expression
        """
        if isinstance(condition, str):
            return ExpressionParser.parse(condition)
        return condition
    
    @staticmethod
    def parse_expressions(exprs: Union[str, List[str], Dict[str, str]]) -> Union[pl.Expr, List[pl.Expr], Dict[str, pl.Expr]]:
        """Parse one or multiple expressions.
        
        Args:
            exprs: Single expression, list of expressions, or dict of expressions
            
        Returns:
            Parsed Polars expression(s)
        """
        if isinstance(exprs, str):
            return ExpressionParser.parse(exprs)
        elif isinstance(exprs, list):
            return [ExpressionParser.parse(e) if isinstance(e, str) else e for e in exprs]
        elif isinstance(exprs, dict):
            return {k: ExpressionParser.parse(v) if isinstance(v, str) else v for k, v in exprs.items()}
        return exprs


class PolarsPipeline:
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
    
    def _load_input(self) -> LazyFrame:
        """Load input data based on specification."""
        input_config = self.spec['input']
        
        # Check if input is a DataFrame/LazyFrame object
        if isinstance(input_config, (pl.DataFrame, pl.LazyFrame)):
            # Convert DataFrame to LazyFrame if needed
            if isinstance(input_config, pl.DataFrame):
                return input_config.lazy()
            return input_config
        
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
            storage_options = input_config.get('storage_options')
            read_options = input_config.get('options', {})
            
            if format_type == 'parquet':
                return parq_read(path, storage_options=storage_options, **read_options)
            elif format_type == 'csv':
                return csv_read(path, storage_options=storage_options, **read_options)
            elif format_type == 'json':
                return json_read(path, storage_options=storage_options, **read_options)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
        
        else:
            raise ValueError("'input' must be a string path, dict configuration, or DataFrame/LazyFrame")
    
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
    
    def _execute_step(self, df: LazyFrame, step: Dict) -> LazyFrame:
        """Execute a single transformation step."""
        operation = step.get('operation')
        params = step.get('params', {})
        
        if not operation:
            raise ValueError("Each step must have an 'operation' field")
        
        # Parse string expressions in params
        parsed_params = self._parse_params(params)
        
        # Get the function from globals (imported from polars_lazy_ops)
        func = globals().get(operation)
        
        if func is None:
            raise ValueError(f"Unknown operation: {operation}")
        
        # Execute the operation
        try:
            return func(df, **parsed_params)
        except Exception as e:
            raise RuntimeError(f"Error executing operation '{operation}': {str(e)}")
    
    def _parse_params(self, params: Dict) -> Dict:
        """Parse parameters, converting string expressions to Polars expressions.
        
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
        """Check if a string looks like a Polars expression.
        
        Args:
            value: String to check
            
        Returns:
            True if looks like an expression
        """
        # Check for common expression patterns
        expression_patterns = [
            r'\bcol\(',
            r'\blit\(',
            r'\bpl\.',
            r'\ball\(\)',
            r'\bwhen\(',
        ]
        
        return any(re.search(pattern, value) for pattern in expression_patterns)
    
    def _write_output(self, df: LazyFrame):
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
            storage_options = output_config.get('storage_options')
            write_options = output_config.get('options', {})
            
            if format_type == 'parquet':
                parq_write(df, path, storage_options=storage_options, **write_options)
            elif format_type == 'csv':
                csv_write(df, path, storage_options=storage_options, **write_options)
            elif format_type == 'json':
                json_write(df, path, storage_options=storage_options, **write_options)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
        
        else:
            raise ValueError("'output' must be a string path or dict configuration")
    
    def execute(self, return_df: bool = False) -> Optional[pl.DataFrame]:
        """Execute the entire pipeline.
        
        Args:
            return_df: If True, return the final DataFrame (collected)
            
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
            return self.df.collect()
        
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


def run_pipeline(spec: Union[Dict, str, Path], return_df: bool = False) -> Optional[pl.DataFrame]:
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
                {'operation': 'cols_fillna', 'params': {'cols': 'col3', 'strategy': 'mean'}}
            ],
            'output': 'output.parquet'
        }
        run_pipeline(spec)
    """
    pipeline = PolarsPipeline(spec)
    return pipeline.execute(return_df=return_df)


def create_pipeline_template(output_path: str = 'pipeline_template.json'):
    """Create a template pipeline JSON file with examples.
    
    Args:
        output_path: Path where template should be saved
    """
    template = {
        "description": "Pipeline template with example transformations",
        "input": {
            "path": "gs://my-bucket/input.csv",
            "format": "csv",
            "storage_options": {
                "google_application_credentials": "path/to/key.json"
            },
            "options": {
                "has_header": True,
                "separator": ","
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
                    "cols": ["quantity"],
                    "strict": False
                }
            },
            {
                "operation": "cols_todate",
                "params": {
                    "cols": "transaction_date",
                    "format": "%Y-%m-%d"
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
            "storage_options": {
                "google_application_credentials": "path/to/key.json"
            },
            "options": {
                "compression": "snappy"
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
                "params": {"cols": "transaction_date"}
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
                        "profit_margin": "(col('revenue') - col('cost')) / col('revenue')"
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
                    "condition": "(col('region') == 'West') & (col('sales') > 500) | (col('vip') == True)"
                }
            },
            # String operations
            {
                "operation": "df_filter",
                "params": {
                    "condition": "col('name').str.contains('Inc')"
                }
            },
            # Date operations
            {
                "operation": "df_filter",
                "params": {
                    "condition": "col('date') > lit('2024-01-01').str.to_date()"
                }
            },
            # Null checks
            {
                "operation": "df_filter",
                "params": {
                    "condition": "col('email').is_not_null()"
                }
            },
            # Creating new columns with expressions
            {
                "operation": "df_with_columns",
                "params": {
                    "exprs": {
                        "total": "col('price') * col('quantity')",
                        "discount_price": "col('price') * 0.9",
                        "full_name": "col('first_name') + lit(' ') + col('last_name')",
                        "is_premium": "col('amount') > 1000"
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
    print("✓ col('text').str.contains('pattern') - String operations")
    print("✓ col('date') > lit('2024-01-01').str.to_date() - Date operations")
    print("✓ col('value').is_null() - Null checks")
    print("\nExample usage:")
    print("1. run_pipeline('pipeline.json')")
    print("2. run_pipeline(spec_dict)")
    print("3. pipeline = PolarsPipeline('pipeline.json')")
    print("   pipeline.dry_run()")
    print("   pipeline.execute()")