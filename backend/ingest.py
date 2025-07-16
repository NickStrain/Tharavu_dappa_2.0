import polars as pl
import pandas as pd
import sqlite3
import chardet
from pathlib import Path
from typing import List, IO, Dict, Any, Union, Optional, Callable
import yaml
import inspect
from collections import defaultdict
import concurrent.futures
import unicodedata

class DataReader:
    """
    Handles data ingestion from multiple sources using Polars and Pandas.
    """

    def read_csv(self, file_path: str, separator: Optional[str] = ",", **kwargs) -> Optional[pl.DataFrame]:
        """Reads a CSV file into a Polars DataFrame."""
        try:
            with open(file_path, 'rb') as f:
                encoding = chardet.detect(f.read())['encoding']
            return pl.read_csv(file_path, encoding=encoding, separator=separator, **kwargs) #return DF
        except (UnicodeDecodeError, FileNotFoundError) as e:
            print(f"Error reading CSV: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
        return None

    def read_excel(self, file_path: str, **kwargs) -> Optional[pl.DataFrame]:
        """Reads an Excel file into a Polars DataFrame."""
        try:
            return pl.read_excel(file_path, **kwargs)
        except (FileNotFoundError, Exception) as e:
            print(f"Error reading Excel: {e}")
        return None

    def read_parquet(self, file_path: str, **kwargs) -> Optional[pl.DataFrame]:
        """Reads a Parquet file into a Polars DataFrame."""
        try:
            return pl.read_parquet(file_path, **kwargs)
        except (FileNotFoundError, Exception) as e:
            print(f"Error reading Parquet: {e}")
        return None

    def read_json(self, file_path: str, **kwargs) -> Optional[pl.DataFrame]:
        """Reads a JSON file into a Polars DataFrame."""
        try:
            return pl.read_json(file_path, **kwargs)
        except (FileNotFoundError, Exception) as e:
            print(f"Error reading JSON: {e}")
        return None

    def read_html(self, path: str, **kwargs) -> Optional[List[pd.DataFrame]]:
        """reads a html file"""
        try:
            return pd.read_html(path, **kwargs)
        except Exception as e:
            print(f"Error reading HTML: {e}")
        return None

    def read_database(self, query: str, **kwargs) -> Optional[pl.DataFrame]:
        """Executes a SQL query and returns a Polars DataFrame."""
        try:
            return pl.read_database(query, **kwargs)
        except Exception as e:
            print(f"Error executing query: {e}")
        return None

    def read_sql(self, query: str, db_path: str, **kwargs) -> Optional[pd.DataFrame]:
        try:
            return pd.read_sql_query(query, sqlite3.connect(db_path), **kwargs)
        except Exception as e:
            print(f"Error executing query: {e}")
        return None

    def write_csv(self, df: pl.DataFrame, file_path: Union[str, Path, IO[bytes]], separator: Optional[str] = ',', **kwargs):
        """Writes a Polars DataFrame to a CSV file."""
        try:
            df.write_csv(file_path, separator=separator, **kwargs)
        except Exception as e:
            print(f"Error writing CSV: {e}")

    def write_excel(self, df: pl.DataFrame, file_path: Union[str, Path, IO[bytes]], **kwargs):
        """Writes a Polars DataFrame to an Excel file."""
        try:
            df.write_excel(file_path, **kwargs)
        except Exception as e:
            print(f"Error writing Excel: {e}")

    def to_pickle(self, df: pl.DataFrame, file_path: Union[str, Path, IO[bytes]], **kwargs):
        """Saves a Polars DataFrame as a pickle file."""
        try:
            df.to_pickle(file_path, **kwargs)
        except Exception as e:
            print(f"Error saving pickle: {e}")

class FrameCleaner:
    """
    Handles DataFrame column renaming operations.
    """
    def rename(self, df: pl.DataFrame, mapping: Union[Dict[str, str], Callable[[str], str]], strict: bool = True) -> pl.DataFrame:
        """Renames columns in a Polars DataFrame."""
        try:
            return df.rename(mapping, strict=strict)
        except Exception as e:
            print(f"Error renaming columns: {e}")
            return df  # Return the original DataFrame if rename fails

    def select_rows_loc(self, df: pd.DataFrame, condition) -> Optional[pd.DataFrame]:
        try:
            return df.loc[condition]
        except Exception as e:
            print(f"Error selecting rows: {e}")
        return None

    def select_by_position(self, df: pd.DataFrame, rows, cols) -> Optional[pd.DataFrame]:
        try:
            return df.iloc[rows, cols]
        except Exception as e:
            print(f"Error using df.iloc: {e}")
        return None

    """Custom dataFrame cleaning operations"""
    def drop_constant_columns(self, df: pd.DataFrame, tol=0.0) -> Optional[pd.DataFrame]:
        try:
            return df.loc[:, df.nunique(dropna=False) > tol]
        except Exception as e:
            print(f"Error dropping constant columns: {e}")
        return None

    def clean_column_names(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        try:
            return df.rename(columns=lambda col: col.strip().lower().replace(" ", "_"))
        except Exception as e:
            print(f"Error cleaning column names: {e}")
        return None


class DataCleaner:
    """
    Handles NaN removal operations for Polars DataFrames.
    """
    def drop_nans(self, df: pl.DataFrame, subset: Optional[Union[str, list[str]]] = None) -> pl.DataFrame:
        """Drops all NaN values from the DataFrame."""
        try:
            return df.drop_nans(subset)
        except Exception as e:
            print(f"Error dropping NaNs: {e}")
            return df  # Return the original DataFrame if dropping NaNs fails


    def drop_nan(self, df: pl.DataFrame, subset: Optional[Union[str, list[str]]] = None) -> pl.DataFrame:
        """Drops all NaN values from the DataFrame."""
        try:
            return df.drop_nans(subset)
        except Exception as e:
            print(f"Error dropping NaNs: {e}")
            return df  # Return the original DataFrame if dropping NaNs fails

    def as_type(self, df: pd.DataFrame, schema: dict) -> Optional[pd.DataFrame]:
        try:
            return df.astype(schema)
        except Exception as e:
            print(f"Error casting columns: {e}")
        return None

    """Cleanup applied to text fields"""

    def normalize_unicode(self, df: pd.DataFrame, columns=None) -> Optional[pd.DataFrame]:
        try:
            for col in df.columns:
                if df[col].dtype == "object" and (columns is None or col in columns):
                    df[col] = df[col].apply(lambda x: unicodedata.normalize("NFKC", x) if isinstance(x, str) else x)
            return df
        except Exception as e:
            print(f"Error normalizing Unicode: {e}")
        return None

    def trim_whitespace(self, df: pd.DataFrame, columns=None) -> Optional[pd.DataFrame]:
        try:
            return df.apply(lambda col: col.str.strip() if col.dtype == "object" and (columns is None or col.name in columns) else col)
        except Exception as e:
            print(f"Error trimming whitespace: {e}")
        return None

    def fill_missing_with_group_stat(self, df: pd.DataFrame, group_cols, target_col, stat='mean') -> Optional[pd.DataFrame]:
        try:
            if stat == 'mean':
                return df.copy().fillna({target_col: df.groupby(group_cols)[target_col].transform('mean')})
            elif stat == 'median':
                return df.copy().fillna({target_col: df.groupby(group_cols)[target_col].transform('median')})
            elif stat == 'mode':
                return df.copy().fillna({target_col: df.groupby(group_cols)[target_col].transform(lambda x: x.mode().iloc[0] if not x.mode().empty else x)})
            else:
                print(f"Unsupported stat: {stat}")
                return None
        except Exception as e:
            print(f"Error filling missing values: {e}")
        return None
    
    """Outlier removal and transformation"""

    def remove_outliers(self, df: pd.DataFrame, cols, method='zscore', thresh=3) -> Optional[pd.DataFrame]:
        try:
            if method == 'zscore':
                return df[((df[cols] - df[cols].mean()) / df[cols].std()).abs().lt(thresh).all(axis=1)]
            else:
                Q1 = df[cols].quantile(0.25)
                Q3 = df[cols].quantile(0.75)
                IQR = Q3 - Q1
                mask = ~((df[cols] < (Q1 - 1.5 * IQR)) | (df[cols] > (Q3 + 1.5 * IQR))).any(axis=1)
                return df[mask]
        except Exception as e:
            print(f"Error removing outliers: {e}")
        return None
    
    def convert_dates(self, df: pd.DataFrame, columns, fmt=None) -> Optional[pd.DataFrame]:
        try: 
            return df.assign(**{col: pd.to_datetime(df[col], format=fmt, errors='coerce') for col in columns})
        except Exception as e: 
            print(f"Error converting dates: {e}")
        return None
    
    def extract_date_parts(self, df: pd.DataFrame, date_col: str) -> Optional[pd.DataFrame]:
        try: 
            return df.assign(year=df[date_col].dt.year, month=df[date_col].dt.month, day=df[date_col].dt.day)
        except Exception as e:
            print(f"Error extracting date parts: {e}")
        return None




cleaner = DataCleaner()
reader = DataReader()
renamer = FrameCleaner()

FUNCTION_MAP = {
    "read_csv": reader.read_csv,
    "drop_nans": cleaner.drop_nans,
    "rename": renamer.rename,
    "read_sql":reader.read_sql,
    "read_html":reader.read_html,
    "as_type":cleaner.as_type,
    "select_rows_loc":renamer.select_rows_loc,
    "select_by_position":renamer.select_by_position,
    "fill_missing_with_group_stat":cleaner.fill_missing_with_group_stat,
    "clean_column_names":renamer.clean_column_names,
    "drop_constant_columns":renamer.drop_constant_columns,
    "trim_whitespace":cleaner.trim_whitespace,
    "normalize_unicode":cleaner.normalize_unicode,
    "remove_outliers":cleaner.remove_outliers,
    "convert_dates":cleaner.convert_dates,
    "extract_date_parts":cleaner.extract_date_parts,
}


workflow_outputs = {}

def resolve_task_args(args):
    """Resolve argument references to actual values from workflow_outputs."""
    resolved_args = {}
    for key, value in args.items():
        if isinstance(value, str) and value in workflow_outputs:
            resolved_args[key] = workflow_outputs[value]
        else:
            resolved_args[key] = value
    return resolved_args

def execute_task(single_node,i, task_number):

    # task_id = nodes["id"]
    # function_name = task["function"]

    task_id = i
    function_name = single_node["function"]

    if function_name not in FUNCTION_MAP:
        print(f"[TASK {task_number}] Skipping {task_id}: Function {function_name} not found.")
        return None

    func = FUNCTION_MAP[function_name]
    args = resolve_task_args(single_node["params"])
    # args = single_node["params"]

    try:
        if "df" in args and not isinstance(args["df"], pl.DataFrame):
            raise TypeError(f"[TASK {task_number}] Error: Expected 'df' to be a Polars DataFrame, but got {type(args['df'])}")

        print(f"[TASK {task_number}] Running {task_id} ({function_name}) with args: {args}")
        result = func(**args)
        print(result)
        print(f"[TASK {task_number}] {task_id} completed.")


        if "vars" in single_node and isinstance(single_node["vars"], str):
            workflow_outputs[single_node["vars"]] = result
            print(f"[TASK {task_number}] Output saved as '{single_node['vars']}'")

        return result
    except Exception as e:
        print(f"[TASK {task_number}] Error in {task_id}: {e}")
    return None

def execute_workflow(yamlstring):

    # Load YAML
    # with open(yaml_path, "r") as f:
    #     workflow = yaml.safe_load(f)


    workflow = yaml.safe_load(yamlstring)
    nodes = workflow['nodes']
    # print(nodes)
    task_counter = 1  # Start numbering tasks
    for i in nodes:
        single_node = nodes[i]
        function_id = single_node["function"]
        result = execute_task(single_node,i, task_counter)
        task_counter += 1


        # if result is not None and "vars" in task:
        #     workflow_outputs[task["vars"]] = result

    return None


# workflow_outputs = execute_workflow("sample.yaml")