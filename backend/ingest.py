import polars as pl
import chardet
from pathlib import Path
from typing import List, IO, Dict, Any, Union, Optional, Callable
import yaml
import inspect
from collections import defaultdict
import concurrent.futures

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

    def read_database(self, query: str, **kwargs) -> Optional[pl.DataFrame]:
        """Executes a SQL query and returns a Polars DataFrame."""
        try:
            return pl.read_database(query, **kwargs)
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
        
   """ To  Write a DataFrame to CSV"""
    def to_csv(self, df : pd.DataFrame, file_path: str(), separator: Optional[str] = ',', **kwargs):
    """ write a dataframe to a csv file """
     try:
        with open(file_path, 'w') as file:
             return df.to_csv(file, index=True, sep=separator, **kwargs)
     except Exception as e:
         print(f"Error on writing to CSV file: {e}")
         return False



    """ To write a DataFrame to Excel"""
    def to_excel(self, df: pd.DataFrame, file_path: str(), **kwargs):
        """ write a dataframe to an excel file """  
        try:git
            with pd.ExcelWriter(file_path, engine='xlsxwriter') as file:
                return df.to_excel(file, index=True, **kwargs)
        except Exception as e:
             print(f"Error on writing to Excel file: {e}")
             return False



    """ To write a DataFrame to JSON"""
    def to_json(self, df: pd.DataFrame, file_path: str(), **kwargs):
        """ write a dataframe to a json file """
        try:
            with open(file_path, 'w') as file:
                return df.to_json(file, orient='columns', lines=True, **kwargs)
        except Exception as e:
            print(f"Error on writing to JSON file: {e}")
            return False



    """ To write a DataFrame to SQL"""
    def to_sql(self, df: pd.DataFrame, table_name: str, con: str, **kwargs):
        """ write a dataframe to a sql table """
        try:
            with pd.SQLAlchemy.connect(con) as conn:
                return df.to_sql(table_name, conn, if_exists='replace', index=True, **kwargs)
        except Exception as e:
            print(f"Error on writing to SQL table: {e}")
            return False



    """ To Return first n rows"""
    def head(self, df: pd.DataFrame, file_path: str, n: int = 5, **kwargs):
        """ Return the first n rows of the DataFrame """
        try:
            return df.head(n)
        except Exception as e:
            print(f"Error on reading file: {e}")
            return None

cleaner = DataCleaner()
reader = DataReader()
renamer = FrameCleaner()

FUNCTION_MAP = {
    "read_csv": reader.read_csv,
    "drop_nans": cleaner.drop_nans,
    "rename": renamer.rename,
    "to_csv": reader.to_csv,
    "to_excel": reader.to_excel,
    "to_json": reader.to_json,  
    "to_sql": reader.to_sql,
    "head": reader.head
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