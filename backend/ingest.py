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


    def sort_values(self,df,column:str,ascending :bool=tool):
        try:
            if isinstance(df,pd.DataFrame):
                return df.sort_value(by=column,ascending=ascending)
            elif isinstance(pf,pl.DataFrame):
                return df.sort(column,descending=not ascending)
            else:
                raise TypeError("Unsupported DataFrame type")
        except Exception as e:
            print(f"Error sorting DataFrame by '{column}': {e}")
            return df
        

    def sort_index(self,df,ascending:bool=True):
        try:
            if isinstance(df,pd.DataFrame):
                return df.sort_index(ascending=ascending)
            elif isinstance(df,pl.DataFrame):
                return df.sort(df.column[0],descending=not ascending)
            else:
                raise TypeError("unsupported DataFrame type")
        except Exception as e:
               print(f"Error sorting DataFrame by index:{e}")
               return df


    def drop_index_rows(self, df, index_to_drop):
        try:
            if isinstance(df,pd.DataFrame):
                return df.drop(index_to_drop)
            elif isinstance(df,pl.DataFrame):
                return df.filter(~df.select(pl.col(df.columns[0])).to_series().is_in(index_to_drop))
            else:
                raise TypeError("Unsupported DataFrame type")
        except Exception as e:
               print(f"Error dropping rows from DataFrame:{e}")
               return df


    def unstack_dataframe(self, df):
        try:
            if isinstance(df,pl.DataFrame):
               return df.unstack()
            elif isinstance(pl.DataFrame):
               return pf.pivot(index='id',column='key',values='values')
            else:
                raise TypeError("unsupported DataFrame type")
        except Exception as e:
            print(f"Error unstacking DataFrame:{e}")
            return df
        

    def unstack_dataframe(self,df):
        try:
            if isinstance(df,p1.DataFrame):
                return df.unstack()
            elif isinstance(df, pd.DataFrame):
                df=df.apply(lambda row:row,axis=1)
                return
                df.pivot(index='id',columns='key',values='values')
            else:
                raise TypeError("Unsupported DataFrame type")
        except Exception as e:
            print(f"Error unstacking DataFrame:{e}")
            return df
    
    
    def unstack_dataframe(self,df):
        try:
            if isinstance(df,pd.DataFrame):
                 df=df.applymap(lambda X: X**2)
                 return df.unstack()
            else:
                 raise TypeError("Unsupported DataFrame type")
        except Exception as e:
             print(f"Error unstacking DataFrame: {e}")
             return df


    def apply_function(self,df,column_name,func):
        try:
            if isinstance(df,pd.DataFrame):
                 df[column_name]=df[column_name].map(func)
                 return df
            elif isinstance(df,pl.DataFrame):
             if column_name in df.columns:
                 df=df.with_columns(pl.col(column_name).map_elements(func).alias(column_name))
                 return df
             else:
                print(f"column'{column_nme}'not found in polars DataFrame.")
                return df
            else:
             raise TypeError("unsupported DataFrame type")
        except Exception as e:
             print(f"Error applying function: {e}")
             return df



    def aggregate_by_class(self,df,target_column):
        try:
            if isinstance(df,pd.DataFrame):
                return
                df.groupby(target_column).agg(['mean','min','max','count'])
            elif isinstance(df,pl.DataFrame):
                df_pd=df.to_pandas() 
                aggregate_df=df_pd.groupby(target_column).agg(['mean','min','max','count'])
                return
                pl.from_pandas(aggregated_df) 
            else:
             raise TypeError("unsupported DataFrame type")
        except Exception as e:
             print(f"Error during aggregation: {e}")
             return df


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
        


cleaner = DataCleaner()
reader = DataReader()
renamer = FrameCleaner()

FUNCTION_MAP = {
    "read_csv": reader.read_csv,
    "drop_nans": cleanerCCV.drop_nans,
    "rename": renamer.rename
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

def execute_workflow(yamlstring):x
    
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


class DataFramecolumn():
    def getcolumn(self,df:pl.DataFrame,column_name:str):
        try:
             if column_name in df.columns:
                 return df[column_name]
             else:
              print(f"column'{column_name}', not found.")
        except Exception as e:
              print(f"Error accessing column'{column_name}': {e}")
              return df


class polarsDataframe():
     def get_index(self,df:pl.DataFrame):
        try:
              df=df.with_row_count(name="index")
              return df["index"].to_list()
        except Exception as e:
               print(f"Error generating index: {e}")
               return df
                 
           
                 
                 
                             
                                                                              
                                



    