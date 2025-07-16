import polars as pl
import pandas as pd

class DataFramecolumn:
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
    

class DataReader:
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
        
class DataRaeder:
     def sort_index(self,df,ascending:bool=True)
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


class DataReader:
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
