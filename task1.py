import polars as pl

class DataFramecolumn:
    def getcolumn(self,df:pl.DataFrame,column_name:str):
        try:
             if column_name in df.columns:
                 return df[column_name]
             else:
              print(f"column'(column_name), not found.")
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
