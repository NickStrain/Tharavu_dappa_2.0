import pandas as pd
import polars as pl
from typing import Optional,Union,List

class Datacleaner:
    """
    remove missing values
    """
    def drop_nans(self, df, subset: Optional[Union[str, List[str]]] = None):                                                                                          
        try:
            if isinstance(df, pd.DataFrame):    #pandas logic
                return df.dropna(subset)      # subset : null checking & row droping to specific columns
            elif isinstance(df,pl.DataFrame):   #polar logic
                return df.drop_nulls(subset)
            else:
                raise TypeError("unsupported DataFrame type")
        except Exception as e:
           print(f"[drop_nans] Error during NaN removal:{e}")
           return df                     #return original df if error


    def check_nulls(self, df, subset: Optional[Union[str, list[str]]] = None):
        try:
           if isinstance(df,pd.DataFrame):
              return df.isnull(subset)
           elif isinstance(df,pl.Dataframe):
              return(df.select(pl.all().is_null()))
           else:
              raise TypeError("unsupported DataFrame type")
        except Exception as e:
           print(f"[check_nulls] Error during null check:{e}")
           return None
                                              


                         # library          check null                    drop na
                         # pandas            df.isnull                   df.dropna()
                         # polars       df.select(pl.all().is_null())    df.drop_nulls()

    
    """
    Fill missing values
    """
    def fill_nans(self, df, value, subset: Optional[Union[str, List[str]]] = None):
        try:
            if isinstance(df, pd.DataFrame):
                return df.fillna(value=value)
            elif isinstance(df, pl.DataFrame):
                return df.fill_null(value=value)
            else:
                raise TypeError("Unsupported DataFrame type")
        except Exception as e:
            print(f"[fill_nans] Error during NaN filling: {e}")
            raise  

    """
    Boolean mask of missing values
    """
     
    def is_null(self, df, subset: Optional[Union[str, List[str]]] = None):
        try:
            if isinstance(df, pd.DataFrame):
                return df.isnull()
            elif isinstance(df, pl.DataFrame):
                return df.is_null()
            else:
                raise TypeError("Unsupported DataFrame type")
        except Exception as e:
            print(f"[is_null] Error during null check: {e}")
            return None

    """
     Boolean mask of non misssing values
    """

    def not_null(self, df, subset: Optional[Union[str, List[str]]] = None):
        try:
            if isinstance(df, pd.DataFrame):
                return df.notnull()
            elif isinstance(df, pl.DataFrame):
                return df.is_not_null()
            else:
                raise TypeError("Unsupported DataFrame type")
        except Exception as e:
            print(f"[not_null] Error during null check: {e}")
            return None

    """
    Remove duplicate rows
    """

    def drop_duplicates(self, df, subset: Optional[Union[str, List[str]]] = None, keep: str = 'first'):
        try:
            if isinstance(df, pd.DataFrame):
                return df.drop_duplicates(subset=subset, keep=keep)
            elif isinstance(df, pl.DataFrame):
                return df.unique(subset=subset, keep=keep)
            else:
                raise TypeError("Unsupported DataFrame type")
        except Exception as e:
            print(f"[drop_duplicates] Error during duplicate removal: {e}")
            raise  
