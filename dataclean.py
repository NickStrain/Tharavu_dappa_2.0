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

"""
Perform groupwisw transforms
"""
def transform(self, df, func, axis=0):
    try:
        if isinstance(df, pd.DataFrame):
            return df.transform(func, axis=axis)
        elif isinstance(df, pl.DataFrame):
            # Polars doesn't have a direct equivalent to pandas' transform method
            if axis == 0:
                return df.apply(func, axis=0)
            elif axis == 1:
                return df.apply(func, axis=1)
            else:
                raise ValueError("Invalid axis")
        else:
            raise TypeError("Unsupported DataFrame type")
    except Exception as e:
        print(f"[transform] Error during transformation: {e}")
        raise  

"""
Subset rows or columns by label or regex
"""
def filter(self, df, condition):
    try:
        if isinstance(df, pd.DataFrame):
            return df[condition]
        elif isinstance(df, pl.DataFrame):
            return df.filter(condition)
        else:
            raise TypeError("Unsupported DataFrame type")
    except Exception as e:
        print(f"[filter] Error during filtering: {e}")
        raise  

"""
Randomly sample rows
"""
def sample(self, df, n=None, frac=None, random_state=None):
    try:
        if isinstance(df, pd.DataFrame):
            return df.sample(n=n, frac=frac, random_state=random_state)
        elif isinstance(df, pl.DataFrame):
            if n is not None:
                return df.sample(n=n, seed=random_state)
            elif frac is not None:
                return df.sample(frac=frac, seed=random_state)
            else:
                raise ValueError("Either n or frac must be specified")
        else:
            raise TypeError("Unsupported DataFrame type")
    except Exception as e:
        print(f"[sample] Error during sampling: {e}")
        raise  
"""
Count unique values in a Series
"""
def value_counts(self, series):
    try:
        if isinstance(series, pd.Series):
            return series.value_counts()
        elif isinstance(series, pl.Series):
            return series.value_counts()
        else:
            raise TypeError("Unsupported Series type")
    except Exception as e:
        print(f"[value_counts] Error during value counting: {e}")
        raise  

"""
Array of unique values
"""
def unique(self, series):
    try:
        if isinstance(series, pd.Series):
            return series.unique()
        elif isinstance(series, pl.Series):
            return series.unique().to_numpy()  #to_numpy() to ensure the op is numpy array,similar to pd's unique
        else:
            raise TypeError("Unsupported Series type")
    except Exception as e:
        print(f"[unique] Error during getting unique values: {e}")
        raise  

"""
Pairwise correlation
"""
def corr(self, df):
    try:
        if isinstance(df, pd.DataFrame):
            return df.corr()
        elif isinstance(df, pl.DataFrame):
            return df.to_pandas().corr() # polar df doesn't have built in corr method  , so using to_pandas
        else:
            raise TypeError("Unsupported DataFrame type")
    except Exception as e:
        print(f"[corr] Error during correlation calculation: {e}")
        raise  