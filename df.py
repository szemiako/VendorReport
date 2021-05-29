import filemask
from glob import glob
import numpy as np
import pandas as pd
from pandas import DataFrame
from setup import FILETYPES_TO_IGNORE
from setup import os
from setup import THIRD_PARTY
from setup import THIS_DIR
from typing import Callable, List, Set

'''Module containing methods to interact with and transform DataFrame
objects, for both export files sent, expected files to be proecssed,
and actual files processed.'''

def df_columns(kind: str) -> list:
    '''Columns for different types of DataFrames to be created.'''
    cols = {
        'configs': [
            'Vendor',
            'Name',
            'Offset',
            'VendorNumber'
        ],
        'expected': [
            'Vendor',
            'Name',
            'FileMask',
            'FileType',
            'Offset',
            'VendorNumber',
            'FeedType'
        ],
        'actual': [
            'FileMask',
            'VendorNumber',
            'FeedType',
            'FileName',
            'FileType',
            'Download',
            'Archive',
            'Start',
            'End', 
            'Vendor',
            'Name'
        ]
    }
    return cols[kind]

def get_feed_type(df: DataFrame) -> DataFrame:
    '''Using the map function, get the "feed delivery type" from the broker
    number (brokers that use third party clearing firms all have the same
    broker number).'''
    df['FeedType'] = df['VendorNumber'].map(THIRD_PARTY).fillna('Direct')
    return df

def fix_file_mask(df: DataFrame) -> DataFrame:
    '''Using the apply function, fix all filemasks using the database
    configuration and offset value.'''
    df['FixedFileMask'] = df.apply(lambda x: filemask.Filemask(x['FileMask'], x['Offset']).mask, axis=1)
    return df

def apply_mapping(kind: str, df: DataFrame) -> DataFrame:
    '''Determine using mapper dict and apply the applicable changes
    to the DataFrame.'''
    mapper = {
        'configs': get_feed_type,
        'expected': fix_file_mask
    }
    if mapper.get(kind): df = mapper[kind](df)
    return df

def df_from_sql(kind: str, results: list) -> DataFrame:
    '''Create DataFrame from results of: connection.BaseConnection._execute_select_all()
    If we are looking for expected files, also fix the filemask to the
    preceeding date.'''
    df = DataFrame([list(i) for i in results], columns=df_columns(kind))
    df = apply_mapping(kind, df)
    return df

def config_converter(df: DataFrame) -> List[List[str]]:
    '''Dict comprehension to convert configurations DataFrame into list
    of lists for SQL query.'''
    return {
        i: {
            'Vendor': v[0],
            'Name': v[1],
            'Offset': v[2],
            'VendorNumber': v[3],
            'FeedType': v[4]
        } for i, v in enumerate(df[[
            'Vendor',
            'Name',
            'Offset',
            'VendorNumber',
            'FeedType'
        ]].values.tolist())
    }

def expected_converter(df: DataFrame) -> List[List[str]]:
    '''Dict comprehension to convert exepected configurations DataFrame
    into list of lists for SQL query.'''
    return {
        i: {
            'Mask': v[0],
            'Vendor': v[1],
            'Name': v[2],
            'VendorNumber': v[3],
            'FeedType': v[4]
        } for i, v in enumerate(df[[
            'FixedFileMask',
            'Vendor',
            'Name',
            'VendorNumber',
            'FeedType'
        ]].values.tolist())
    }

def configs_to_dict(kind: str, df: DataFrame) -> List[List[str]]:
    '''Get the required dict comprehension converter, and apply the
    conversion.'''
    mapper = {
        'configs': config_converter,
        'expected': expected_converter
    }
    return mapper[kind](df)

def get_export_path(filemask: str) -> str:
    '''Locate the latest file for a given jurisdiction's filemask pattern.'''
    return sorted(glob(f'{THIS_DIR}/exports/{filemask}'), key=os.path.getmtime, reverse=True)[0] 

def export_jurisdictions() -> dict:
    '''Current jurisdictions and their wildcard filemasks. New
    jursidictions can be added here. The jurisdiction is also the
    company's name.'''
    return {
        '3ec4': '3ec4_status_*.csv',
        '52fb': '52fb_status_*.csv'
    }

def build_export_df() -> DataFrame:
    '''Build the DataFrame of all export data. Loop through all
    jursidictions and get file's data, loaded into one DataFrame.'''
    jurisdictions = export_jurisdictions()
    export_df = DataFrame()

    for i in jurisdictions:
        export_path = get_export_path(jurisdictions[i])
        if export_path != '':
            df = pd.read_csv(export_path)
            df['Name'] = i            # The jurisdiction is also the company's name.
            export_df = export_df.append(df)
            df = df.iloc[0:0]                # Clear out the temporary DataFrame, df.
    
    return export_df

def filter_data(df: DataFrame) -> DataFrame:
    '''Filter out any files to not process based on the Filetype.'''
    df = df[~df['FileType'].isin(FILETYPES_TO_IGNORE)] # ~ operator means "not", such that "FileType" is "not in" filetypes to ignore.
    return df

def group_actual_data(df: DataFrame, keys: List, date_cols: Set) -> List[DataFrame]:
    '''Condense (i.e., aggregate) DataFrame columns. For each date col,
    find the appropriate aggregate value and group on the keys to get
    that value. Then, return the results as a list of DataFrames for
    future consolidation.'''
    def _function_map() -> dict:
        '''Dictionary of the date columns that we are interested in, and
        the aggregate functions that we want to perform on each.'''
        return {
            'Download': 'min',
            'Start': 'min',
            'End': 'max'
        }

    def _get_grouped(df: DataFrame, keys: List, col: str, func: Callable) -> DataFrame:
        '''Helper function to: i. Use a subset of columns from the
        "parent" DataFrame; and, ii. Get a desired aggregate value
        based on the keys.'''
        _keys = keys[:]
        _keys.append(col)

        df = df[_keys]
        df = df.groupby(keys).agg({col: func}) # agg method is equivalent to: df.groupby(keys)[col].func
        df = df.reset_index()                  # reset_index() so that we can merge DataFrames later.
        return df

    dfs = []
    fm = _function_map()

    for i in date_cols:
        dfs.append(_get_grouped(df, keys, i, fm[i])) # Apply the aggregation, and append it to output list.
    
    return dfs

def consolidate_data(dfs: List[DataFrame], keys: List) -> DataFrame:
    '''For each aggregate DataFrame (i.e., DataFrame that contains a
    desired aggregated value), merge the DataFrames together into one
    "unique" DataFrame.'''
    consolidated = dfs[0] # Seed output DataFrame
    for i in dfs[1:]:     # Already seeded ouput DataFrame, so start from index 1.
        consolidated = consolidated.merge(
            i,
            left_on=keys,
            right_on=keys,
            how='left',
            suffixes=['', '_r']
        )
    # All "duplicated" columns will end with suffix '_r'
    # (e.g., Vendor_r_r_r). Shouldn't be any, but this is a sanity-check.
    consolidated = consolidated.drop(columns=[col for col in consolidated.columns if col.endswith('_r')])
    return consolidated

def group_and_consolidate(df: DataFrame, keys: List, date_cols: Set) -> DataFrame:
    '''Helper function to execute each "aggregation" and "consolidation".
    We go from one (1) DataFrame to many, to many DataFrames back to
    one (1).'''
    dfs = group_actual_data(df, keys, date_cols) # 1 to Many
    df = consolidate_data(dfs, keys)             # Many to 1
    return df

def to_datetime(df: DataFrame, date_cols: Set) -> DataFrame:
    '''Force type change for specific columns.'''
    for i in date_cols:
        df[i] = df[i].astype(np.datetime64)
    return df

def apply_feed_type(df: DataFrame, source: DataFrame) -> DataFrame:
    return df.merge(
        source,
        left_on=['Vendor'],
        right_on=['Vendor'],
        suffixes=('', '_r'),
        how='outer'
    )