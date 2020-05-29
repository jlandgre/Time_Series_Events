import pandas as pd
import numpy as np
import datetime as dt

def init():
    data = {'Desc':[], 'colname':[], 'size':[], 'dtype_string':[], 'dtype_int':[],
            'dtype_float':[], 'isnull':[], 'notnull':[], 'Desc2':[], 'Val2':[], 'time':[]}
    dfDebug = pd.DataFrame(data=data)
    return dfDebug

def CountDTypeString(df, col):
    return df.loc[df[col].apply(lambda x: isinstance(x, str)), col].size
def CountDTypeInt(df, col):
    return df.loc[df[col].apply(lambda x: isinstance(x, int)), col].size
def CountDTypeFloat(df, col):
    return df.loc[df[col].apply(lambda x: isinstance(x, float)), col].size
def CountNull(df, col):
    return df.loc[df[col].isnull(), col].size
def CountNotNull(df, col):
    return df.loc[~df[col].isnull(), col].size

#Add a new log row to dfDebug
def loginfo(dfDebug, logtype, desc, df=None, col='', desc2='', val2=''):

    #Construct log row as a list of values
    if logtype == 'colinfo':
        lst = [desc, col, df[col].size, CountDTypeString(df, col), CountDTypeInt(df, col),
               CountDTypeFloat(df, col), CountNull(df, col), CountNotNull(df, col), desc2, val2, '']
    elif logtype == 'indexsize':
        lst = [desc,'',df.index.size, '', '', '', '', '', desc2, val2, '']
    elif logtype == 'time':
        lst = [desc, '', '', '', '', '', '', '', desc2, val2, dt.datetime.now().strftime('%H:%M:%S.%f')]
    elif logtype == 'info':
        lst = [desc, '','', '','', '','', '', desc2, val2, '']

    dfDebug.loc[dfDebug.index.size] = lst

    #Set integer type for count columns
    lst_desc_cols = ['Desc', 'colname', 'Desc2', 'Val2', 'time']
    for col in dfDebug.columns:
        if col not in lst_desc_cols: dfDebug[col] = dfDebug[col].astype('str')

    return dfDebug
