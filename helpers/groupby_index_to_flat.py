import pandas as pd

def groupby_index_to_flat(df: pd.DataFrame) -> pd.DataFrame:
    new_names = dict([
        (col, '_'.join(col))
        if col[1] != 'first'
        else (col, col[0])
        for col
        in df.columns.to_flat_index()
    ])
    df.columns = df.columns.to_flat_index()
    return df.rename(columns=new_names)