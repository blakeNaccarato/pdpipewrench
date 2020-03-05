def add_to_col(df, col_name, val):
    df = df.copy()
    df.loc[:, col_name] = df.loc[:, col_name] + val
    return df


def high_enough(df, col_name, val):
    return df.loc[:, col_name] > val
