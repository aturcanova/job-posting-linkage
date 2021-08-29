""" Check dataframe """

from IPython.display import display


def contains_all_nan(df, print_df=True):
    """Print a number of rows containing only NaN values.

    In case the resulting dataframe is not empty,
    display the dataframe.

    Args:
        df (pandas.DataFrame): Dataframe to be examined.
        print_df (bool): If True, dataframe will be displayed.
    """
    row_is_NaN = (df.isnull()).all(axis=1)

    row_is_NaN_df = df[row_is_NaN]

    df_length = len(row_is_NaN_df)

    print(f"Number of NaN rows: {df_length}")

    if df_length > 0 and print_df:
        display(row_is_NaN_df.head())


def contains_any_nan(df, print_df=True):
    """Print a number of rows containing some NaN values.

    In case the resulting dataframe is not empty,
    display the dataframe.

    Args:
        df (pandas.DataFrame): Dataframe to be examined.
        print_df (bool): If True, dataframe will be displayed.
    """
    row_has_NaN = (df.isnull()).any(axis=1)

    row_has_NaN_df = df[row_has_NaN]

    df_length = len(row_has_NaN_df)

    print(f"Number of NaN rows: {df_length}")

    if df_length > 0 and print_df:
        display(row_has_NaN_df.head())


def column_contains_nan(df, column_name, print_df=True):
    """Print a number of rows containing NaN value in the column.

    In case the resulting dataframe is not empty,
    display the dataframe.

    Args:
        df (pandas.DataFrame): Dataframe to be examined.
        column_name (str): Name of a column to be examined.
        print_df (bool): If True, dataframe will be displayed.
    """
    column_is_NaN = df[column_name].isna()

    column_is_NaN_df = df[column_is_NaN]

    df_length = len(column_is_NaN_df == True)

    print(f"Number of NaN rows: {df_length}")

    if df_length > 0 and print_df:
        display(column_is_NaN_df.head())


def drop_all_nan(df):
    """Drop rows containing only NaN values and print number of rows left.

    Args:
        df (pandas.DataFrame):
    """
    df.dropna(how='all', inplace=True)

    print(f"Number of rows left: {len(df)}")


def drop_subset_nan(df, subset):
    """Drop rows containing some NaN values and print number of rows left.

    Args:
        df (pandas.DataFrame):
        subset: Sting or list with column names to consider.
    """
    if isinstance(subset, str):
        df.dropna(subset=[subset], inplace=True)
    else:
        df.dropna(subset=subset, inplace=True)

    print(f"Number of rows left: {len(df)}")


def count_redundant_spaces(df, column_name):
    """Print number of rows containing redundant spaces.

    Args:
        df (pandas.DataFrame): Dataframe to be examined.
        column_name (str): Column name to count spaces in.
    """
    # Space at the beginning
    # The result should be empty dataframe
    print(f"Space at the beginning: {len(df[df[column_name].str.contains('^ ', regex=True) == True])} records")

    # Space at the end
    # The result should be empty dataframe
    print(f"Space at the end:       {len(df[df[column_name].str.contains(' $', regex=True) == True])} records")

    # Double spaces
    # The result should be empty dataframe
    print(f"Double space:           {len(df[df[column_name].str.contains('  ', regex=True) == True])} records")


def print_dataframe_length(df):
    """Print length of a dataframe with a description.

    Args:
        df (pandas.DataFrame): Dataframe to be examined.
    """
    print(f"The length of the dataframe: {len(df)}")
