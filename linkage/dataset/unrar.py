""" Get dataframes from .rar files """

import os
import csv
import pyunpack
import pandas as pd
import dask.dataframe as dd

from linkage.model.utils import save_dataframe


def filter_by_id(df, index_column):
    """Filter dataframe by the index.

    Filter out rows where the ID does not start with 'DE'.

    Args:
        df (pandas.DataFrame): Dataframe to perform the operation on.
        index_column (str): Name of the index.

    Returns:
        pandas.DataFrame: Filtered dataframe.
    """
    # Some IDs may contain asterisks or other characters
    # Therefore the filter includes all IDs beginning with 'DE'
    return df[df[index_column].str.contains('^DE', regex=True)]


def joining(df, id_df):
    """Join two dataframes.

    Both dataframes have to have the same index.

    Args:
        df (pandas.DataFrame): Dataframe to perform the operation on.
        id_df (pandas.DataFrame): Dataframe to join.

    Returns:
        pandas.DataFrame: Joined dataframe.
    """
    return df.join(id_df, how='inner')


def unrar_and_filter(rars, source_dir, dest_dir, source_file, dest_file, index_column, useful_columns, dtype=str):
    """Extract .rar files and process content into a dataframe.

    In a loop, .rar files are processed and then deleted.
    For each file, file is extracted and read into a dataframe.
    The format of the dataframe, e.g. index, useful columns
    and the data types, is specified during the reading.
    Next, the dataframe is filtered and appended to a list.
    Processed extracted file is then removed.
    At the end, all dataframes stored in the list are
    concatenated into a single dataframe.

    Args:
        rars (list): List of .rar files indices.
        source_dir: Source directory.
        dest_dir: Destination directory.
        source_file: Name of the extracted file.
        dest_file: Name of the file to store the dataframe in.
        index_column: Name of the index.
        useful_columns: Specify which columns to read. Other will be ignored.
        dtype: String or list of the column datatypes.

    Returns:
        pandas.DataFrame: Read dataframe.
    """
    result_list = []  # append each chunk df here

    for rar in rars:
        rar_file = source_file[:-4]
        bvd_id_name_rar = os.path.join(source_dir, f'{rar_file}.part0{rar}.rar')

        print(f'Unpacking {bvd_id_name_rar}')

        # Name of the BvD_ID_and_Name.part0x.rar after un-raring
        unrared_txt = os.path.join(dest_dir, source_file)

        # Unrar rar file to the intermediate directory
        pyunpack.Archive(bvd_id_name_rar).extractall(dest_dir)

        print('Reading..')

        # Process file
        # Read the large file with specified chunksize
        df = dd.read_csv(unrared_txt,
                         # index_col=INDEX_COL_NAMES,  # set index during reading
                         usecols=useful_columns,  # decide which columns to take
                         dtype=dtype,     # specify column types
                         engine='c',
                         error_bad_lines=False,
                         sep='\t',
                         quoting=csv.QUOTE_NONE,
                         encoding='utf8')  # .set_index(INDEX_COL_NAMES)

        print('Filtering..')

        # Perform operation on german_id_df and the chunk
        # result_df = joining(df)
        result_df = filter_by_id(df, index_column)

        result_df = result_df.compute(num_workers=2)

        result_list.append(result_df)

        print('Done.')

        # Remove intermediate file
        os.remove(unrared_txt)

    # Concatenate the list into dataframe
    df_concat = pd.concat(result_list)

    # Save the resulting dataframe
    save_dataframe(df_concat, dest_dir, dest_file)

    return df_concat


def unrar_names(type_unrar, source_dir, dest_dir, source_file, dest_file, index_column, useful_columns, dtype=str):
    """Call a function to extract and process company name data into a dataframe.

    Args:
        type_unrar: Type 'all' for all files, 'part01' for the first file only.
        source_dir: Source directory.
        dest_dir: Destination directory.
        source_file: Source file.
        dest_file: Destination file.
        index_column: Name of the index.
        useful_columns: Specify which columns to read. Other will be ignored.
        dtype: String or list of the column datatypes.

    Returns:
        pandas.DataFrame: Read dataframe containing company names.
    """
    if type_unrar == 'all':
        rars = list(range(1, 5))
    else:
        rars = [1]

    return unrar_and_filter(rars, source_dir, dest_dir, source_file, dest_file, index_column, useful_columns, dtype)


def unrar_addresses(type_unrar, source_dir, dest_dir, source_file, dest_file, index_column, useful_columns, dtype=str):
    """Call a function to extract and process address data into a dataframe.

    Args:
        type_unrar: Type 'all' for all files, 'part01' for the first file only.
        source_dir: Source directory.
        dest_dir: Destination directory.
        source_file: Source file.
        dest_file: Destination file.
        index_column: Name of the index.
        useful_columns: Specify which columns to read. Other will be ignored.
        dtype: String or list of the column datatypes.

    Returns:
        pandas.DataFrame: Read dataframe containing addresses.
    """
    if type_unrar == 'all':
        rars = list(range(1, 9))
    else:
        rars = [1]

    return unrar_and_filter(rars, source_dir, dest_dir, source_file, dest_file, index_column, useful_columns, dtype)
