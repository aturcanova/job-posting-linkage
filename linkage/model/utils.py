""" Utility functions """

import os
import re
import json
import numpy as np
import pandas as pd


class Replacements:
    """Load and store a dictionary from a JSON file.
    """

    def __init__(self, json_path):
        """Initialization.

        Args:
            json_path: Path to the JSON file.
        """
        self.update(json_path)

    def update(self, json_path):
        """ Load configuration from a JSON file.

        Args:
            json_path (str): A path to the JSON file.
        """
        with open(json_path, encoding='utf-8') as f:
            config = json.load(f)
            self.__dict__.update(config)


def group_by_removing_space(x):
    """Remove spaces in between single consecutive letters.

    Args:
        x: Regex group containing single consecutive letters.

    Returns:
        str: Word created from the single consecutive letters.
    """
    x = x.group().replace(' ', '')
    x = x + ' '
    return x


def group_single_consecutive_letters(column):
    """ Group single consecutive letters in Pandas Series.

    Find single consecutive letters resulting from
    the standardization, e.g. B I G. Remove spaces in between
    single consecutive letters to create words, e.g. BIG.

    Args:
        column (pandas.Series): Column to perform the operation on.

    Returns:
        pandas.Series: Modified column.
    """
    # Create a new dataframe with company names containing more than one consecutive single letter words
    column = column.loc[(column.notna() & column.str.contains(r'\b((?:\w ?\b){2,})', regex=True))]

    # Remove spaces to create words
    column = column.str.replace(r"\b((?:\w) ?\b){2,}", group_by_removing_space, regex=True)

    return column


def isna(string):
    """Check if the string is NaN.

    Args:
        string: String or pandas.Series to check.

    Returns:
        bool: True if string is NaN, False otherwise.
    """
    return string != string


def most_common(arr):
    """Get most common value in an array.

    Args:
        arr: An array to get the most common value from.

    Returns:
        The most common value in the array.
    """
    values, counts = np.unique(arr, return_counts=True)
    index = np.argmax(counts)
    return values[index]


def replace_with_json(column, parent_dir, file_name):
    """Replace values in column by values specified in a JSON file.

    Args:
        column (pandas.Series): Column to perform the operation on.
        parent_dir: Parent directory of the JSON file.
        file_name: JSON file.

    Returns:
        pandas.Series: Modified column.
    """
    # Read JSON file to the Replacements (dictionary)
    replacements = Replacements(os.path.join(parent_dir, file_name))
    # Pre-compile the dictionary
    replacements_compile = {re.compile(k): v for k, v in replacements.__dict__.items()}

    # Replace values in a column using the dictionary
    column = column.replace(replacements_compile, regex=True)

    return column


def zip_fill(x, num_to_preserve=2, zip_mean=False):
    """Substitute or fill the last digits of a ZIP code with 'x'.

    Args:
        x: A ZIP code to substitute values in.
        num_to_preserve: Number of digits not to be replaced, starting from beginning.
        zip_mean (bool): If true, function was called during German-Zip-Codes processing.

    Returns:
        str: Modified ZIP code containing one or more 'x'.
    """
    x = str(x)
    x = re.sub('[^0-9]', '', x)
    if num_to_preserve is 0:
        num_to_preserve = len(x)
    elif num_to_preserve < 1 or num_to_preserve > 4:
        num_to_preserve = 2

    if zip_mean and len(x) == 5:  # During averaging, zip codes starting with 0 loose the 0
        x = '0' + x

    x = x[:num_to_preserve]
    y = (5 - num_to_preserve) * 'x'
    y = x + y

    return y


def save_dataframe(df, dest_dir, file_name):
    """Save a dataframe to a specified file and a directory.

    Args:
        df (pandas.DataFrame): Dataframe to be saved.
        dest_dir: Directory to save the file to.
        file_name: File to save the dataframe in.
    """
    # Create parent directory if does not exist
    os.makedirs(dest_dir, exist_ok=True)

    # Save dataframe to a csv file
    df.to_csv(os.path.join(dest_dir, file_name), sep='\t')

    print('Successfully saved.')


def read_dataframe(source_dir, file_name, index=None, useful_cols=None, dtype=str, separator='\t'):
    """Read a dataframe from a specified .csv file.

    Args:
        source_dir: A directory where the file is located.
        file_name: A file to be read the dataframe from.
        index: Index of the dataframe.
        useful_cols: If not None, read only specified columns of the dataframe.
        dtype: Data types of the columns, list or type.
        separator: Separator used in the .csv file.

    Returns:
        pandas.DataFrame: Read dataframe.
    """

    df_concat = pd.read_csv(os.path.join(source_dir, file_name),
                            index_col=index,
                            usecols=useful_cols,
                            dtype=dtype,
                            error_bad_lines=False,
                            sep=separator
                            )

    return df_concat
