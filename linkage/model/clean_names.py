""" Clean company names """

import os
import pkg_resources

os.environ["MODIN_ENGINE"] = "dask"
import modin.pandas as pd

from linkage.model.utils import replace_with_json
from linkage.model.utils import group_single_consecutive_letters

df_german_vocab = pd.read_csv(os.path.join('../data/external/most-common-words', 'derewo-40000.csv'), sep=" ")


def clean_names(df, column_name='NAME', remove_redundant=True):
    """Standardize company names.

    Should be applied on the alphabetical columns only.
    First, spaces on the beginning and the end
    of the field are added. Then, all non-alphanumerical
    characters are removed, followed by replacement or
    removal of words defined in the PDP.
    Then, single consecutive letters are joint to
    words, independent numbers are removed, multiple
    spaces are replaced with single spaces, and
    spaces at the beginning and the end are stripped.

    Args:
        df (pandas.DataFrame): Dataframe to perform the operation on.
        column_name (str): Name of the column to be standardized.
        remove_redundant (bool): If true, remove words defined in PDP.
    """

    parent_dir = pkg_resources.resource_filename("linkage.model.to_replace", "")

    # Every json should contain replacement of double space by single space
    file_names = [
        "replace_with_abbrev.json",     # Replace the different versions for the abbreviations
        "std_name.json",                # Replace the different versions for the abbreviations
        "corporates.json",              # Remove corporations
        # "non_corporates.json",          # Replace non-corporations
        "stem_name.json",               # Remove abbreviations, get stem
        "forgotten_german.json"         # Remove forgotten, e.g. DEUT, GERMANY
    ]

    column = df[column_name]

    # Add spaces at the beginning and end of each column value
    column = column.swifter.apply(lambda x: f' {x} ')

    # Remove all non-alphabetical characters, preserve unicode alphabets
    column = column.replace(r'[\W_]', r' ', regex=True)

    if remove_redundant:
        # Replace or remove redundant
        for fn in file_names:
            column = replace_with_json(column, parent_dir, fn)

    # Group single consecutive letters to words
    column.update(group_single_consecutive_letters(column))

    # Remove independent numbers
    column = column.str.replace(r' [0-9]+ ', r' ', regex=True)

    # Remove redundant spaces
    # Replace double space
    # Remove space at the beginning and end of the name
    column = column.str.replace(' +', ' ', n=-1, case=False, regex=True).str.strip()

    # Update the dataframe
    df.loc[:, column_name] = column


def _clean_with_dictionary(s):
    """Clean string with a dictionary.

    Parse string by space and search for the words
    to replace. Join remaining words to string.

    Args:
        s (str): String to clean with a dictionary.

    Returns:
        str: Cleaned string.
    """
    s_split = s.split()

    cleaned = ''

    for ch in s_split:
        if not (df_german_vocab['Wort'] == ch).any():
            cleaned = cleaned + ' ' + ch

    return cleaned


def clean_names_with_dictionary(df, column_name):
    """Clean standardized company names using dictionary.

    Read the dictionary and standardize it.
    Apply the dictionary cleaning to the dataframe.
    At the end, remove redundant spaces.

    Args:
        df (pandas.DataFrame): Dataframe to perform the operation on.
        column_name (str): Name of the column to be cleaned with a dictionary.
    """
    parent_dir = pkg_resources.resource_filename("linkage.model.to_replace", "")
    file_names = ["german_states.json",      # Remove names of German states
                  "basic.json"]

    german_chars = {'Ä': 'AE', 'Ö': 'OE', 'Ü': 'UE', 'ß': 'SS'}

    df_german_vocab['Wort'] = df_german_vocab['Wort'].str.upper().replace(german_chars, regex=True)

    column = df[column_name]

    column = column.swifter.apply(_clean_with_dictionary)\
        .swifter.apply(lambda x: f' {x} ')

    for fn in file_names:
        column = replace_with_json(column, parent_dir, fn)

    # Remove redundant spaces
    # Replace double space
    # Remove space at the beginning and end of the name
    column = column.str.replace(' +', ' ', n=-1, case=False, regex=True).str.strip()

    # Update the dataframe
    df.loc[:, column_name] = column
