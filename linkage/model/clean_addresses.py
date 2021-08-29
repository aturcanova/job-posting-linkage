""" Clean addresses """

import swifter
import pkg_resources

from linkage.model.utils import replace_with_json, isna
from linkage.model.utils import group_single_consecutive_letters


def clean_addresses(df, column_name):
    """Standardize address column.

    Should be applied on the alphabetical columns only.
    All non-alphanumerical characters are removed,
    except fot the '|' needed for the Orbis dataset.
    Then, single consecutive letters are joint to
    words, independent numbers are removed, multiple
    spaces are replaced with single spaces, and
    spaces at the beginning and the end are stripped.

    Args:
        df (pandas.DataFrame): Dataframe to perform the operation on.
        column_name (str): Name of the column to be standardized.
    """
    parent_dir = pkg_resources.resource_filename("linkage.model.to_replace", "")
    file_names = ["redundant_in_addresses.json",  # Remove names of German states
                  ]

    column = df[column_name]

    # Addresses to uppercase
    # apply func helps to preserve only numerical values otherwise they are deleted by upper func
    column = column.swifter.apply(lambda x: f' {x} ' if not isna(x) else x)

    # Remove all non-alphabetical characters, preserve unicode alphabets
    column = column.str.replace(r'[^\w|]|[_]', r' ', regex=True)

    for fn in file_names:
        column = replace_with_json(column, parent_dir, fn)

    # Replace double space
    column = column.str.replace(' +', ' ', n=-1, case=False, regex=True)

    # Group single consecutive letters to words
    column.update(group_single_consecutive_letters(column))

    # Remove independent numbers
    column = column.str.replace(r'[0-9]+', r' ', regex=True)

    # Remove redundant spaces
    # Replace double space
    # Remove space at the beginning and end of the name
    column = column.str.replace(' +', ' ', n=-1, case=False, regex=True).str.strip()

    # Update the dataframe
    df.loc[:, column_name] = column


def replace_english_names(df, column_name):
    """Replace English names with their German equivalents.

    English to German translatins are stored in a dictionary
    at 'linkage/model/to_replace/english_names_german_cities.json'.

    Args:
        df (pandas.DataFrame): Dataframe to perform the operation on.
        column_name (str): Name of the column to replace values of.
    """
    parent_dir = pkg_resources.resource_filename("linkage.model.to_replace", "")
    file_name = 'english_names_german_cities.json'

    column = df[column_name]

    # Replace English names with their German equivalents
    column = replace_with_json(column, parent_dir, file_name)

    # Update the dataframe
    df.loc[:, column_name] = column
