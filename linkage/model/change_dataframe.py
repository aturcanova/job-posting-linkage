""" Change dataframe """

import ftfy
import swifter
import unihandecode

from linkage.model.utils import isna


def replace_german_characters(df, columns):
    """ Replace german characters with umlaut with their Latin version.

    Args:
        df (pandas.DataFrame): Dataframe to perform the operation on.
        columns: String or list with column names.
    """

    german_chars = {'Ä': 'AE', 'Ö': 'OE', 'Ü': 'UE', 'ß': 'SS'}

    if isinstance(columns, str):
        df.replace({columns: german_chars}, inplace=True, regex=True)
    else:
        for column_name in columns:
            df.replace({column_name: german_chars}, inplace=True, regex=True)


def _repair_unicode(x):
    """ Use the library Fix Text For You (ftfy) to repair broken Unicode characters.

    Args:
        x (pandas.Series): Values to perform the operation on.

    Returns:
        pandas.Series: Modified series without broken Unicode characters.
    """
    if not isna(x):
        return ftfy.ftfy(x.upper())
    else:
        return x


def repair_broken_unicode(df, columns):
    """ Repair broken Unicode.

    Args:
        df (pandas.DataFrame): Dataframe to perform the operation on.
        columns: String or list with column names.
    """
    if isinstance(columns, str):
        df.loc[:, columns] = df[columns].swifter.apply(_repair_unicode)
    else:
        for column_name in columns:
            df.loc[:, column_name] = df[column_name].swifter.apply(_repair_unicode)


def _normalize_unicode(x):
    """Use the library Unihandecode to replace other Latin characters with their Latin version.

    Args:
        x (pandas.Series): Values to perform the operation on.

    Returns:
        pandas.Series: Modified series with only standard Latin characters.
    """
    if not isna(x):
        return unihandecode.unidecode(x)
    else:
        return x


def replace_other_latin_characters(df, columns):
    """Replace non-standard Latin characters.

    Args:
        df (pandas.DataFrame): Dataframe to perform the operation on.
        columns: String or list with column names.
    """
    if isinstance(columns, str):
        df.loc[:, columns] = df[columns].swifter.apply(_normalize_unicode)
    else:
        for column_name in columns:
            df.loc[:, column_name] = df[column_name].swifter.apply(_normalize_unicode)
