""" Read and process German-Zip-Codes file """

from linkage.model.utils import zip_fill, read_dataframe


class GermanZipCodes:
    """Store adapted 'German-Zip-Codes' in dataframes.

    Attributes:
        original_zipcodes_df (pandas.DataFrame): Contains unchanged data read from 'German-Zip-Codes.csv'.
        zip_df (pandas.DataFrame): Contains standardized version of 'original_zipcodes_df'.
        zip_mean_df (pandas.DataFrame): Zipcodes are averaged and the last three digits are substituted for 'x'.
    """

    def __init__(self):
        self.original_zipcodes_df = self._read_zip_df()
        self.zip_df = self._get_zip_df()
        self.zip_mean_df = self._get_zip_mean_df()

    def _read_zip_df(self):
        """ Read table stored in 'data/external/german-zip-codes/German-Zip-Codes.csv' to a dataframe.

        Returns:
            pandas.DataFrame: Non-standardized 'German-Zip-Codes'.
        """

        file_name = 'German-Zip-Codes.csv'
        file_path = '../data/external/german-zip-codes'
        usecols = ['Ort', 'Plz', 'Bundesland']

        zip_df = read_dataframe(file_path, file_name, None, usecols, dtype=str, separator=";")

        return zip_df

    def _get_zip_df(self):
        """Standardize 'original_zipcodes_df'.

        Numerical columns of 'German-Zip-Codes' are standardized.
        String is first converted to uppercase, and all
        non-alphanumerical characters are removed.
        The German characters with umlaut are replaced
        with their Latin equivalents.
        Multi-spaces are replaced with single spaces,
        and spaces at the beginning and the end are stripped.
        Additionally, the column's type is then is cast to categorical
        to limit memory usage.

        Returns:
            pandas.DataFrame: Standardized 'German-Zip-Codes'.
        """
        alphabetical_cols = ['Ort', 'Bundesland']
        german_chars = {'Ä': 'AE', 'Ö': 'OE', 'Ü': 'UE', 'ß': 'SS'}

        zip_df = self.original_zipcodes_df.copy()

        # Clean names
        for column_name in alphabetical_cols:
            column = zip_df[column_name]

            # Upper case
            column = column.str.upper()

            # Remove all non-alphabetical characters, preserve unicode alphabets
            column = column.str.replace(r'[\W_]', r' ', regex=True)

            # Replace German characters
            column = column.replace(german_chars, regex=True)

            # Remove redundant spaces
            # Replace double space
            column = column.str.replace(' +', ' ', n=-1, case=False, regex=True)

            # Remove space at the beginning and end of the name
            column = column.str.strip()

            zip_df[column_name] = column

        # Convert a column type to categorical to save memory
        zip_df['Bundesland'] = zip_df['Bundesland'].astype('category')

        return zip_df

    def _get_zip_mean_df(self):
        """Average ZIP codes and substitute the last three digits for 'x'.

        Standardized version of 'German-Zip-Codes' is grouped by
        the 'Ort' and 'Bundesland'. The 'Plz' values in groups are
        combined and averaged. Resulting NaN values are dropped.
        The last three digits of the resulting values are substituted for 'x'.

        Returns:
            pandas.DataFrame: Standardized 'German-Zip-Codes' with adjusted ZIP codes.
        """
        zip_df_copy = self.zip_df.copy()
        zip_df_copy['Plz'] = zip_df_copy['Plz'].astype(int)

        # Group the dataframe by cities and states and get ZIP code average in every group
        zip_mean_df = zip_df_copy.groupby(["Ort", "Bundesland"])['Plz'].mean().copy()

        # Drop NaN values resulting from the 'group by' operation
        zip_mean_df = zip_mean_df.dropna(axis=0).reset_index()

        # Rewrite Plz mean values to unknown last three digits
        zip_mean_df['Plz'] = zip_mean_df['Plz'].swifter.apply(lambda x: zip_fill(x, zip_mean=True))

        return zip_mean_df
