""" Fill missing values of address """
import re
from linkage.model.utils import most_common, isna, zip_fill


class FillAddress:
    """Gather methods to fill missing address values.

    Attributes:
        bundesland_lst (list): All German states, with and without umlaut.
        zip_df (pandas.DataFrame): Contains standardized version of 'German-Zip-Codes.csv'.
        zip_mean_df (pandas.DataFrame): Zipcodes are averaged and the last three digits are substituted for 'x'.
        zipcode_column_name (str): Name of the column storing ZIP codes.
        city_column_name (str): Name of the column storing cities.
        state_column_name (str): Name of the column soring states.
    """

    bundesland_lst = [
        'SCHLESWIG HOLSTEIN',
        'HAMBURG',
        'MECKLENBURG VORPOMMERN',
        'BREMEN',
        'BRANDENBURG',
        'BERLIN',
        'NIEDERSACHSEN',
        'SACHSEN ANHALT',
        'SACHSEN',
        'THÜRINGEN', 'THUERINGEN',
        'HESSEN',
        'NORDRHEIN WESTFALEN',
        'RHEINLAND PFALZ',
        'SAARLAND',
        'BADEN WÜRTTEMBERG', 'BADEN WUERTTEMBERG',
        'BAYERN'
    ]

    def __init__(self, df, zip_df, zip_mean_df, zipcode_column_name, city_column_name, state_column_name):
        self.zip_df = zip_df
        self.zip_mean_df = zip_mean_df
        self.zipcode_column_name = zipcode_column_name
        self.city_column_name = city_column_name
        self.state_column_name = state_column_name

    def _fill_missing_zipcode(self, row):
        """Fill missing ZIP code with respect to other fields.

        Apply when the ZIP code value is empty or
        contains non-numerical characters.
        If the column value storing city is not empty,
        retrieve the ZIP code. If there is only one match,
        use the ZIP code, otherwise use the state value
        and retrieve mean ZIP code. If there is more
        than one match, use the most common value.

        Args:
            row: Row of a dataframe to alter.

        Returns:
            Return altered row.
        """
        city_name = row[self.city_column_name]
        zipcode = str(row[self.zipcode_column_name])
        row[self.zipcode_column_name] = None

        if not isna(zipcode) and re.search('[^0-9]+', str(zipcode)):  # Special format of ZIP codes, e.g. D9 rewrite to 9xxxx
            zipcode = re.sub('[^0-9]', '', str(zipcode))

            is_correct = self.zip_df.loc[lambda df: df['Plz'] == zipcode, :].size > 0

            if is_correct:  # If the ZIP code is matching any record, keep it
                row[self.zipcode_column_name] = zipcode
                return row

            elif len(zipcode) > 0:  # Otherwise rewrite it using 'x'
                zipcode = zipcode + ((5 - len(zipcode)) * 'x')
                row[self.zipcode_column_name] = zipcode
                return row

        zip_value_loc = self.zip_mean_df.loc[lambda df: df['Ort'] == city_name, :]
        zip_value = zip_value_loc['Plz'].values

        if zip_value.size == 1:  # Exactly one match on city
            row[self.zipcode_column_name] = zip_value[0]
        elif zip_value.size > 1:  # Choose the one with matching Bundesland
            zip_and_state_value_loc = self.zip_mean_df.loc[lambda df: df['Bundesland'] == row[self.state_column_name],
                                      :]
            zip_and_state_value = zip_and_state_value_loc['Plz'].values

            if zip_and_state_value.size == 1:
                row[self.zipcode_column_name] = zip_and_state_value[0]
            elif zip_and_state_value.size > 1:  # The most common value
                row[self.zipcode_column_name] = most_common(zip_and_state_value)
            elif zip_and_state_value.size == 0:
                row[self.zipcode_column_name] = most_common(zip_value)
        return row

    def _fill_missing_city(self, row):
        """Fill missing city with respect to other fields.

        Apply when the city value is empty or contains any
        numerical characters. If the ZIP code is in non-proper
        format, adjust the ZIP code. Then, if the zipcode
        is correct (not missing, not containing 'x', and it is
        located in 'zip_df'), retrieve the city name. Otherwise
        correct the ZIP code. If multiple city names were found,
        retrieve the most common.

        Args:
            row: Row of a dataframe to alter.

        Returns:
            Return altered row.
        """
        zipcode = str(row[self.zipcode_column_name])
        city = row[self.city_column_name]

        if not isna(city) and re.search('[^a-zA-Z ]+', str(city)):  # City contains redundant characters
            city = re.sub('[^a-zA-Z ]', '', city)

            is_correct = self.zip_df.loc[lambda df: df['Ort'] == city, :].size > 0
            if is_correct:  # If the city is matching any record, keep it
                row[self.city_column_name] = zipcode
                return row #[self.city_column_name]

        city_value = None

        if (not isna(zipcode)) and ('x' not in zipcode):  # Full zipcode is available
            zip_loc = self.zip_df.loc[lambda df: (df['Plz'] == zipcode)]

            if zip_loc.size > 0:  # Zipcode is correct
                city_value_loc = self.zip_df.loc[lambda df: df['Plz'] == str(zipcode), :]

            else:
                if len(zipcode) == 5:
                    num_to_preserve = 2
                else:
                    num_to_preserve = 0
                zipcode = zip_fill(zipcode, num_to_preserve)
                row[self.zipcode_column_name] = zipcode
                city_value_loc = self.zip_df.loc[lambda df: df['Plz'] == str(zipcode), :]

            city_value = city_value_loc['Ort'].values

        if city_value is not None and city_value.size > 0:
            row[self.city_column_name] = most_common(city_value)

        return row #[self.city_column_name]

    def _fill_missing_state(self, row):
        """Fill missing state with respect to other fields.

        Apply when the state value is empty or contains any
        numerical characters. If the ZIP code is numerical only,
        retrieve the state name from 'zip_df', otherwise
        use 'zip_mean_df'. If the state value was not found,
        use the city name. If multiple values were found,
        use the most common one.

        Args:
            row: Row of a dataframe to alter.

        Returns:
            Return altered row.
        """
        zipcode = row[self.zipcode_column_name]
        city = row[self.city_column_name]

        if not isna(city) and not isna(zipcode):  # City and zipcode are available

            if 'x' not in str(zipcode):  # Full zipcode
                state_value_loc = self.zip_df.loc[lambda df: (df['Plz'] == zipcode) & (df['Ort'] == city), :]

            else:  # Partial zipcode
                zipcode = zipcode[:2]
                state_value_loc = self.zip_mean_df.loc[
                                  lambda df: (df['Plz'].astype('str').str.contains(f'^{zipcode}', regex=True)) & (
                                          df['Ort'] == city), :]

            if state_value_loc.size == 0:  # No match
                state_value_loc = self.zip_df.loc[lambda df: df['Ort'] == city, :]  # Match on city only

                if state_value_loc['Bundesland'].values.size == 1:  # One match on city
                    row[self.zipcode_column_name] = state_value_loc['Plz'].values[0]

                elif state_value_loc['Bundesland'].values.size > 1:  # Multiple matches on city
                    state = most_common(
                        state_value_loc['Bundesland'])  # Take most common state (higher chance of correctness)
                    zipcode_value_loc = self.zip_mean_df.loc[
                                        lambda df: (df['Bundesland'] == state) & (df['Ort'] == city), :]
                    row[self.zipcode_column_name] = zipcode_value_loc['Plz'].values[0]

                elif state_value_loc['Bundesland'].values.size == 0:  # No matches on city

                    if 'x' not in str(zipcode):  # Full zipcode
                        state_value_loc = self.zip_df.loc[lambda df: df['Plz'] == str(zipcode), :]

                    else:  # Partial zipcode
                        zipcode = zipcode[:2]
                        state_value_loc = self.zip_mean_df.loc[
                                          lambda df: df['Plz'].astype('str').str.contains(f'^{zipcode}', regex=True), :]

                    if state_value_loc['Bundesland'].values.size > 0:  # Matches on zipcode
                        row[self.city_column_name] = state_value_loc['Ort'].values[0]
                    else:
                        pass

        elif not isna(city):  # City is available
            state_value_loc = self.zip_df.loc[lambda df: df['Ort'] == city, :]

        elif not isna(zipcode):  # Zipcode is available
            if 'x' not in zipcode:  # Full zipcode
                state_value_loc = self.zip_df.loc[lambda df: df['Plz'] == str(zipcode), :]

            else:  # Partial zipcode
                state_value_loc = self.zip_df.loc[
                                  lambda df: df['Plz'].astype(str).str.contains(fr'^\b{zipcode}\b', regex=True), :]

        state_value = state_value_loc['Bundesland'].values

        # If not empty, choose the most common value
        if state_value.size > 0:
            row[self.state_column_name] = most_common(state_value)

        return row[self.state_column_name]

    def _fill_unclear_state(self, row):
        """Splits value of 'Region in country' and search for a state.

        Splits 'Region in country' by '|' and search for
        a name of a German state. If the name of a state
        contains umlaut, replace it with the Latin version.

        Args:
            row: Row of a dataframe to alter.
        """
        region_in_country = row[self.state_column_name]
        region_in_country = region_in_country.split('|')
        row[self.state_column_name] = None

        for region in region_in_country:
            # Search for a state name
            if region in self.bundesland_lst:
                # Replace umlaut with Latin version
                if 'Ü' in region:
                    region = region.replace('Ü', 'UE')
                row[self.state_column_name] = region

    def fill_missing_zipcode(self, df):
        """Apply '_fill_missing_zipcode' on a dataframe.

        Perform the operation on each row.

        Args:
            df (pandas.DataFrame): Dataframe to perform operation on.

        Returns:
            pandas.DataFrame: Modified dataframe with filled missing ZIP codes.
        """
        df.update(df.apply(self._fill_missing_zipcode, axis=1, result_type='broadcast'))
        return df

    def fill_missing_city(self, df):
        """Apply '_fill_missing_city' on a dataframe.

        Perform the operation on each row.

        Args:
            df (pandas.DataFrame): Dataframe to perform operation on.

        Returns:
            pandas.DataFrame: Modified dataframe with filled missing cities.
        """
        df.apply(self._fill_missing_city, axis=1)
        return df

    def fill_missing_state(self, df):
        """Apply '_fill_missing_state' on a dataframe.

        Perform the operation on each row.

        Args:
            df (pandas.DataFrame): Dataframe to perform operation on.

        Returns:
            pandas.DataFrame: Modified dataframe with filled missing states.
        """
        df.apply(self._fill_missing_state, axis=1)
        return df

    def fill_unclear_state(self, df):
        """Apply '_fill_unclear_state' on a dataframe.

        Perform the operation on each row.

        Args:
            df (pandas.DataFrame): Dataframe to perform operation on.

        Returns:
            pandas.DataFrame: Modified dataframe with filled unclear states.

        """
        df.apply(self._fill_unclear_state, axis=1)
        return df
