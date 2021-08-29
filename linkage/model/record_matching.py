""" Record Linkage methods """

import recordlinkage
import pandas as pd

from recordlinkage.compare import Exact
from linkage.model.record_linkage_utils import CompareZipCodes, CompareString


class Linking:
    """Perform record linkage.
    """

    def __init__(self, jp_index, jp_company_name, jp_company_name_stand, jp_company_name_dict_clean,
                 jp_company_city, jp_company_zip, jp_company_state,
                 jp_job_city, jp_job_zip, jp_job_state,
                 orbis_index, orbis_company_name, orbis_company_name_stand, orbis_company_name_dict_clean,
                 orbis_company_city, orbis_company_zip, orbis_company_state):
        self.jp_index = jp_index
        self.jp_company_name = jp_company_name
        self.jp_company_name_stand = jp_company_name_stand
        self.jp_company_name_dict_clean = jp_company_name_dict_clean
        self.jp_company_city = jp_company_city
        self.jp_company_zip = jp_company_zip
        self.jp_company_state = jp_company_state
        self.jp_job_city = jp_job_city
        self.jp_job_zip = jp_job_zip
        self.jp_job_state = jp_job_state
        self.orbis_index = orbis_index
        self.orbis_company_name = orbis_company_name
        self.orbis_company_name_stand = orbis_company_name_stand
        self.orbis_company_name_dict_clean = orbis_company_name_dict_clean
        self.orbis_company_city = orbis_company_city
        self.orbis_company_zip = orbis_company_zip
        self.orbis_company_state = orbis_company_state

    def blocking(self, df_1, df_2, col1, col2=None):
        """Add a block index and make an index of record pairs.

        Args:
            df_1 (pandas.DataFrame): A left dataframe to add block index to.
            df_2 (pandas.DataFrame): A right dataframe to add block index to.
            col1 (str): A column of 'df_1' to add block index to.
            col2 (str): A column of 'df_2' to add block index to.

        Returns:
            pandas.MultiIndex: Record pairs. Each record pair contains the index labels of two records.
        """
        # Create index
        indexer = recordlinkage.Index()

        if col2 is None:
            indexer.block(col1)  # exact match on the specified column
        else:
            indexer.block(col1, col2)  # exact match on specified columns

        # Make record pairs
        candidate_links = indexer.index(df_1, df_2)

        print(f'Num of candidates: {len(candidate_links)}\n')

        return candidate_links

    def compare_similar_records(self, df_1, df_2, candidate_links, addr_type='company'):
        """Compare the attributes of candidate record pairs.

        Args:
            df_1 (pandas.DataFrame): A left dataframe to compare the attributes of.
            df_2 (pandas.DataFrame): A right dataframe to compare the attributes of.
            candidate_links (pandas.MultiIndex): Record pairs.
            addr_type (str): Type of the JobPostings address. Either 'company' or 'job'.

        Returns:
            pandas.DataFrame: Feature vectors, i.e. the result of comparing each record pair.
        """

        method = 'jarowinkler'
        method_short = 'jarowinkler'

        compare_names = recordlinkage.Compare()

        compare_names.add(CompareString(self.jp_company_name_stand, self.orbis_company_name_stand,
                                        threshold=0.95, threshold_short=0.98,
                                        method=method, method_short=method_short,
                                        label='company_name_similar'))
        compare_names.add(CompareString(self.jp_company_name, self.orbis_company_name_stand,
                                        threshold=0.95, threshold_short=0.98,
                                        method=method, method_short=method_short,
                                        label='company-copy_name_similar'))
        compare_names.add(CompareString(self.jp_company_name_stand, self.orbis_company_name,
                                        threshold=0.95, threshold_short=0.98,
                                        method=method, method_short=method_short,
                                        label='company_name-copy_similar'))
        compare_names.add(CompareString(self.jp_company_name, self.orbis_company_name,
                                        threshold=0.95, threshold_short=0.98,
                                        method=method, method_short=method_short,
                                        label='company-copy_name-copy_copy_similar'))
        compare_names.add(CompareZipCodes(f'{addr_type}_zipcode', self.orbis_company_zip,
                                          label='company_zipcode_similar'))
        compare_names.add(CompareString(f'{addr_type}_city', self.orbis_company_city,
                                        threshold=0.95, threshold_short=0.98,
                                        method=method, method_short=method_short,
                                        label='company_city_similar'))
        compare_names.add(Exact(f'{addr_type}_state', self.orbis_company_state,
                                        label='company_state_exact'))

        features_name = compare_names.compute(candidate_links, df_1, df_2)

        # Sum the comparison results.
        print(f"Comparison results:\n{features_name.sum(axis=1).value_counts().sort_index(ascending=False)}\n")

        return features_name

    def get_best_match(self, potential_matches_name, index_1, index_2):
        """Get the best matches from the features vectors.

        Group the potential matches by the 'index_1' and keep
        the maximum.

        Args:
            potential_matches_name (pandas.DataFrame): Feature vectors, i.e. the result of comparing each record pair.
            index_1 (str): An index of the left dataframe, e.g. the JobPostings index.
            index_2 (str): An index of the right dataframe, e.g. the Orbis index.

        Returns:
            pandas.DataFrame: Feature vectors with the highest results.
        """
        # Get the sum of all achieved points
        potential_matches_sum = potential_matches_name.sum(axis=1)

        # Get the highest achieved points
        potential_matches_max = potential_matches_sum.groupby(level=0).apply(max).reset_index()
        potential_matches_max.columns = [index_1, 'sum']  # Rename the columns for joining

        # Adjust the dataframe with the sums
        potential_matches_sum = potential_matches_sum.reset_index()
        potential_matches_sum.columns = [index_1, index_2, 'sum']  # Rename the columns for joining

        # Inner join the dataframe with the sums and the highest sum
        # To get both JP_INDEX and 'orbis_index' in one dataframe
        potential_matches = pd.merge(potential_matches_sum, potential_matches_max, how='inner',
                                     left_on=[index_1, 'sum'], right_on=[index_1, 'sum'])

        # Reset index for subsequent joining
        potential_matches_name = potential_matches.reset_index()

        return potential_matches_name

    def merge_dataframes_on_linkage_result(self, features_name, df_1, df_2, addr_type='company', score_threshold=2.5):
        """Merge feature vectors with dataframes.

        Get a sum of the obtained score of the potential matches.
        Filter the potential matches by the sum of the score and
        keep only the highest scores for each JobPostings index.
        Left join the feature vectors with the dataframe 'df_1'
        and with the dataframe 'df_2' to retrieve the values
        of their attributes.

        Args:
            features_name (pandas.DataFrame): Feature vectors, i.e. the result of comparing each record pair.
            df_1 (pandas.DataFrame):A left dataframe to join with the feature vectors.
            df_2 (pandas.DataFrame):A right dataframe to join with the feature vectors.
            addr_type (str): Type of the JobPostings address. Either 'company' or 'job'.
            score_threshold (int): Everything below this threshold is rejected as match.

        Returns:
            pandas.DataFrame: A resulting dataframe of the merging.
        """
        # Get a sum of a score of the potential matches
        addr_features_sum = features_name[
            ['company_zipcode_similar', 'company_city_similar', 'company_state_exact']].sum(
            axis=1)

        # Filter the potential matches by the score
        potential_matches_name = features_name[((addr_features_sum >= score_threshold)
                                                & ((features_name['company_name_similar'] == 1)
                                                   | (features_name['company-copy_name_similar'] == 1)
                                                   | (features_name['company_name-copy_similar'] == 1)
                                                   | (features_name['company-copy_name-copy_copy_similar'] == 1)
                                                   ))]

        print(f"Num. of potential matches by name: {len(potential_matches_name)}")

        # Filter the best matches
        potential_matches_name = self.get_best_match(potential_matches_name, self.jp_index, 'orbis_index')

        # Join with the JobPostings and Orbis dataset to retrieve the values
        df_merge_name = potential_matches_name.merge(df_1, how='left', left_on=self.jp_index, right_on=self.jp_index)
        df_merge_name = df_merge_name.merge(df_2, how='left', left_on='orbis_index', right_on='orbis_index')

        df_merge_name = df_merge_name.drop_duplicates([self.jp_index, self.orbis_index])

        print(f"Num. of best matches by name: {len(df_merge_name)}")

        # Get only useful columns
        df_merge_name_result = df_merge_name[[self.jp_index, self.orbis_index,
                                              self.jp_company_name_stand, self.orbis_company_name_stand,
                                              self.jp_company_name, self.orbis_company_name,
                                              f'{addr_type}_city', self.orbis_company_city,
                                              f'{addr_type}_zipcode', self.orbis_company_zip,
                                              f'{addr_type}_state', self.orbis_company_state]].copy()

        return df_merge_name_result

    def process_matched(self, not_matched_df, matched_df, df_merge_name_result, count_column):
        """Process matched records.

        Add results of the matching to the 'matched_df' dataframe,
        remove already matched records from the 'not_matched_df', and
        print counts of matched and not-matched records.
        Additionally, delete 'df_merge_name_result' dataframe.

        Args:
            not_matched_df (pandas.DataFrame): A dataframe with not yet matched records.
            matched_df (pandas.DataFrame): A dataframe to store all results of matching.
            df_merge_name_result (pandas.DataFrame): A resulting dataframe of matching.
            count_column (str): Name of the column to count values in.
        """
        # Add matches to a new df
        result = df_merge_name_result.set_index([self.jp_index, self.orbis_index]).copy()
        matched_df = pd.concat([matched_df, result])

        # Remove matches from old JobPostings dataframe
        not_matched_df.drop(df_merge_name_result[self.jp_index], axis=0, inplace=True)

        # Print counts of matched and not-matched records
        print_matched_counts(matched_df, count_column)
        print_unmatched_counts(not_matched_df, count_column)

        del df_merge_name_result


def print_matched_counts(matched_df, col_name):
    """Print number of matched records formatted.

    Args:
        matched_df (pandas.DataFrame): A dataframe to count values in.
        col_name (str): Name of the column to count values in.
    """
    print(f"Num. of all matched records:                {len(matched_df)}")
    print(
        f"Num. of all matched records deduplicated:   {len(matched_df.pivot_table(index=[col_name], aggfunc='size'))}")


def print_unmatched_counts(not_matched_df, col_name):
    """Print number of not matched records formatted.

    Args:
        not_matched_df (pandas.DataFrame): A dataframe to count values in.
        col_name (str): Name of the column to count values in.
    """
    print(f"Num. of not matched records:                {len(not_matched_df)}")
    print(
        f"Num. of not matched records deduplicated:   {len(not_matched_df.pivot_table(index=[col_name], aggfunc='size'))}")
