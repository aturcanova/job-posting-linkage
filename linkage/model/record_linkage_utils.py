""" User defined Record Linkage functions """

import pandas as pd

from recordlinkage.algorithms.string import cosine_similarity
from recordlinkage.algorithms.string import damerau_levenshtein_similarity
from recordlinkage.algorithms.string import jaro_similarity
from recordlinkage.algorithms.string import jarowinkler_similarity
from recordlinkage.algorithms.string import levenshtein_similarity
from recordlinkage.algorithms.string import longest_common_substring_similarity
from recordlinkage.algorithms.string import qgram_similarity
from recordlinkage.algorithms.string import smith_waterman_similarity

from recordlinkage.utils import fillna as _fillna
from recordlinkage.base import BaseCompareFeature


class CompareZipCodes(BaseCompareFeature):
    """ Compute the similarity between ZIP code values.

    This class is used to compare string values.
    """

    def _compute_vectorized(self, s1, s2):
        """Compute the (partial) similarity between ZIP code values.

        In case of agreement, the similarity is 1 and in case of
        complete disagreement it is 0. In case of partial agreement,
        the similarity is 0.5 for first two characters to match and
        0.25 for only one characters to match.

        Args:
            s1 (pandas.Series): Series on the left side.
            s2 (pandas.Series): Series on the right side.

        Returns:
            pandas.Series: Similarity of ZIP code values.
        """
        # Check if the zipcode are identical (return 1 or 0)
        sim = (s1 == s2).astype(float)

        # Check the first 2 numbers of the distinct comparisons
        sim[(sim == 0) & (s1.str[0:2] == s2.str[0:2])] = 0.5

        # Check the first number of the distinct comparisons
        sim[(sim == 0) & (s1.str[0:1] == s2.str[0:1])] = 0.25

        return sim


class CompareString(BaseCompareFeature):
    """Compute the (partial) similarity between strings values.

    This class is used to compare string values. The implemented algorithms
    are: 'jaro','jarowinkler', 'levenshtein', 'damerau_levenshtein', 'qgram'
    or 'cosine'. In case of agreement, the similarity is 1 and in case of
    complete disagreement it is 0. Series are divided by the length of their
    string values. If one of the values in left or right series is shorter
    or equal to 'len_short', different methods are used. The Python Record
    Linkage Toolkit uses the 'jellyfish' package for the Jaro, Jaro-Winkler,
    Levenshtein and Damerau-Levenshtein algorithms. This method is taken
    from the original Python Record Linkage Toolkit and adapted to the
    needs of the company names comparison.

    Attributes:
        left_on (str or int): The name or position of the column in the left DataFrame.
        right_on (str or int): The name or position of the column in the right DataFrame.
        method (str): An approximate string comparison method. Options are ['jaro',
            'jarowinkler', 'levenshtein', 'damerau_levenshtein', 'qgram',
            'cosine', 'smith_waterman', 'lcs']. Default: 'levenshtein'
        threshold (float or tuple): A threshold value.
            All approximate string comparisons higher or equal than this threshold are 1. Otherwise 0.
        threshold_short (float): A threshold value for strings shorter or equal to 'len_short'.
            All approximate string comparisons higher or equal than this threshold are 1. Otherwise 0.
        len_short (int): Threshold for short strings.
            For strings shorter or equal to 'len_short' values of 'threshold_short' and 'method_short' will apply.
        method_short (str): An approximate string comparison method for strings shorter or equal to 'len_short'.
        missing_value (numpy.dtype): The value for a comparison with a missing value. Default 0.
    """

    name = "stringCompare"
    description = "Compare string attributes of record pairs."

    def __init__(self,
                 left_on,
                 right_on,
                 method='levenshtein',
                 threshold=None,
                 threshold_short=None,
                 len_short=7,
                 method_short='levenshtein',
                 missing_value=0.0,
                 label=None):
        super(CompareString, self).__init__(left_on, right_on, label=label)

        self.method = method
        self.threshold = threshold
        self.threshold_short = threshold_short
        self.len_short = len_short
        self.method_short = method_short
        self.missing_value = missing_value

    def _get_sim_alg(self, method):
        """Get the similarity comparison method.

        Args:
            method (str): An approximate string comparison method. Options are ['jaro',
                'jarowinkler', 'levenshtein', 'damerau_levenshtein', 'qgram',
                'cosine', 'smith_waterman', 'lcs'].

        Returns:
            Similarity comparison method.
        """

        if method == 'jaro':
            str_sim_alg = jaro_similarity
        elif method in ['jarowinkler', 'jaro_winkler', 'jw']:
            str_sim_alg = jarowinkler_similarity
        elif method == 'levenshtein':
            str_sim_alg = levenshtein_similarity
        elif method in ['dameraulevenshtein', 'damerau_levenshtein', 'dl']:
            str_sim_alg = damerau_levenshtein_similarity
        elif method in ['q_gram', 'qgram']:
            str_sim_alg = qgram_similarity
        elif method == 'cosine':
            str_sim_alg = cosine_similarity
        elif method in ['smith_waterman', 'smithwaterman', 'sw']:
            str_sim_alg = smith_waterman_similarity
        elif method in ['longest_common_substring', 'lcs']:
            str_sim_alg = longest_common_substring_similarity
        else:
            raise ValueError("The algorithm '{}' is not known.".format(
                self.method))
        return str_sim_alg

    def _compute_vectorized(self, s_left, s_right):
        """Compute the (partial) similarity between strings values.

        Series are divided by the length of their string values.
        If one of the values in left or right series is shorter
        or equal to 'len_short', different methods are used.
        In case of agreement, the similarity is 1 and in case of
        complete disagreement it is 0. The results are joined to
        a single series for both, long and short strings.

        Args:
            s_left (pandas.Series): Series on the left side.
            s_right (pandas.Series): Series on the right side.

        Returns:
            pandas.Series: Similarity of string values.
        """

        # Split the series by the length of the strings
        s_left_short = s_left.where((s_left.str.len() <= self.len_short)
                                    | (s_right.str.len() <= self.len_short)).copy()

        s_right_short = s_right.where((s_right.str.len() <= self.len_short)
                                      | (s_left.str.len() <= self.len_short)).copy()

        s_left_long = s_left.where((s_left.str.len() > self.len_short)
                                   | (s_right.str.len() > self.len_short)).copy()

        s_right_long = s_right.where((s_right.str.len() > self.len_short)
                                     | (s_left.str.len() > self.len_short)).copy()

        # Get comparison algorithms for the short and long strings
        str_sim_alg_long = self._get_sim_alg(self.method)
        str_sim_alg_short = self._get_sim_alg(self.method_short)

        # Apply the similarity algorithms
        c_long = str_sim_alg_long(s_left_long, s_right_long)
        c_short = str_sim_alg_short(s_left_short, s_right_short)

        # Evaluate the results of the similarity algorithms
        # Assign 1 to the values higher or equal to the threshold, otherwise 0
        if self.threshold is not None:
            c_long = c_long.where((c_long < self.threshold) | (pd.isnull(c_long)), other=1.0)
            c_long = c_long.where((c_long >= self.threshold) | (pd.isnull(c_long)), other=0.0)

        if self.threshold_short is not None:
            c_short = c_short.where((c_short < self.threshold_short) | (pd.isnull(c_short)), other=1.0)
            c_short = c_short.where((c_short >= self.threshold_short) | (pd.isnull(c_short)), other=0.0)

        # Join the results
        c_long.update(c_short)
        c_long = _fillna(c_long, self.missing_value)

        return c_long
