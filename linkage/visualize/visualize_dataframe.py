""" Visualize data in dataframe """

from linkage.visualize.plot import plot_stacked_barchart


def show_nan_counts(df, labels, xmin=None, xmax=None, ymin=None, ymax=None):
    """Count NaN values and call a method to plot them.

    Args:
        df (pandas.DataFrame): A dataframe to retrieve values from.
        labels (list): A list of labels for individual bars.
        xmin (int): A minimal value on the x-axis to be shown.
        xmax (int): A maximal value on the x-axis to be shown.
        ymin (int): A minimal value on the y-axis to be shown.
        ymax (int): A maximal value on the y-axis to be shown.
    """
    not_nan_counts = df.notnull().sum()
    nan_counts = df.isnull().sum()

    plot_stacked_barchart(not_nan_counts, nan_counts, title='Num. of not NaN and NaN values per column.',
                          ylabel='Num. of records', labels=labels, bottom_label='Not NaN', up_label='NaN',
                          xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax)
