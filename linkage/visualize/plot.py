""" Plot from dataframes """

import matplotlib.pyplot as plt


def plot_histogram(df, title, ylabel, xlabel, column_name=None, num=40):
    """Plot a histogram showing values with highest appearance.

    Args:
        df (pandas.DataFrame): A dataframe to retrieve values from.
        title (str): A title of the plot.
        ylabel (str): A label of the y-axis.
        xlabel (str): A label of the x-axis.
        column_name (str): A column name to take values from.
        num (int): A number of visualized elements.
    """
    plt.figure(figsize=(15, 7))
    if column_name is not None:
        df[column_name].value_counts().nlargest(num).plot(kind='bar')
    else:
        df.nlargest(num).plot(kind='bar')
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)


def plot_stacked_barchart(bottom_counts, up_counts, title, ylabel, labels, bottom_label, up_label,
                          xmin=None, xmax=None, ymin=None, ymax=None):
    """Plot a barchart showing values stacked on the top of each other.

        Args:
            bottom_counts (list): Counts for the bottom bars.
            up_counts (list): Counts for the upper bars.
            title (str): A title of the plot.
            ylabel (str): A label of the y-axis.
            labels (list): A list of labels for individual bars.
            bottom_label (str): A label of the x-axis on the bottom.
            up_label (str): A label of the x-axis on the top.
            xmin (int): A minimal value on the x-axis to be shown.
            xmax (int): A maximal value on the x-axis to be shown.
            ymin (int): A minimal value on the y-axis to be shown.
            ymax (int): A maximal value on the y-axis to be shown.
        """
    width = 0.6
    plt.figure(figsize=(12, 6))
    plt.axis([xmin, xmax, ymin, ymax])
    plt.bar(labels, bottom_counts, width, color='b', label=bottom_label)
    plt.bar(labels, up_counts, width, bottom=bottom_counts, color='r', label=up_label)
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xticks(labels, rotation='vertical')
    plt.legend()
