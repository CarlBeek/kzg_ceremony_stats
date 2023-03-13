import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def plot_cumulative_field(df: pd.DataFrame, field: str='nonce') -> None:
    # Sort the 'B' values and compute the empirical CDF
    sorted_field = np.sort(df[field].dropna())
    ecdf = np.arange(1, len(sorted_field)+1) / len(sorted_field)

    # Plot the ECDF as a step function
    plt.step(sorted_field, ecdf, where='post')

    # Set the chart title and axis labels
    plt.title('Empirical CDF of {:}'.format(field))
    plt.xscale('log')
    plt.xlabel('log({:})'.format(field))
    plt.ylabel('Cumulative Distribution')
    plt.show()

