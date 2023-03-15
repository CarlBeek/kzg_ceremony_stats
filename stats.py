import click
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def plot_cumulative_field(df: pd.DataFrame, field: str='nonce') -> None:
    # Sort the `field`` values and compute the empirical CDF
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


@click.command()
@click.option('--participants_path', default='participants.pkl', show_default=True)
@click.option('--nonce_cdf', is_flag=True, default=True, show_default=True)
@click.option('--balance_cdf', is_flag=True, default=True, show_default=True)
def calculate_stats(participants_path: str, nonce_cdf: bool, balance_cdf: bool) -> None:
    participants_df = pd.read_pickle(participants_path)
    if nonce_cdf:
        plot_cumulative_field(participants_df, 'nonce')
    if balance_cdf:
        plot_cumulative_field(participants_df, 'balance')
