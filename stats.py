import click
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def basic_stats(df: pd.DataFrame) -> None:
    total = df['address'].shape[0]
    print(f'\n There are {total} total contributions')

    print(
        f'''
        Sign-in stats:
            {df['address'].dropna().shape[0]:>6} ({df['address'].dropna().shape[0]/total:>6.2%}) contributions used SIWE
            {total - df['address'].dropna().shape[0]:>6} ({1 - df['address'].dropna().shape[0]/total:>6.2%}) contributions used GH
        '''
        )
    print(
        f'''
        Account stats:
            Nonces   range from {df['nonce'].min():>3} to {df['nonce'].max():>7}.      The mean is {df['nonce'].mean():>5.2f}
            Balances range from {df['balance'].min()/1e18:>3} to {df['balance'].max()/1e18:>7.2f}.      The mean is {df['balance'].mean()/1e18:>5.2f}
        '''
    )
    print(
        f'''
        Bots stats:
            {df[df['bots'] != ''].shape[0]:>6} ({df[df['bots'] != ''].shape[0]/total:>6.2%}) are possibly bots
            {df[df['bots'] == 'one'].shape[0]:>6} ({df[df['bots'] == 'one'].shape[0]/total:>6.2%}) are "ones" bots
            {df[df['bots'] == 'no bls sig'].shape[0]:>6} ({df[df['bots'] == 'no bls sig'].shape[0]/total:>6.2%}) have no BLS signature
            {df[df['bots'] == 'no ecdsa sig'].shape[0]:>6} ({df[df['bots'] == 'no ecdsa sig'].shape[0]/df[df['address'] != ''].shape[0]:>6.2%}) have no ECDSA signature
        '''
    )

def plot_ecdf(df: pd.DataFrame, field: str='nonce') -> None:
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


def print_zero_nonce(df: pd.DataFrame) -> None:
    print(df[df['nonce'] == 0])

def violin_plot(df: pd.DataFrame, field: str='nonce') -> None:
    return
    # Plot the violin plot
    plt.violinplot([df[field].dropna().values], showmeans=True, showmedians=True)

    plt.xticks(range(1, len(categories) + 1), categories)
    plt.xlabel('Category')
    plt.ylabel('Value')
    plt.show()

@click.command()
@click.option('--participants_path', default='participants.pkl', show_default=True)
@click.option('--nonce_cdf', is_flag=True, default=True, show_default=True)
@click.option('--balance_cdf', is_flag=True, default=True, show_default=True)
def calculate_stats(participants_path: str, nonce_cdf: bool, balance_cdf: bool) -> None:
    participants_df = pd.read_pickle(participants_path)
    basic_stats(participants_df)
    print_zero_nonce(participants_df)
    # if nonce_cdf:
    #     violin_plot(participants_df, 'nonce')
    # if balance_cdf:
    #     plot_ecdf(participants_df, 'balance')
