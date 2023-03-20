import click
import numpy as np
import pandas as pd
import plotly.express as px


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
    fig = px.ecdf(df, x=field, color="bots", marginal="box", log_x=True)
    fig.show()

def output_low_nonces(df: pd.DataFrame) -> None:
    df[df['nonce'] < 3].to_csv('low_nonce.csv')

@click.command()
@click.option('--participants_path', default='participants.pkl', show_default=True)
@click.option('--nonce_cdf', is_flag=True, default=True, show_default=True)
@click.option('--balance_cdf', is_flag=True, default=True, show_default=True)
def calculate_stats(participants_path: str, nonce_cdf: bool, balance_cdf: bool) -> None:
    participants_df = pd.read_pickle(participants_path)
    output_low_nonces(participants_df)
    basic_stats(participants_df)
    if nonce_cdf:
        plot_ecdf(participants_df, 'nonce')
    if balance_cdf:
        plot_ecdf(participants_df, 'balance')
