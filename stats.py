import click
import os

import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio

from typing import Sequence

G2 = '0x93e02b6052719f607dacd3a088274f65596bd0d09920b61ab5da61bbdc7f5049334cf11213945d57e5ac7d055d042b7e024aa2b2f08f0a91260805272dc51051c6e47ad4fa403b02b4510b647ae3d1770bac0326a805bbefd48056c8c121bdb8'


def insert_bot_info(participants_df: pd.DataFrame) -> pd.DataFrame:
    def catagorise_bot(row) -> str:
        # Mark "Ones" bots
        if row['pot_pk12'] == G2:
            return 'one'
        # Mark unsigned BLS contributions
        elif row['bls_sig12'] == '':
            return 'no bls sig'
        return ''
    def is_bot(bot_type: str) -> bool:
        return bot_type != ''
    participants_df['bot_type'] = participants_df.apply(catagorise_bot, axis=1)
    participants_df['is_bot'] = is_bot(participants_df['bot_type'])
    return participants_df


def insert_eth_bal(participants_df: pd.DataFrame) -> pd.DataFrame:
    participants_df['eth_balance'] = participants_df['balance'] * 1e-18
    return participants_df


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
            Balances range from {df['eth_balance'].min():>3} to {df['eth_balance'].max():>7.2f}.      The mean is {df['eth_balance'].mean():>5.2f}
        '''
    )
    print(
        f'''
        Bots stats:
            {df[df['is_bot']].shape[0]:>6} ({df[df['is_bot']].shape[0]/total:>6.2%}) are possibly bots
            {df[df['bot_type'] == 'one'].shape[0]:>6} ({df[df['bot_type'] == 'one'].shape[0]/total:>6.2%}) are "ones" bots
            {df[df['bot_type'] == 'no bls sig'].shape[0]:>6} ({df[df['bot_type'] == 'no bls sig'].shape[0]/total:>6.2%}) have no BLS signature
        '''
    )


def plot_ecdf(df: pd.DataFrame, field: str='nonce') -> None:
    def ecdf(data):
        data = data.dropna()
        x = - np.sort(-data)
        n = len(data)
        y = np.arange(1, n+1) / n
        return x, y

    x_bot, y_bot = ecdf(df[df['is_bot']][field])
    x_notbot, y_notbot = ecdf(df[~df['is_bot']][field])

    fig = go.Figure(go.Scatter(x=x_bot, y=y_bot, mode='lines', name='Bot'))
    fig.add_trace(go.Scatter(x=x_notbot, y=y_notbot, mode='lines', name='Not Bot'))

    fig.update_layout(
        hovermode='x unified',
        title=f'ECDF of {field}',
        xaxis_title=f'{field}',
        yaxis_title='ECDF'
    )
    fig.update_xaxes(type='log')

    if not os.path.exists('plots'):
        os.makedirs('plots')
    path = f'plots/{field}_ecdf.html'
    fig.write_html(path)
    print(f'{field} ECDF output to {path}')

def output_low_nonces(df: pd.DataFrame) -> None:
    df[df['nonce'] < 3].to_csv('low_nonce.csv')

def print_ecdf_nonce_thresholds(df: pd.DataFrame, thresholds: list) -> None:
    bots_df = df[df['is_bot']]
    normal_df = df[~df['is_bot']]
    print('\n        Participants that meet nonce threshold:')
    print('        [threshold]:    [normal]    [bots]')
    for threshold in thresholds:
        print(f'            {threshold:>6}:       {(normal_df[normal_df["nonce"] > threshold].shape[0]/normal_df.shape[0]):>6.2%}    {(bots_df[bots_df["nonce"] > threshold].shape[0]/bots_df.shape[0]):>6.2%}')

def output_poap_list(df: pd.DataFrame) -> None:
    # Filter out GH contributions
    df = df[df['address'].notna()]
    # Filter out "ones_bots"
    df = df[df['bot_type'] != 'one']
    poap_addresses = df['address']
    poap_addresses.to_csv('poap_addresses.txt', index=False, header=False)

@click.command()
@click.option('--participants_path', default='participants.pkl', show_default=True)
@click.option('--nonce_cdf', is_flag=True, default=True, show_default=True)
@click.option('--balance_cdf', is_flag=True, default=True, show_default=True)
@click.option('--nonce_thresholds', '-t', multiple=True, type=click.INT)
@click.option('--generate_poap_list', is_flag=True, default=True, show_default=True)
def calculate_stats(
                    participants_path: str,
                    nonce_cdf: bool,
                    balance_cdf:bool,
                    nonce_thresholds: Sequence[int],
                    generate_poap_list: bool
                    ) -> None:
    participants_df = pd.read_pickle(participants_path)
    participants_df = insert_bot_info(participants_df)
    participants_df = insert_eth_bal(participants_df)
    output_low_nonces(participants_df)
    basic_stats(participants_df)
    if nonce_thresholds is not None:
        print_ecdf_nonce_thresholds(participants_df, nonce_thresholds)
    if nonce_cdf:
        plot_ecdf(participants_df, 'nonce')
    if balance_cdf:
        plot_ecdf(participants_df, 'eth_balance')
    if generate_poap_list:
        output_poap_list(participants_df)
