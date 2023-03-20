import click
import os

import numpy as np
import pandas as pd
import plotly.express as px


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
            {df[df['is_bot'] != ''].shape[0]:>6} ({df[df['is_bot'] != ''].shape[0]/total:>6.2%}) are possibly bots
            {df[df['bot_type'] == 'one'].shape[0]:>6} ({df[df['bot_type'] == 'one'].shape[0]/total:>6.2%}) are "ones" bots
            {df[df['bot_type'] == 'no bls sig'].shape[0]:>6} ({df[df['bot_type'] == 'no bls sig'].shape[0]/total:>6.2%}) have no BLS signature
        '''
    )


def plot_ecdf(df: pd.DataFrame, field: str='nonce') -> None:
    fig = px.ecdf(df, x=field, marginal='box', color='is_bot', log_x=True, ecdfmode='complementary')
    fig.update_traces(hovertemplate=None)
    fig.update_layout(title=f'ECDF of {field}', hovermode="x unified")

    if not os.path.exists('plots'):
        os.makedirs('plots')
    path = f'plots/{field}_ecdf.html'
    fig.write_html(path)
    print(f'{field} ECDF output to {path}')

def output_low_nonces(df: pd.DataFrame) -> None:
    df[df['nonce'] < 3].to_csv('low_nonce.csv')

@click.command()
@click.option('--participants_path', default='participants.pkl', show_default=True)
@click.option('--nonce_cdf', is_flag=True, default=True, show_default=True)
@click.option('--balance_cdf', is_flag=True, default=False, show_default=True)
def calculate_stats(participants_path: str, nonce_cdf: bool, balance_cdf: bool) -> None:
    participants_df = pd.read_pickle(participants_path)
    participants_df = insert_bot_info(participants_df)
    participants_df = insert_eth_bal(participants_df)
    output_low_nonces(participants_df)
    basic_stats(participants_df)
    if nonce_cdf:
        plot_ecdf(participants_df, 'nonce')
    if balance_cdf:
        plot_ecdf(participants_df, 'eth_balance')
