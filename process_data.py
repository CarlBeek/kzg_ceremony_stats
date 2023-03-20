import click
import json
from tqdm import tqdm
import web3
import pandas as pd
import os
import requests
from typing import Optional

from process_ens import update_missing_ens

TRANSCRIPT_URL = 'https://seq.ceremony.ethereum.org/info/current_state'
CUT_OFF_BLOCK = 16394156


def download_new_transcript(transcript_path: str) -> None:
    print('Downloading transcript.json...')
    response = requests.get(TRANSCRIPT_URL)
    json_data = json.loads(response.text)
    with open(transcript_path, 'w') as f:
        json.dump(json_data, f)

def maybe_get_address(w3: web3.Web3, pid: str) -> Optional[str]:
    if not pid.startswith('eth'):
        return None
    return w3.toChecksumAddress(pid[4:])


def transcript_to_df(transcript: dict) -> pd.DataFrame:
    participants = []
    for i in range(1, len(transcript['participantIds'])):
        pid = transcript['participantIds'][i]
        pot_pk12 = transcript['transcripts'][0]['witness']['potPubkeys'][i]
        pot_pk13 = transcript['transcripts'][1]['witness']['potPubkeys'][i]
        pot_pk14 = transcript['transcripts'][2]['witness']['potPubkeys'][i]
        pot_pk15 = transcript['transcripts'][3]['witness']['potPubkeys'][i]
        bls_sig12 = transcript['transcripts'][0]['witness']['blsSignatures'][i]
        bls_sig13 = transcript['transcripts'][1]['witness']['blsSignatures'][i]
        bls_sig14 = transcript['transcripts'][2]['witness']['blsSignatures'][i]
        bls_sig15 = transcript['transcripts'][3]['witness']['blsSignatures'][i]
        ecdsa_sig = transcript['participantEcdsaSignatures'][i]
        participant = {
            'participantId': pid,
            'pot_pk12': pot_pk12,
            'pot_pk13': pot_pk13,
            'pot_pk14': pot_pk14,
            'pot_pk15': pot_pk15,
            'bls_sig12': bls_sig12,
            'bls_sig13': bls_sig13,
            'bls_sig14': bls_sig14,
            'bls_sig15': bls_sig15,
            'ecdsa_sig': ecdsa_sig,
        }
        participants.append(participant)
    return pd.DataFrame(participants)

def insert_new_participants(w3: web3.Web3, participants_df: pd.DataFrame, transcript_df: pd.DataFrame) -> pd.DataFrame:
    participants_df = pd.concat([participants_df, transcript_df], ignore_index=True)
    participants_df.drop_duplicates(subset=['participantId'], keep='first', inplace=True)

    participants_df['address'] = participants_df['participantId'].apply(lambda pid: maybe_get_address(w3, pid))
    return participants_df


def update_missing_balance(w3: web3.Web3, participants_df: pd.DataFrame, save_path: str, save_int: int, block: int) -> pd.DataFrame:
    if 'balance' not in participants_df:
        participants_df['balance'] = None
    missing_data_df = participants_df[participants_df['balance'].isna()]
    missing_data_df = missing_data_df[missing_data_df['address'].notna()]
    num_missing = missing_data_df.shape[0]
    print(f'{num_missing} balances missing')
    for i, row in tqdm(missing_data_df.iterrows(), total=num_missing):
        address = row['address']
        participants_df.loc[i, 'balance'] = w3.eth.get_balance(address, block)
        if i % save_int == 0:
            participants_df.to_pickle(save_path)
    participants_df.to_pickle(save_path)
    return participants_df


def update_missing_nonce(w3: web3.Web3, participants_df: pd.DataFrame, save_path: str, save_int: int, block: int) -> pd.DataFrame:
    if 'nonce' not in participants_df:
        participants_df['nonce'] = None
    missing_data_df = participants_df[participants_df['nonce'].isna()]
    missing_data_df = missing_data_df[missing_data_df['address'].notna()]
    num_missing = missing_data_df.shape[0]
    print(f'{num_missing} nonces missing')
    for i, row in tqdm(missing_data_df.iterrows(), total=num_missing):
        address = row['address']
        participants_df.loc[i, 'nonce'] = w3.eth.get_transaction_count(address, block)
        if i % save_int == 0:
            participants_df.to_pickle(save_path)
            participants_df.head()
    participants_df.to_pickle(save_path)
    return participants_df


def patch_missing_df_data(w3: web3.Web3,participants_df: pd.DataFrame, save_path: str, save_int: int=128, block: int=CUT_OFF_BLOCK) -> pd.DataFrame:
    participants_df = update_missing_balance(w3, participants_df, save_path, save_int, block)
    participants_df = update_missing_nonce(w3, participants_df, save_path, save_int, block)
    participants_df = update_missing_ens(w3, participants_df, save_path, save_int, block)
    return participants_df


@click.command()
@click.option('--fetch_new_transcript', is_flag=True, default=False, show_default=True, help='Flag to download the latest transcript')
@click.option('--patch_missing_data', is_flag=True, default=False, show_default=True, help='Flag to fetch missing data from RPC endpoint')
@click.option('--transcript_path', default='transcript.json', show_default=True)
@click.option('--participants_path', default='participants.pkl', show_default=True)
@click.option('--web3_provider', default='https://rpc.ankr.com/eth', show_default=True)
def load_new_data(
        fetch_new_transcript: bool,
        patch_missing_data: bool,
        transcript_path: str,
        participants_path: str,
        web3_provider: str,
        ):
    provider = web3.providers.HTTPProvider(web3_provider)
    w3 = web3.Web3(provider)

    participants_df = pd.DataFrame()
    if os.path.isfile(participants_path):
        participants_df = pd.read_pickle(participants_path)
    
    if fetch_new_transcript:
        response = requests.get(TRANSCRIPT_URL)
        json_data = json.loads(response.text)
        with open(transcript_path, 'w') as f:
            json.dump(json_data, f)


    with open(transcript_path) as json_file:
        transcript = json.load(json_file)
        transcript_df = transcript_to_df(transcript)

    participants_df = insert_new_participants(w3, participants_df, transcript_df)

    if patch_missing_data:
        participants_df = patch_missing_df_data(w3, participants_df, participants_path)
    participants_df.drop('bots', axis=1)
    participants_df.to_pickle(participants_path)
