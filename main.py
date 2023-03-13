import json
from tqdm import tqdm
import web3
import pandas as pd
import os
from typing import Optional

import stats


TRANSCRIPT_PATH = 'transcript.json'
PARTICIPANTS_DF_PATH = 'participants.pkl'
WEB3_PROVIDER = 'https://rpc.ankr.com/eth'
    

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


def update_missing_balance(w3: web3.Web3,participants_df: pd.DataFrame, save_path: str, save_int: int, block: str) -> pd.DataFrame:
    if 'balance' not in participants_df:
        participants_df['balance'] = None
    missing_data_df = participants_df[participants_df['balance'].isna()]
    missing_data_df = missing_data_df[missing_data_df['address'].notna()]
    print('Patching the {:} missing balances'.format(missing_data_df.shape[0]))
    for i, row in tqdm(missing_data_df.iterrows(), total=missing_data_df.shape[0]):
        address = row['address']
        participants_df.loc[i, 'balance'] = w3.eth.get_balance(address, block)
        if i % save_int == 0:
            participants_df.to_pickle(PARTICIPANTS_DF_PATH)
    participants_df.to_pickle(PARTICIPANTS_DF_PATH)
    return participants_df


def update_missing_nonce(w3: web3.Web3, participants_df: pd.DataFrame, save_path: str, save_int: int, block: str) -> pd.DataFrame:
    if 'nonce' not in participants_df:
        participants_df['nonce'] = None
    missing_data_df = participants_df[participants_df['nonce'].isna()]
    missing_data_df = missing_data_df[missing_data_df['address'].notna()]
    print('Patching the {:} missing nonces'.format(missing_data_df.shape[0]))
    for i, row in tqdm(missing_data_df.iterrows(), total=missing_data_df.shape[0]):
        address = row['address']
        participants_df.loc[i, 'nonce'] = w3.eth.get_transaction_count(address, block)
        if i % save_int == 0:
            participants_df.to_pickle(PARTICIPANTS_DF_PATH)
            participants_df.head()
    participants_df.to_pickle(PARTICIPANTS_DF_PATH)
    return participants_df


def patch_missing_df_data(w3: web3.Web3,participants_df: pd.DataFrame, save_path: str, save_int: int=256, block: str='0xED14F1') -> pd.DataFrame:
    participants_df = update_missing_balance(w3, participants_df, save_path, save_int, block)
    participants_df = update_missing_nonce(w3, participants_df, save_path, save_int, block)
    return participants_df


if __name__ == '__main__':
    provider = web3.providers.HTTPProvider(WEB3_PROVIDER)
    w3 = web3.Web3(provider)

    participants_df = pd.DataFrame()
    if os.path.isfile(PARTICIPANTS_DF_PATH):
        participants_df = pd.read_pickle(PARTICIPANTS_DF_PATH)

    with open(TRANSCRIPT_PATH) as json_file:
        transcript = json.load(json_file)
        transcript_df = transcript_to_df(transcript)

    participants_df = insert_new_participants(w3, participants_df, transcript_df)
    participants_df.to_pickle(PARTICIPANTS_DF_PATH)

    patch_missing_df_data(w3, participants_df, PARTICIPANTS_DF_PATH)

    stats.plot_cumulative_field(participants_df, 'nonce')
