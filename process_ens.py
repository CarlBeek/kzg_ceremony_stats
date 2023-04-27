import web3
import requests
import sys
from tqdm import tqdm
import pandas as pd

DEFAULT_ENS_RESOLVER_ADDRESS = '0xA2C122BE93b0074270ebeE7f6b7292C7deB45047'


def _get_resolver_contract(w3: web3.Web3, address: str) -> web3.contract.Contract:
    # Public Resolver ABI (only including the 'name' function)
    public_resolver_abi = [
        {
            "constant": True,
            "inputs": [{"name": "node", "type": "bytes32"}],
            "name": "name",
            "outputs": [{"name": "", "type": "string"}],
            "payable": False,
            "stateMutability": "view",
            "type": "function",
        }
    ]

    # Create a contract instance for the ENS resolver
    resolver_contract = w3.eth.contract(address=address, abi=public_resolver_abi)
    return resolver_contract


def reverse_ens_lookup(w3: web3.Web3, address: str, block_number: int, resolver_contract: web3.contract.Contract) -> str:
    ens = w3.ens
    reverse_name = f"{address[2:].lower()}.addr.reverse"
    ens_name = ''
    # Call the 'name' function with the specified block number
    ens_name_hash = ens.namehash(reverse_name)
    try:
        ens_name = resolver_contract.functions.name(ens_name_hash).call(block_identifier=block_number)
    except web3.exceptions.BadFunctionCallOutput:  # Some ENS names are badly formatted and this causes the above function to crash
        pass
    except requests.exceptions.HTTPError as e:
        print(f'RPC error: {e}')
        sys.exit(1)
    return ens_name

def update_missing_ens(w3: web3.Web3, participants_df: pd.DataFrame, save_path: str, save_int: int, block: int) -> pd.DataFrame:
    if 'ens' not in participants_df:
        participants_df['ens'] = None
    missing_data_df = participants_df[participants_df['ens'].isna()]
    missing_data_df = missing_data_df[missing_data_df['address'].notna()]
    num_missing = missing_data_df.shape[0]
    print(f'{num_missing} ENS names missing')
    resolver_contract = _get_resolver_contract(w3, DEFAULT_ENS_RESOLVER_ADDRESS)
    for i, row in tqdm(missing_data_df.iterrows(), total=num_missing):
        address = row['address']
        participants_df.loc[i, 'ens'] = reverse_ens_lookup(w3, address, block, resolver_contract)
        if i % save_int == 0:
            participants_df.to_pickle(save_path)
            participants_df.head()
    participants_df.to_pickle(save_path)
    return participants_df
