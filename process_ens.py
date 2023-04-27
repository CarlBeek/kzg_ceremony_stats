import web3
from tqdm import tqdm
import pandas as pd

def reverse_ens_lookup(w3: web3.Web3, address: str, block_number: int) -> str:
    ens = w3.ens
    reverse_name = f"{address[2:].lower()}.addr.reverse"
    resolver_address = ens.resolver(reverse_name)
    ens_name = ''

    if resolver_address:
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
        resolver_contract = w3.eth.contract(address=resolver_address.address, abi=public_resolver_abi)

        # Call the 'name' function with the specified block number
        ens_name_hash = ens.namehash(reverse_name)
        try:
            ens_name = resolver_contract.functions.name(ens_name_hash).call(block_identifier=block_number)
        except web3.exceptions.BadFunctionCallOutput:  # Some ENS names are badly formatted and this causes the above function to crash
            pass
    return ens_name

def update_missing_ens(w3: web3.Web3, participants_df: pd.DataFrame, save_path: str, save_int: int, block: int) -> pd.DataFrame:
    if 'ens' not in participants_df:
        participants_df['ens'] = None
    missing_data_df = participants_df[participants_df['ens'].isna()]
    missing_data_df = missing_data_df[missing_data_df['address'].notna()]
    num_missing = missing_data_df.shape[0]
    print(f'{num_missing} ENS names missing')
    for i, row in tqdm(missing_data_df.iterrows(), total=num_missing):
        address = row['address']
        participants_df.loc[i, 'ens'] = reverse_ens_lookup(w3, address, block)
        if i % save_int == 0:
            participants_df.to_pickle(save_path)
            participants_df.head()
    participants_df.to_pickle(save_path)
    return participants_df
