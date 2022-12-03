import os
from web3 import Web3

w3 = Web3(Web3.HTTPProvider(os.getenv("INFURA_URL")))


def get_ens(name):
    return w3.ens.address(name=name)


def get_ens_reverse(address):
    return w3.ens.name(address=address, strict=False)
