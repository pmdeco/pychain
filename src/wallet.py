import argparse
import asyncio
import base58
import blake3
import hashlib
import json
import logging
import os
import pickle
import queue
import random
import socket
import struct
import time
import threading
import uuid
import websockets
import yaml

from collections import deque
from datetime import datetime, timezone
from jsonrpcclient import Ok, parse_json, request_json
from jsonrpcserver import method, Success, Result, async_dispatch

import dsc

VERSION = "DSC 1.0"
YAMLCONFIG = yaml.safe_load(open('dsc-config.yaml', 'r'))
MT_ADDR = YAMLCONFIG['metronome']['server']
MT_PORT = YAMLCONFIG['metronome']['port']
PL_ADDR = YAMLCONFIG['pool']['server']
PL_PORT = YAMLCONFIG['pool']['port']
BC_ADDR = YAMLCONFIG['blockchain']['server']
BC_PORT = YAMLCONFIG['blockchain']['port']
VD_PORT = YAMLCONFIG['validator']['port']


# Class defining wallet. Init has storage for private and public keys
# Keys are stored in both human-readable format and literal bytes
class Wallet:
    def __init__(self):
        self.private_key = None
        self.public_key = None
        self.private_key_hr = None
        self.public_key_hr = None
        self.balance = None

    # Creates the keys for the wallet. Public and private keys are generated with SHA-256
    # These are not true key pairs that can sign and verify objects
    # We asked for clarity going back to Nov 9th, but received no response.
    def create_keys(self, seed):
        self.private_key = self.calc_sha256_str(seed)
        self.private_key_hr = base58.b58encode(self.private_key).decode('utf-8')
        self.public_key = self.calc_sha256_str(self.private_key)
        self.public_key_hr = base58.b58encode(self.public_key).decode('utf-8')
        self.print_prv_hr()
        self.print_pub_hr()

    # Takes a tuple, interates through each element, converts each element to a string
    # and creates a hash with shake_128, to obtain a 16-byte hash
    @staticmethod
    def calc_sha128_tup(tup):
        hash_out = hashlib.shake_128()
        for t in tup:
            hash_out.update(str(t).encode('utf-8'))
        return hash_out.digest(16)

    # Takes a tuple, interates through each element, converts each element to a string
    # and creates a hash with sha256, to obtain a 32-byte hash
    @staticmethod
    def calc_sha256_tup(tup):
        hash_out = hashlib.sha256()
        for t in tup:
            hash_out.update(str(t).encode('utf-8'))
        return hash_out.digest()

    # Passes a single element thats converted to a string to generaate a SHA-256 bit hash
    @staticmethod
    def calc_sha256_str(s):
        return hashlib.sha256(str(s).encode('utf-8')).digest()

    # Creates a transaction id, which is 16-bytes per spec, calls calc_sha128_tup
    def calc_tx_id(self, tup):
        return self.calc_sha128_tup(tup)

    # Creates a transaction signatures, which is 32-bytes per spec, calls calc_sha256_tup
    # Note, this is not a true prv/pub key pair
    def calc_sig(self, tup):
        return self.calc_sha256_tup(tup)

    # Attempts to open a wallet file and store keys
    def is_wallet(self, filename):
        for f in filename:
            if os.path.exists(f):
                self.read_keys()
                return True
            else:
                return False

    # Creates wallet keys if no wallet file exists
    def create_wallet(self):
        if not self.is_wallet(('dsc-key.yaml',)):
            self.create_keys(str(get_mac_addr() + str(dsc.get_time_ms())))
            self.save_keys()
        elif self.is_wallet(('dsc-key.yaml',)):
            dsc.print_ts('Wallet already exists at dsc-key.yaml, wallet creation aborted')
            exit(1)
        else:
            pass

    # Saves keys to spec files
    def save_keys(self):
        dsckey = {'wallet': {'private_key': self.private_key_hr}}
        yaml.dump(dsckey, open('dsc-key.yaml', 'w'))
        os.chmod('dsc-key.yaml', 0o400)
        dsccfg = yaml.safe_load(open('dsc-config.yaml', 'r'))
        dsccfg['wallet']['public_key'] = self.public_key_hr
        yaml.dump(dsccfg, open('dsc-config.yaml', 'w'))
        dsc.print_ts('Saved public key to dsc-config.yaml and saved private key to dsc-key.yaml in local folder')

    # Reads from YAML files to get keys, converts base58 keys to bytes
    def read_keys(self):
        dsccfg = yaml.safe_load(open('dsc-config.yaml', 'r'))
        dsckey = yaml.safe_load(open('dsc-key.yaml', 'r'))
        self.public_key_hr = dsccfg['wallet']['public_key']
        self.public_key = base58.b58decode(self.public_key_hr)
        self.private_key_hr = dsckey['wallet']['private_key']
        self.private_key = base58.b58decode(self.private_key_hr)

    # Prints both private and public keys as base58
    def print_keys(self):
        if self.is_wallet(('dsc-key.yaml',)):
            dsc.print_ts("Reading dsc_config.yaml and dsc-key.yaml")
            self.print_pub_hr()
            self.print_prv_hr()
        else:
            dsc.print_ts("Error in finding key information, ensure that dsc-config.yaml and dsc-key.yaml exist and"
                         " that they contain the correct information. You may need to run <~./dsc.py --wallet-create>")

    # Prints both private and publics as byte literals
    def print_keys_bytes(self):
        if self.is_wallet(('dsc-key.yaml',)):
            dsc.print_ts("Reading dsc_config.yaml and dsc-key.yaml")
            self.print_pub()
            self.print_prv()
        else:
            dsc.print_ts("Error in finding key information, ensure that dsc-config.yaml and dsc-key.yaml exist and"
                         " that they contain the correct information. You may need to run <~./dsc.py --wallet-create>")

    # Function to create a transaction.
    def create_transaction(self, recipient, amount):
        if self.is_wallet(('dsc-key.yaml',)):
            recip = base58.b58decode(recipient)
            amount = float(amount)
            tx_time = dsc.get_time_ms()
            tx_id = self.calc_tx_id((self.public_key, recip, amount, tx_time))
            signature = self.calc_sig((self.public_key, recip, amount, tx_time, tx_id, self.private_key))
            dsc.print_ts(f"Transaction created with transaction id: {base58.b58encode(tx_id).decode('utf-8')}")
            return dsc.pack_tx(self.public_key, recip, amount, tx_time, tx_id, signature)

    @staticmethod
    def json_pool_tx_send(tx_pkg):  # json format for sending tx to pool
        tx = dsc.pickled_b58(tx_pkg)  # converts tx pickle encoded in base58
        json_data = {  # because bytes cannot be sent over json
            "jsonrpc": "2.0",
            "method": "add_queue",
            "params": {"tx": tx},
            "id": 1
        }
        return json.dumps(json_data)

    @staticmethod
    def json_pool_tx_query(tx_id):  # json format to query pool with tx_id
        json_data = {
            "jsonrpc": "2.0",
            "method": "tx_query",
            "params": {"tx_id": tx_id},
            "id": 1
        }
        return json.dumps(json_data)

    def json_pool_pubid_query(self):  # json format to query pool with pub_id for all txs
        json_data = {
            "jsonrpc": "2.0",
            "method": "pubid_query",
            "params": {"pub_key": self.public_key_hr},
            "id": 1
        }
        return json.dumps(json_data)

    def json_get_bal(self):  # json format to ask blockchain for balance
        json_data = {
            "jsonrpc": "2.0",
            "method": "get_balance_cache",
            "params": {"pub_key": self.public_key_hr},
            "id": 1
        }
        return json.dumps(json_data)

    def print_prv(self):
        dsc.print_ts(f"DCS Private Key: {self.private_key}")

    def print_pub(self):
        dsc.print_ts(f"DSC Public Key:  {self.public_key}")

    def print_prv_hr(self):
        dsc.print_ts(f"DSC Private Key: {self.private_key_hr}")

    def print_pub_hr(self):
        dsc.print_ts(f"DSC Public Key:  {self.public_key_hr}")

    def __str__(self):
        return f"{self.private_key} {self.public_key}"


# Function to create a unique data to generate private/public keys.
def get_mac_addr():
    mac = ':'.join(['{:02x}'.format((uuid.getnode() >> elements) & 0xff) for elements in range(5, -1, -1)])
    return mac


async def request(host, port, jrpc_msg):
    async with websockets.connect(f"ws://{host}:{port}") as ws:
        await ws.send(jrpc_msg)
        response = parse_json(await ws.recv())

    if isinstance(response, Ok):
        return dsc.unpickle_b58(response.result)
    else:
        logging.error(response.message)


# main fn, with argparse
async def main():
    dsc.print_ts(VERSION)
    wallet = Wallet()
    wallet.is_wallet(('dsc-key.yaml',))

    # https://docs.python.org/3/library/argparse.html#module-argparse
    parser = argparse.ArgumentParser(
        prog='DSC',
        description='DataSys Coin Blockchain')
    parser.add_argument('-wc', '--wallet-create', action='store_true',
                        help='Create a wallet')
    parser.add_argument('-wkb', '--wallet-key-bytes', action='store_true',
                        help='Display public and private keys as Bytes')
    parser.add_argument('-wk', '--wallet-key', action='store_true',
                        help='Display public and private keys as Base58')
    parser.add_argument('-wb', '--wallet-balance', action='store_true',
                        help='Query balance from blockchain')
    parser.add_argument('-ws', '--wallet-send', nargs=2, metavar=('amount', 'address'),
                        help='Send coin <amount> to <address>')
    parser.add_argument('-wt', '--wallet-transaction', nargs=1, metavar='transaction-id',
                        help='Query the blockchain and pools for a specific transaction')
    parser.add_argument('-wtx', '--wallet-transactions', action='store_true',
                        help='Query the blockchain and pools for all transactions associated with your public key')
    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
    if args.wallet_create:
        wallet.create_wallet()
    if args.wallet_key:
        wallet.print_keys()
    if args.wallet_key_bytes:
        wallet.print_keys_bytes()
    if args.wallet_send:
        amount, recipient = args.wallet_send
        tx_pkg = wallet.create_transaction(recipient, amount)
        response = await request(PL_ADDR, PL_PORT, wallet.json_pool_tx_send(tx_pkg))
        dsc.print_ts(f'Transaction :: {response}')
    if args.wallet_balance:
        response = await request(BC_ADDR, BC_PORT, wallet.json_get_bal())
        dsc.print_ts(f'Current Balance :: {response[0]} at Block {response[1]}, last transaction {response[2]}')
    if args.wallet_transaction:
        tx_id = args.wallet_transaction[0]
        dsc.print_ts(f'{tx_id}')
        response = await request(PL_ADDR, PL_PORT, wallet.json_pool_tx_query(tx_id))
        dsc.print_ts(f'Query :: {tx_id} :: {response}')
    if args.wallet_transactions:
        response = await request(PL_ADDR, PL_PORT, wallet.json_pool_pubid_query())
        for i in response:
            tx = dsc.unpack_tx_as_b58(i[0])
            dsc.print_ts(f"{tx[4]} {i[1]}")


if __name__ == "__main__":
    asyncio.run(main())
