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

    # https://docs.python.org/3/library/argparse.html#module-argparse
    parser = argparse.ArgumentParser(
        prog='DSC',
        description='DataSys Coin Blockchain')
    parser.add_argument('-bl', '--blockchain-last-block', action='store_true',
                        help='Obtain details on latest block on the blockchain')
    parser.add_argument('-br', '--blockchain-reward-status', action='store_true',
                        help='Display details on the blockchain')

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
