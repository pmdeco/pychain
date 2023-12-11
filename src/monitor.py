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


def json_monitor_balance_all():  # json format to ask blockchain for balance
    json_data = {
        "jsonrpc": "2.0",
        "method": "monitor_balance_all",
        "id": 1
    }
    return json.dumps(json_data)


def json_monitor_blockchain_details():  # json format to ask blockchain for balance
    json_data = {
        "jsonrpc": "2.0",
        "method": "monitor_blockchain_details",
        "id": 1
    }
    return json.dumps(json_data)


def json_blockchain_last_block():  # json format to ask blockchain for balance
    json_data = {
        "jsonrpc": "2.0",
        "method": "monitor_blockchain_last_block",
        "id": 1
    }
    return json.dumps(json_data)


def json_get_bal(pub_key):  # json format to ask blockchain for balance
    json_data = {
        "jsonrpc": "2.0",
        "method": "get_balance_cache",
        "params": {"pub_key": pub_key},
        "id": 1
    }
    return json.dumps(json_data)


def json_get_reward():  # json format to ask blockchain for balance
    json_data = {
        "jsonrpc": "2.0",
        "method": "get_reward",
        "id": 1
    }
    return json.dumps(json_data)


async def blockchain_balance_all():
    response = await request(BC_ADDR, BC_PORT, json_monitor_balance_all())
    for key in response:
        dsc.print_ts(f'{key} :: Balance {response[key]["balance"]} :: '
                     f'Last Tx {response[key]["last_tx"][0]} :: @ Block {response[key]["last_tx"][1]}')


async def monitor_blockchain_details():
    blk_count = 1
    response = await request(BC_ADDR, BC_PORT, json_monitor_blockchain_details())
    dsc.print_ts(f'Block #{blk_count}')
    for hdr in response:
        blk_hdr_tup = dsc.unpack_blk_hdr_as_b58(hdr)
        print(f'\tBlock Size           {blk_hdr_tup[0]}\n'
              f'\tVersion              {blk_hdr_tup[1]}\n'
              f'\tBlock Hash           {blk_hdr_tup[2]}\n'
              f'\tTimestamp            {blk_hdr_tup[3]}\n'
              f'\tDifficulty Target    {blk_hdr_tup[4]}\n'
              f'\tNonce                {blk_hdr_tup[5]}\n'
              f'\tTransaction Count    {blk_hdr_tup[6]}')
        print()
        blk_count += 1


async def blockchain_last_block():
    response = await request(BC_ADDR, BC_PORT, json_blockchain_last_block())
    blk_hdr_tup = dsc.unpack_blk_hdr_as_b58(response)
    print(f'\tBlock Size           {blk_hdr_tup[0]}\n'
          f'\tVersion              {blk_hdr_tup[1]}\n'
          f'\tBlock Hash           {blk_hdr_tup[2]}\n'
          f'\tTimestamp            {blk_hdr_tup[3]}\n'
          f'\tDifficulty Target    {blk_hdr_tup[4]}\n'
          f'\tNonce                {blk_hdr_tup[5]}\n'
          f'\tTransaction Count    {blk_hdr_tup[6]}')


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
    parser.add_argument('-ba', '--blockchain-all-blocks', action='store_true',
                        help='Print headers from all blocks on the blockchain')
    parser.add_argument('-bl', '--blockchain-last-block', action='store_true',
                        help='Print headers from last block on the blockchain')
    parser.add_argument('-br', '--blockchain-reward-status', action='store_true',
                        help='Display details on the blockchain')
    parser.add_argument('-bb', '--blockchain-all-balances', action='store_true',
                        help='Display details all balances')
    parser.add_argument('-bp', '--blockchain-pubkey-balances', nargs=1, metavar='pub-id',
                        help='Display details public key balance, requires argument <pub_key>')

    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
    if args.blockchain_all_blocks:
        await monitor_blockchain_details()
    if args.blockchain_last_block:
        await blockchain_last_block()
    if args.blockchain_reward_status:
        response = await request(BC_ADDR, BC_PORT, json_get_reward())
        dsc.print_ts(f'Current reward amount: {response}')
    if args.blockchain_all_balances:
        await blockchain_balance_all()
    if args.blockchain_pubkey_balances:
        response = await request(BC_ADDR, BC_PORT, json_get_bal(args.blockchain_pubkey_balances[0]))
        dsc.print_ts(f'Current Balance :: {response[0]} at Block {response[1]}, last transaction {response[2]}')


if __name__ == "__main__":
    asyncio.run(main())
