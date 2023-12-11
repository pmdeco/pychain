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


class Block:
    def __init__(self, data):
        self.data = data
        self.next = None

    # Block specific functions to print the header data of the block.
    def print_blk_info(self):
        blk_hdr_tup = dsc.unpack_blk_hdr_as_b58(self.data[0])
        print(f'\tBlock Size           {blk_hdr_tup[0]}\n'
              f'\tVersion              {blk_hdr_tup[1]}\n'
              f'\tBlock Hash           {blk_hdr_tup[2]}\n'
              f'\tTimestamp            {blk_hdr_tup[3]}\n'
              f'\tDifficulty Target    {blk_hdr_tup[4]}\n'
              f'\tNonce                {blk_hdr_tup[5]}\n'
              f'\tTransaction Count    {blk_hdr_tup[6]}')

    def get_balance(self, pub_key):
        balance = 0
        for i in self.data[1]:
            tx_tup = dsc.unpack_tx(i)
            if (tx_tup[0] == pub_key) and (tx_tup[1] != pub_key):
                balance = balance - tx_tup[2]
            if (tx_tup[0] != pub_key) and (tx_tup[1] == pub_key):
                balance = balance + tx_tup[2]
        return balance


class Blockchain:
    def __init__(self):
        self.head = None
        self.last_blk_hash = None
        self.block_count = 0
        self.validator_block_count = 0
        self.cache = {}
        self.reward = 1024

    def is_empty(self):
        return self.head is None

    # Fn to create a new block with tuple pair (blk_header, txs[])
    def append(self, data):
        new_blk = Block(data)
        _, _, blk_hash, *_ = dsc.unpack_blk_hdr_as_b58(new_blk.data[0])
        self.block_count += 1
        self.last_blk_hash = blk_hash

        # traverse linked list, add block
        if self.head is None:
            self.head = new_blk
            return
        last_blk = self.head
        while last_blk.next:
            last_blk = last_blk.next
        last_blk.next = new_blk

        # if the data[1] or transactions exist, call update_cache to update the balances
        if new_blk.data[1]:
            self.update_cache(new_blk)

    def update_cache(self, blk):
        txs_lst = []
        for tx in blk.data[1]:
            txs_lst.append(dsc.unpack_tx_as_b58(tx))

        ordered_txs = sorted(txs_lst, key=lambda x: x[3])

        for otx in ordered_txs:
            sender, receiver, amt, timestamp, tx_id, sig = otx

            if sender in self.cache:
                self.cache[sender]['balance'] -= amt
                self.cache[sender]['last_tx'] = (self.block_count, tx_id)
            else:
                self.cache[sender] = {'balance': -amt, 'last_tx': (self.block_count, tx_id)}

            if receiver in self.cache:
                self.cache[receiver]['balance'] += amt
                self.cache[receiver]['last_tx'] = (self.block_count, tx_id)
            else:
                self.cache[receiver] = {'balance': amt, 'last_tx': (self.block_count, tx_id)}

    @staticmethod
    def first_block():
        first_hash = blake3.blake3(b'DSC').digest(length=24)
        return dsc.pack_blk_hdr(128, 1, first_hash, dsc.get_time_ms(), 0, 0, 0), []

    def print_block_hdr_all(self):
        current_blk = self.head
        blk_count = 0
        while current_blk:
            blk_count += 1
            dsc.print_ts(f'Printing All Blocks\n'
                         f'\t\t\t\t\t\t Block #{blk_count}')
            current_blk.print_blk_info()
            current_blk = current_blk.next
        print()

    def print_block_hdr_last(self):
        current_blk = self.head
        blk_count = 0
        while current_blk:
            blk_count += 1
            if current_blk.next is None:
                dsc.print_ts(f'Printing Last Block\n'
                             f'\t\t\t\t\t\t Last Block #{blk_count}')
                current_blk.print_blk_info()
            current_blk = current_blk.next
        print()

    def obtain_block_hdr_all(self):
        lst = []
        current_blk = self.head
        blk_count = 0
        while current_blk:
            blk_count += 1
            lst.append(current_blk.data[0])
            current_blk = current_blk.next
        return lst

    def obtain_block_hdr_last(self):
        current_blk = self.head
        blk_count = 0
        while current_blk:
            blk_count += 1
            if current_blk.next is None:
                return current_blk.data[0]
            current_blk = current_blk.next

    def print_balance(self, pub_key_hr):
        current_blk = self.head
        blk_count = 0
        balance = 0
        while current_blk:
            blk_count += 1
            balance += current_blk.get_balance(pub_key_hr)
            current_blk = current_blk.next
        dsc.print_ts(f'Balance request for ')

    def get_balance(self, pub_key):
        print(f'pub_key')
        current_blk = self.head
        blk_count = 0
        balance = 0
        while current_blk:
            blk_count += 1
            balance += current_blk.get_balance(pub_key)
            current_blk = current_blk.next
        return balance

    def update_reward(self):
        if self.validator_block_count % 10 == 0 and self.reward > 0:
            self.reward = self.reward / 2
            dsc.print_ts(f'Reward Updated: after {self.validator_block_count} validator blocks, '
                         f'reward :: {self.reward}')
        elif self.reward == 1:
            self.reward = 0
            dsc.print_ts(f'Reward Updated: after {self.validator_block_count} validator blocks, '
                         f'reward :: {self.reward}')
        else:
            pass


BC = Blockchain()
BC.append(Blockchain.first_block())


@method
async def ping() -> Result:
    response = dsc.pickled_b58("pong :: blockchain")
    return Success(response)


@method
async def get_balance(pub_key) -> Result:
    if pub_key:
        dsc.print_ts(f'Balance requested for: {pub_key}')
        response = dsc.pickled_b58(BC.cache[pub_key]['balance'])
        return Success(response)


@method
async def get_target_hash() -> Result:
    dsc.print_ts(f'Request for target hash {BC.last_blk_hash}')
    response = dsc.pickled_b58(BC.last_blk_hash)
    return Success(response)


# Returns hash of the last block
@method
async def get_last_hash() -> Result:
    dsc.print_ts(f'Request for last hash: {BC.last_blk_hash}')
    response = dsc.pickled_b58(BC.last_blk_hash)
    return Success(response)


@method
async def get_reward() -> Result:
    response = dsc.pickled_b58(BC.reward)
    return Success(response)


@method
async def add_blank_blk(blk_data) -> Result:
    if blk_data:
        data = dsc.unpickle_b58(blk_data)
        _, _, blk_hash, *_ = dsc.unpack_blk_hdr_as_b58(data[0])
        dsc.print_ts(f'New block received from metronome, Block: {BC.block_count + 1}, hash {blk_hash}')
        BC.append(data)
        response = dsc.pickled_b58("[published]")
        return Success(response)


@method
async def add_new_blk(blk_data) -> Result:
    if blk_data:
        data = dsc.unpickle_b58(blk_data)
        _, _, blk_hash, *_ = dsc.unpack_blk_hdr_as_b58(data[0])
        dsc.print_ts(f'New block received from validator, Block: {BC.block_count + 1}, hash {blk_hash}')
        BC.append(data)
        BC.validator_block_count += 1
        BC.update_reward()
        response = dsc.pickled_b58("[published]")
        return Success(response)


@method
async def get_balance_cache(pub_key) -> Result:
    if pub_key:
        if pub_key in BC.cache:
            result = BC.cache[pub_key]['balance'], BC.cache[pub_key]['last_tx'][0], BC.cache[pub_key]['last_tx'][1]
        else:
            result = 0
        response = dsc.pickled_b58(result)
        return Success(response)


@method
async def monitor_balance_all() -> Result:
    response = dsc.pickled_b58(BC.cache)
    return Success(response)


@method
async def monitor_blockchain_details() -> Result:
    response = dsc.pickled_b58(BC.obtain_block_hdr_all())
    return Success(response)


@method
async def monitor_blockchain_last_block() -> Result:
    response = dsc.pickled_b58(BC.obtain_block_hdr_last())
    return Success(response)


async def request(host, port, jrpc_msg):
    async with websockets.connect(f"ws://{host}:{port}") as ws:
        await ws.send(jrpc_msg)
        response = parse_json(await ws.recv())

    if isinstance(response, Ok):
        return dsc.unpickle_b58(response.result)
    else:
        logging.error(response.message)


async def jrpc_server(websocket, path):
    if response := await async_dispatch(await websocket.recv()):
        await websocket.send(response)


def start_server():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    server = websockets.serve(jrpc_server, BC_ADDR, BC_PORT)

    loop.run_until_complete(server)
    loop.run_forever()


if __name__ == "__main__":
    dsc.print_ts(VERSION)
    server_thread = threading.Thread(target=start_server)
    # _thread = threading.Thread(target=)

    server_thread.start()
    # _thread.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Stopping the server and _ process...")
        server_thread.join()
        # _thread.join()
