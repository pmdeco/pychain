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

# Deque for all incoming transactions from the wallets, provides O(1) time for
# pushing and popping to both ends of the deque as transactions come in
tx_deque = deque()

# This is a dictionary to store collected out of the deque by a validator
# Once the validator is posted to the blockchain, the blockchain will remove
# these from this list
# KEY: tx[4]  (tx_id): VALUE: tx
tx_unconfirmed = {}


# @method are used with websockets jrpc server/client calls
@method
async def ping() -> Result:
    return Success("pong :: pool")


# tx_query is an incoming request to query for a transaction based upon its tx_id
# this request comes from the wallet, if the tx_id is found in the deque:
# returns "submitted" status, if not found in the deque, returns "unknown" status
# Deque was chosen
@method
async def tx_query(tx_id) -> Result:
    if tx_id:
        dsc.print_ts(f'Query transaction for {tx_id}')
        for tx in tx_deque:
            tup = dsc.unpack_tx_as_b58(tx)
            if tx_id == tup[4]:
                response = dsc.pickled_b58("[submitted]")
                return Success(response)
        else:
            if tx_id in tx_unconfirmed:
                response = dsc.pickled_b58("[unconfirmed]")
                return Success(response)
            else:
                response = dsc.pickled_b58("[unknown]")
                return Success(response)


# pubid_query is an incoming request to search for all transactions based upon the pubid
# of the wallet. Returns a list of all matching tx with the public key
@method
async def pubid_query(pub_key) -> Result:
    response = []
    if pub_key:
        for tx in tx_deque:
            tup = dsc.unpack_tx_as_b58(tx)
            if pub_key == tup[0]:
                response.append((tx, "[submitted]"))
    response = dsc.pickled_b58(response)
    return Success(response)


# add_deque: incoming request to add a transaction to the pool's deque
@method
async def add_queue(tx) -> Result:
    if tx:
        tx_pkg = dsc.unpickle_b58(tx)
        tx_tup = dsc.unpack_tx_as_b58(tx_pkg)
        dsc.print_ts(
            f"Transaction ID {tx_tup[4]} received from {tx_tup[0]}")
        tx_deque.append(tx_pkg)
        dsc.print_ts(f"Appended to Deque")
        response = dsc.pickled_b58("[added]")
        return Success(response)


# Validator_send_tx: sends a list of transactions, up to 8000 for the validator's next block.
@method
async def validator_send_txs() -> Result:
    tx_arr = []
    while tx_deque and len(tx_arr) < 7999:
        popped_tx = tx_deque.popleft()
        tx_tup = dsc.unpack_tx_as_b58(popped_tx)
        tx_unconfirmed[tx_tup[4]] = popped_tx
        tx_arr.append(popped_tx)
    response = dsc.pickled_b58(tx_arr)
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

    server = websockets.serve(jrpc_server, PL_ADDR, PL_PORT)

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
        print("Stopping the server and reversi process...")
        server_thread.join()
        # _thread.join()
