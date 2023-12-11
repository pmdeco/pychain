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


class Metronome:
    def __init__(self):
        self.difficulty = 20
        self.registered_validators = {}
        self.found_hashes = []
        # each found hash is a tuple (fingerprint, public_key, nonce) used to confirm calc

    @staticmethod
    def create_blank_block_data(last_hash):
        nonce = dsc.rand_unsigned_int(dsc.MAX_UINT)
        new_hash = blake3.blake3(last_hash + nonce.to_bytes(16, 'little', signed=False)).digest(length=24)
        data = ((dsc.pack_blk_hdr(128, 1, new_hash, dsc.get_time_ms(), 0, nonce, 0)), ())
        return data

    @staticmethod
    def json_blockchain_blank_blk_send(data):
        blk_data = dsc.pickled_b58(data)
        json_data = {
            "jsonrpc": "2.0",
            "method": "add_blank_blk",
            "params": {"blk_data": blk_data},
            "id": 1
        }
        return json.dumps(json_data)

    @staticmethod
    def json_blockchain_get_last_hash():
        json_data = {
            "jsonrpc": "2.0",
            "method": "get_last_hash",
            "params": {},
            "id": 1
        }
        return json.dumps(json_data)

    @staticmethod
    def json_blockchain_get_reward():
        json_data = {
            "jsonrpc": "2.0",
            "method": "get_reward",
            "params": {},
            "id": 1
        }
        return json.dumps(json_data)

    @staticmethod
    def json_validator_notify_winner(winning_hash, difficulty, nonce, reward):
        data = (winning_hash, difficulty, nonce, reward)
        data = dsc.pickled_b58(data)
        json_data = {
            "jsonrpc": "2.0",
            "method": "create_block",
            "params": {"data": data},
            "id": 1
        }
        return json.dumps(json_data)

    async def pick_a_winner(self):
        last_hash_b58 = await request(BC_ADDR, BC_PORT, self.json_blockchain_get_last_hash())
        last_hash_bytes = base58.b58decode(last_hash_b58)
        reward = await request(BC_ADDR, BC_PORT, self.json_blockchain_get_reward())
        target_hash = ''.join(format(byte, '08b') for byte in last_hash_bytes)
        difficulty = self.difficulty
        winner = self.found_hashes.pop(random.choice(range(len(self.found_hashes))))
        fingerprint, public_key, nonce = winner
        hasher = blake3.blake3(fingerprint + public_key +
                               nonce.to_bytes(16, 'little', signed=False) + last_hash_bytes).digest(length=24)
        hasher_str = ''.join(format(byte, '08b') for byte in hasher)
        winning_hash = base58.b58encode(hasher)
        if target_hash[:difficulty] == hasher_str[:difficulty]:
            response = await request(self.registered_validators[fingerprint], VD_PORT,
                                     self.json_validator_notify_winner(winning_hash, difficulty, nonce, reward))
            await self.update_difficulty()
            self.found_hashes.clear()
            print(f'Winning hash matches: winner notified.\n\t{response}')
        elif len(self.found_hashes) > 0:
            print(f'[Failure]: unable to match last hash.')
            await self.pick_a_winner()
        else:
            pass

    async def send_blk_whileloop(self):
        while True:
            await asyncio.sleep(6)
            if len(self.found_hashes) > 0:
                await self.pick_a_winner()
            else:
                try:
                    await self.send_blank_blk_data()
                    await self.update_difficulty()
                except Exception as e:
                    logging.error(f'Error sending blank block data: {e}')

    async def send_blank_blk_data(self):
        last_hash_b58 = await request(BC_ADDR, BC_PORT, self.json_blockchain_get_last_hash())
        last_hash_bytes = base58.b58decode(last_hash_b58)
        data = self.create_blank_block_data(last_hash_bytes)
        result = await request(BC_ADDR, BC_PORT, self.json_blockchain_blank_blk_send(data))
        dsc.print_ts(f'Blank block sent to blockchain: {result}')

    async def update_difficulty(self):
        if len(self.found_hashes) + 1 > 8:
            self.difficulty += 1
        elif len(self.found_hashes) + 1 < 4:
            self.difficulty -= 1
        else:
            pass
        if self.difficulty < 10:
            self.difficulty = 10


@method
async def ping() -> Result:
    response = dsc.pickled_b58("pong :: metronome")
    return Success(response)


@method
async def found_hash(tup) -> Result:
    fingerprint, public_id, flag, nonce, hash_rate = dsc.unpickle_b58(tup)
    if flag <= -1:
        dsc.print_ts(f'Validator {fingerprint} failed to find matching hash. Hash rate: {hash_rate}')
    else:
        dsc.print_ts(f'Validator {fingerprint} found a winning hash with nonce {nonce}. Hash rate: {hash_rate}')
        metronome.found_hashes.append((fingerprint, public_id, nonce))
    response = dsc.pickled_b58("[hash metrics received]")
    return Success(response)


@method
async def get_difficulty() -> Result:
    response = dsc.pickled_b58(metronome.difficulty)
    return Success(response)


@method
async def register_validator(data) -> Result:
    fingerprint, ip_addr = dsc.unpickle_b58(data)
    metronome.registered_validators[fingerprint] = ip_addr
    print(f'{metronome.registered_validators}')
    response = dsc.pickled_b58("[Registered]")
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


metronome = Metronome()


def start_metronome_pick_a_winner():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    sending_blocks = metronome.send_blk_whileloop()

    loop.run_until_complete(sending_blocks)
    loop.run_forever()


def start_server():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    server = websockets.serve(jrpc_server, MT_ADDR, MT_PORT)

    loop.run_until_complete(server)
    loop.run_forever()


if __name__ == "__main__":
    dsc.print_ts(VERSION)
    server_thread = threading.Thread(target=start_server)
    metronome_thread = threading.Thread(target=start_metronome_pick_a_winner)

    server_thread.start()
    metronome_thread.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Stopping the server and metronome process...")
        server_thread.join()
        metronome_thread.join()
