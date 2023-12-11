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


# Validator class
class Validator:
    def __init__(self, fingerprint):
        self.fingerprint = fingerprint
        self.private_key = None
        self.private_key_hr = None
        self.public_key = None
        self.public_key_hr = None
        self.ip_addr = None

    def create_keys(self, fingerprint):
        self.private_key = dsc.calc_sha256_str(fingerprint)
        self.private_key_hr = base58.b58encode(self.private_key).decode('utf-8')
        self.public_key = dsc.calc_sha256_str(self.private_key)
        self.public_key_hr = base58.b58encode(self.public_key).decode('utf-8')

    # Obtain own IP address to send to metronome for registration
    def get_own_ip(self):
        try:
            host = socket.gethostname()
            self.ip_addr = socket.gethostbyname(host)
        except socket.error as e:
            print(f'Socket Error: {e}')

    @staticmethod
    def json_blockchain_new_blk(data):
        blk = dsc.pickled_b58(data)
        json_data = {
            "jsonrpc": "2.0",
            "method": "add_new_blk",
            "params": {"blk_data": blk},
            "id": 1
        }
        return json.dumps(json_data)

    # json message to obtain last hash from bc
    @staticmethod
    def json_blockchain_get_last_hash():
        json_data = {
            "jsonrpc": "2.0",
            "method": "get_last_hash",
            "id": 1
        }
        return json.dumps(json_data)

    # json msg to obtain target hash value from bc
    @staticmethod
    def json_blockchain_get_target_hash():
        json_data = {
            "jsonrpc": "2.0",
            "method": "get_target_hash",
            "id": 1
        }
        return json.dumps(json_data)

    # json msg format for notify validator of found hash
    def json_metronome_found_hash(self, flag, nonce, hash_rate):
        tup = (self.fingerprint, self.public_key, flag, nonce, hash_rate)
        data = dsc.pickled_b58(tup)
        json_data = {
            "jsonrpc": "2.0",
            "method": "found_hash",
            "params": {"tup": data},
            "id": 1
        }
        return json.dumps(json_data)

    @staticmethod
    def json_metronome_get_difficulty():
        json_data = {
            "jsonrpc": "2.0",
            "method": "get_difficulty",
            "id": 1
        }
        return json.dumps(json_data)

    # json msg format for registering with metronome
    def json_metronome_register(self):
        data = dsc.pickled_b58((self.fingerprint, self.ip_addr))
        json_data = {
            "jsonrpc": "2.0",
            "method": "register_validator",
            "params": {"data": data},
            "id": 1
        }
        return json.dumps(json_data)

    @staticmethod
    def json_pool_request_txs():
        json_data = {
            "jsonrpc": "2.0",
            "method": "validator_send_txs",
            "id": 1
        }
        return json.dumps(json_data)

    async def proof_of_work(self):
        nonce = 0
        last_hash_b58 = await request(BC_ADDR, BC_PORT, self.json_blockchain_get_last_hash())
        last_hash_bytes = base58.b58decode(last_hash_b58)
        target_hash = ''.join(format(byte, '08b') for byte in last_hash_bytes)
        difficulty = await request(MT_ADDR, MT_PORT, self.json_metronome_get_difficulty())
        start_time = time.time()
        while time.time() - start_time < 6:
            hasher = blake3.blake3(self.fingerprint + self.public_key +
                                   nonce.to_bytes(16, 'little', signed=False) + last_hash_bytes).digest(length=24)
            hasher_str = ''.join(format(byte, '08b') for byte in hasher)
            if target_hash[:difficulty] == hasher_str[:difficulty]:
                flag = 1
                duration = time.time() - start_time
                hash_rate = (nonce / duration)
                dsc.print_ts(f'[Success] Matching hash with {nonce} in {duration} seconds at difficulty {difficulty}')
                break
            else:
                nonce = nonce + 1
        else:
            flag = -1
            hash_rate = (nonce / 6)
        sent_pow = await request(MT_ADDR, MT_PORT, self.json_metronome_found_hash(flag, nonce, hash_rate))
        dsc.print_ts(f'Sent hash metrics to metronome: {sent_pow}')
        time_remaining = start_time + 6 - time.time()
        if time_remaining > 0:
            await asyncio.sleep(time_remaining)

    async def tx_winner_reward(self, reward):
        time_stamp = dsc.get_time_ms()
        tx_id = dsc.calc_sha256_tup((b'REWARD', self.public_key, reward, time_stamp))
        tx_sig = dsc.calc_sha256_tup((b'REWARD', self.public_key, reward, time_stamp, tx_id, self.private_key))
        return dsc.pack_tx(b'REWARD', self.public_key, reward, time_stamp, tx_id, tx_sig)


fprint = str(uuid.uuid4())
validator = Validator(fprint.encode('utf-8'))


def get_ip():
    try:
        host = socket.gethostname()
        return socket.gethostbyname(host)
    except socket.error as e:
        print(f'Socket Error: {e}')


@method
async def ping() -> Result:
    response = dsc.pickled_b58("pong :: validator")
    return Success(response)


@method
async def create_block(data) -> Result:
    winning_hash, difficulty, nonce, reward = dsc.unpickle_b58(data)
    tx_arr = await request(PL_ADDR, PL_PORT, validator.json_pool_request_txs())
    tx_arr.append(await validator.tx_winner_reward(reward))
    len_tx_arr = len(tx_arr)
    data = (dsc.pack_blk_hdr((128 + (128 * len_tx_arr)), 1, winning_hash,
                             dsc.get_time_ms(), difficulty, nonce, len_tx_arr),
            tx_arr)
    print(f'{len_tx_arr}')
    response = await request(BC_ADDR, BC_PORT, validator.json_blockchain_new_blk(data))
    dsc.print_ts(f'New block created by {validator.fingerprint.decode("utf-8")} in blockchain, '
                 f'hash {winning_hash.decode("utf-8")} :: {response}')
    response = dsc.pickled_b58(f'New block created by {validator.fingerprint} in blockchain, '
                               f'hash {winning_hash} :: {response}')
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

    VD_ADDR_IP = get_ip()

    server = websockets.serve(jrpc_server, VD_ADDR_IP, VD_PORT)

    loop.run_until_complete(server)
    loop.run_forever()


def start_validator():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    v = run_validator()

    loop.run_until_complete(v)
    loop.run_forever()


async def run_validator():
    validator.get_own_ip()
    validator.create_keys(validator.fingerprint)

    YAMLCONFIG['validator']['fingerprint'] = validator.fingerprint.decode('utf-8')
    YAMLCONFIG['validator']['public_key'] = validator.public_key_hr
    with open('dsc-config.yaml', 'w') as config_file:
        yaml.dump(YAMLCONFIG, config_file)

    await request(MT_ADDR, MT_PORT, validator.json_metronome_register())

    # https://docs.python.org/3/library/argparseexit.html#module-argparse
    parser = argparse.ArgumentParser(
        prog='DSC Validator',
        description='DataSys Validator')
    parser.add_argument('-pc', '--pos-check', action='store_true',
                        help='Proof of Space Sanity Check')
    args = parser.parse_args()

    if args.pos_check:
        dsc.print_ts("POS CHECK")
    else:
        if YAMLCONFIG['validator']['proof_pow']['enable']:
            dsc.print_ts("POW")
            while True:
                await validator.proof_of_work()
        elif YAMLCONFIG['validator']['proof_pom']['enable']:
            pass
            # dsc.print_ts("POM")
            # hash_tree = await validator.proof_of_memory(5000)
            # while True:
            # await validator.metronome_found_hash(mt_port,
            # await validator.proof_of_memory_lookup(hash_tree,
            # bc_port, mt_port))
        elif YAMLCONFIG['validator']['proof_pos']['enable']:
            dsc.print_ts("POS")
        else:
            dsc.print_ts("Config not properly enabled")


if __name__ == "__main__":
    server_thread = threading.Thread(target=start_server)
    validator_thread = threading.Thread(target=start_validator)

    server_thread.start()
    validator_thread.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Stopping the server and reversi process...")
        server_thread.join()
        validator_thread.join()
