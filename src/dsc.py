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
import subprocess
import struct
import time
import uuid
import websockets
import yaml

from collections import deque
from datetime import datetime, timezone
from jsonrpcclient import Ok, parse_json
from jsonrpcserver import method, Success, Result, async_dispatch


# Constants #
MAX_UINT = 18_446_744_073_709_551_615  # Unsigned 8-byte Int: 2**(8*8) - 1


# sha256 to create keys
def calc_sha256_str(s):
    return hashlib.sha256(str(s).encode('utf-8')).digest()


def current_time():
    return int(time.time() * 1000)


# Simple function to a tuple and print each element.
def print_tup(tup):
    for t in tup:
        print(t)


# This is the print function called to add a time stamp.
def print_ts(s):
    datetime_current = datetime.now()
    datetime_format = datetime_current.strftime('%Y%m%d %H:%M:%S.%f')[:-3]
    print(f'{datetime_format} {s}')


def to_b58_str(val):
    return base58.b58encode(val).decode('utf-8')


# Used to generate the timestamp when creating a transaction or block
def get_time_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# Used to generate a random nonce for the size of an unsized int
def rand_unsigned_int(val):
    return random.randint(0, val)


def get_ip_addr():
    try:
        # command = f"ip -4 addr show eth0 | grep inet | awk '{{print $2}}' | cut -d'/' -f1"
        command = f"ip -4 addr show wlp3s0 | grep inet | awk '{{print $2}}' | cut -d'/' -f1"
        ip_address = subprocess.check_output(command, shell=True, text=True).strip()
        return ip_address
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        return None


# Transaction #######################################################
#  Format string for transaction when packed into struct            #
#  Little-endian : <                                                #
#  32s : 32-byte string : sender-key                                #
#  32s : 32-byte string : receiver-key                              #
#    d :  8-byte double : amount                                    #
#    Q :  8-byte uint   : timestamp                                 #
#  16s : 16-byte string : transaction ID                            #
#  32s : 32-byte string : signature of tx                           #
#  struct.calcsize('<32s 32s d Q 16s 32s') = 128 bytes              #
# ###################################################################
TX_STOR_FMT = '<32s 32s d Q 16s 32s'


# Returns a byte-literal structure for a transaction
def pack_tx(sender, receiver, amt, timestamp, tx_id, sig):
    return struct.pack(TX_STOR_FMT, sender, receiver, float(amt), timestamp, tx_id, sig)


# Returns a tuple of the elements packed in struct
def unpack_tx(tx_pkg):
    return struct.unpack(TX_STOR_FMT, tx_pkg)


# Returns unpacked struct, with byte literals encoded as base58
def unpack_tx_as_b58(tx_pkg):
    send_key, recv_key, amount, timestamp, tx_id, sig = struct.unpack(TX_STOR_FMT, tx_pkg)
    return to_b58_str(send_key), to_b58_str(recv_key), amount, timestamp, to_b58_str(tx_id), to_b58_str(sig)


def calc_sha256_tup(tup):
    hash_out = hashlib.sha256()
    for t in tup:
        hash_out.update(str(t).encode('utf-8'))
    return hash_out.digest()


# Returns a base58 string of the pickled input, for network usage
def pickled_b58(e):
    pickled = pickle.dumps(e)
    return base58.b58encode(pickled).decode('utf-8')


# Returns original data from serialized base58 pickle for network usage
def unpickle_b58(pkl_b58):
    pickled = base58.b58decode(pkl_b58)
    return pickle.loads(pickled)


# Blockchain Header #################################################
#  Format string for block header packed into struct                #
#  Little-endian : <                                                #
#    I :  4-byte uint    : block-size                               #
#    I :  4-byte uint    : version                                  #
#  32s : 32-byte string  : block hash                               #
#    Q :  8-byte uint    : timestamp                                #
#    L :  4-byte uint    : difficulty target                        #
#    Q :  8-byte uint    : nonce                                    #
#    I :  4-byte uint    : transaction counter                      #
#  64x : 64-byte padding                                            #
#    + : array of 128-byte transactions                             #
#  struct.calcsize('<I I 32s Q L Q I 64x') = 128 bytes + 128B*TX#   #
# ###################################################################
BLK_HDR_STOR_FMT = '<I I 32s Q L Q I 64x'


def pack_blk_hdr(blk_sz, vers, blk_hash, timestamp, diff_target, nonce, tx_count):
    return struct.pack(BLK_HDR_STOR_FMT,
                       blk_sz, vers, blk_hash, timestamp, diff_target, nonce, tx_count)


def unpack_blk_hdr(blk_hdr_pkg):
    return struct.unpack(BLK_HDR_STOR_FMT, blk_hdr_pkg)


# Returns unpacked struct, with byte literals encoded as base58
def unpack_blk_hdr_as_b58(blk_hdr_pkg):
    blk_sz, vers, blk_hash, timestamp, diff, nonce, tx_count = struct.unpack(BLK_HDR_STOR_FMT, blk_hdr_pkg)
    return blk_sz, vers, to_b58_str(blk_hash), timestamp, diff, nonce, tx_count


# ( (blk_hdr) , [ tx , tx , tx, tx ] )
