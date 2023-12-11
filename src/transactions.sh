#!/bin/bash

for i in $(seq 1 1 100000);
do
  python3 wallet.py -ws 2.3456 b'abcdefg';
done