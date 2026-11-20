#!/bin/sh
# LAB ONLY: race and withholding experiments on the 100 ms, 50 Mbit link, 2 MB block from A.
# Instance 7 must already be up as: lab.py up 7 --stack dual --stack-b legacy
# Races pace at 33 s, not 30 s: each node refreshes its header-sync Status every 30 s and sends
# at most one Status a second, so a 30 s pace lands the refresh just before A's block and holds
# A's header back about 350 ms.
cd "$(dirname "$0")"; mkdir -p results
# Experiment 2: B reaches O over legacy gossip only, so O holds both bodies and the claim decides.
python3 lab.py exp2 7 --stack dual --label "dual,B=legacy" --sizes 2000000 \
  --offsets 0,250,500,750,1000,1500 --windows=-1,0 --rtt 100 --rate 50 --trials 8 --pace 33 \
  --out results/exp2-mixed.csv >> results/exp2-inst7.log 2>&1 &
# Experiment 3: same topology, A sits on its body.
( python3 lab.py up 8 --stack dual --stack-b legacy >> results/exp3-inst8.log 2>&1; sleep 200
  python3 lab.py exp3 8 --stack dual --label "dual,B=legacy" --size 2000000 \
    --delays 0,500,1000,1250,1500,1750,2000,2500,3000 --offsets 1000 --rtt 100 --rate 50 \
    --trials 6 --pace 33 --out results/exp3-mixed.csv >> results/exp3-inst8.log 2>&1 ) &
# Experiment 3 with every node on the dual stack: O never fetches B's body.
( python3 lab.py up 9 --stack dual >> results/exp3-inst9.log 2>&1; sleep 200
  python3 lab.py exp3 9 --stack dual --label "dual" --size 2000000 \
    --delays 0,2000,5000,10000 --offsets 1000 --rtt 100 --rate 50 \
    --trials 4 --pace 33 --out results/exp3-native.csv >> results/exp3-inst9.log 2>&1 ) &
wait
