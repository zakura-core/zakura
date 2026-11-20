#!/bin/sh
# LAB ONLY: reruns after the controller's RPC traffic was taken out of the shaped path.
# Instances 1-3 and 9 are up on the dual stack, 7 and 8 as dual with B on legacy.
# Races pace at 33 s, not 30 s: each node refreshes its header-sync Status every 30 s and sends
# at most one Status a second, so a 30 s pace lands the refresh just before A's block and holds
# A's header back about 350 ms.
cd "$(dirname "$0")"; mkdir -p results
lag() { # inst rtt: header lag after submit, and a truly unshaped-rate 2 MB cell
  python3 lab.py exp1 "$1" --stack dual --sizes 0,500000,2000000 --rtt "$2" --rate 50 --idle 18 \
    --trials 8 --out "results/lag-dual-rtt$2.csv" >> "results/lag-inst$1.log" 2>&1
  python3 lab.py exp1 "$1" --stack dual --sizes 2000000 --rtt "$2" --rate 0 --idle 18 \
    --trials 8 --out "results/lag-dual-rtt$2.csv" >> "results/lag-inst$1.log" 2>&1
}
lag 1 100 & lag 2 20 & lag 3 300 &
# Experiment 2: B reaches O over legacy gossip only, so O holds both bodies and the claim decides.
( python3 lab.py exp2 7 --stack dual --label "dual,B=legacy" --sizes 2000000 \
    --offsets 0,50,100,200,400,700,1000 --windows=-1,0 --rtt 100 --rate 50 --trials 6 --pace 33 \
    --out results/exp2-mixed.csv >> results/exp2-inst7.log 2>&1 ) &
# Experiment 3: same topology, A sits on its body. Then an honest 10 Mbit race.
( python3 lab.py exp3 8 --stack dual --label "dual,B=legacy" --size 2000000 \
    --delays 0,250,500,750,1000,1250,1500,1750,2000,2500 --offsets 600 --rtt 100 --rate 50 \
    --trials 6 --pace 33 --out results/exp3-mixed.csv >> results/exp3-inst8.log 2>&1
  python3 lab.py exp2 8 --stack dual --label "dual,B=legacy" --sizes 2000000 \
    --offsets 600,1200 --windows=-1,0 --rtt 100 --rate 10 --trials 6 --pace 33 \
    --out results/exp2-mixed-10mbit.csv >> results/exp3-inst8.log 2>&1 ) &
# Every node on the dual stack: withholding, then an honest race.
( python3 lab.py exp3 9 --stack dual --label "dual" --size 2000000 \
    --delays 0,2000,10000,30000 --offsets 600 --rtt 100 --rate 50 \
    --trials 3 --pace 33 --out results/exp3-native.csv >> results/exp3-inst9.log 2>&1
  python3 lab.py exp2 9 --stack dual --label "dual" --sizes 2000000 \
    --offsets 0,200,1000 --windows=-1 --rtt 100 --rate 50 --trials 4 --pace 33 \
    --out results/exp2-native.csv >> results/exp3-inst9.log 2>&1 ) &
wait
