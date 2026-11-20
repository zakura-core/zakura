#!/bin/sh
# LAB ONLY: experiment 1 matrix. Usage: run_exp1.sh <stack> <first-inst>
# Uses three instances, one per RTT, starting at <first-inst>.
#
# Blocks are spaced 18 s apart. A node announces a new block-sync tip early only while its
# Status bucket for that peer is more than half full, and the bucket refills one token per 15 s.
# Faster trials drain it and add about 10 s to every delivery, which mainnet's 75 s spacing
# never does. Funding the chain drains it too, hence the settle time.
cd "$(dirname "$0")"
stack="$1"; first="$2"; mkdir -p results
run() { # inst rtt
  log="results/exp1-$stack-inst$1.log"
  python3 lab.py up "$1" --stack "$stack" >> "$log" 2>&1
  sleep 200
  for rate in 50 10; do
    python3 lab.py exp1 "$1" --stack "$stack" --sizes 2000000,1000000,500000,0 \
      --rtt "$2" --rate "$rate" --idle 18 --trials 12 --out "results/exp1-$stack-rtt$2.csv" \
      >> "$log" 2>&1
  done
  python3 lab.py exp1 "$1" --stack "$stack" --sizes 2000000 --rtt "$2" --rate 0 --idle 18 \
    --trials 12 --out "results/exp1-$stack-rtt$2.csv" >> "$log" 2>&1
}
run "$first" 100 & run "$((first + 1))" 20 & run "$((first + 2))" 300 & wait
