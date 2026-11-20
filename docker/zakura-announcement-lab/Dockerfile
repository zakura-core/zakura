# LAB ONLY: runtime image for the announcement-window lab. The node binary is mounted in.
# faketime moves a node's wall clock (edge case `future`); tcpdump captures legacy gossip.
FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 iputils-ping iperf3 ca-certificates faketime tcpdump && rm -rf /var/lib/apt/lists/*
