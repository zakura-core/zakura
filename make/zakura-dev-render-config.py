#!/usr/bin/env python3
"""Render make/zakura-dev.toml into the local zakura-dev config."""

from __future__ import annotations

import os
import pathlib

PEERS = {
    "Mainnet": [
        "1398f62c6d1a457c51ba6a4b5f3dbd2f69fca93216218dc8997e416bd17d93ca@165.22.54.66:8234",
        "fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618@104.131.184.123:8234",
        "9ec67ad6834bc2ca0d659c240e042d3446c37cabcc092b527d459c87d938b4a4@159.65.183.89:8234",
        "bd3dc5d2a3d44c6bf90e364bf446231dbf9737e38a562ccf9e91ea631ea59b22@143.244.184.176:8234",
        "14ab98fa0c4b07d40119e1dbc9f3c36d20c8f226ae5ba4216218a2034f148e57@159.203.38.10:8234",
        "681d21b18644cd82ec13256a97f92bec1fff815683ef6f65dc7c993f098a4fe5@64.227.44.93:8234",
        "058b3f20dc9bef7bb447f94d7663d793cfbc036720f97e52d7f13661b21818e1@161.35.156.226:8234",
        "291323d78eb7186c3fa225ef5e305e95363e0ef06d42dca91bd4ef0254aed1ae@139.59.64.115:8234",
        "85e425233a68697d4be91dd5d542305a8a327cd06d992d53c0913cef2fa75084@168.144.173.250:8234",
    ],
    "Testnet": [
        "57ad39fad4f0bca46cf1ea831772a99d5027b372fef2be5a0ea68e1b5bb4da49@167.99.103.111:8234",
        "4faac8f988a7820690d63b57a385cd6f833638b068e774550712c05e4b692426@167.99.110.145:8234",
        "9ce6b95aa197d169399788fe01dd8a88140e81d23d00b4739aeeb1113c6247a2@138.68.229.254:8234",
        "2bbb907b5d90598ef49f2e637066586b311a64587479be6ed43e8388587fcd2a@164.92.209.78:8234",
        "50999835f48f4a048c0e9042e5332844c9673943d7fab1f7e993bae698c27ea3@206.189.148.0:8234",
    ],
}


def main() -> None:
    network = os.environ["NETWORK"]
    peer_lines = "\n".join(f'    "{peer}",' for peer in PEERS[network])
    template = pathlib.Path(os.environ["TEMPLATE"])
    output = pathlib.Path(os.environ["OUTPUT"])
    text = template.read_text()
    text = (
        text.replace("@@NETWORK@@", network)
        .replace("@@CACHE_DIR@@", os.environ["CACHE_DIR"])
        .replace("@@IDENTITY_DIR@@", os.environ["IDENTITY_DIR"])
        .replace("@@TRACE_DIR@@", os.environ["TRACE_DIR"])
        .replace("@@BOOTSTRAP_PEERS@@", peer_lines)
    )
    output.write_text(text)


if __name__ == "__main__":
    main()
