"""Normalize peer addresses at the profiler and dashboard input boundaries."""

import ipaddress


def canonical_ip(text):
    """Return the canonical IP from an address or socket address, else `None`.

    Accept bare IPv4 and IPv6, `ip:port`, and `[ip]:port`. IPv4 mapped into IPv6
    comes back as IPv4, so privacy checks recognize every spelling of one peer.
    Redacted labels and other non-address values return `None`.
    """
    if not isinstance(text, str):
        return None
    host = text.rsplit(":", 1)[0] if text.count(":") == 1 or text.startswith("[") else text
    try:
        address = ipaddress.ip_address(host.strip("[]"))
    except ValueError:
        return None
    return str(getattr(address, "ipv4_mapped", None) or address)
