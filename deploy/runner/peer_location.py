"""Where a peer outside the fleet is, looked up on this host in a downloaded database.

The database is DB-IP's free "IP to City Lite" file (MaxMind DB format), which the
dashboard deploy downloads; its licence asks every page that shows a location to credit
DB-IP. No address is ever sent anywhere to be located.

Locations are optional: without the database, or without the `maxminddb` reader
(Debian/Ubuntu package `python3-maxminddb`), every peer simply has no location.
"""

import functools
import logging
import os

# Text read from the database is shown on the page; anything longer is cut.
MAX_NAME_LENGTH = 64
# Peers whose location is remembered between page refreshes; the page lists at most 200.
CACHED_PEERS = 4096


def no_location(ip):
    """The locator to use when locations are switched off or cannot be read."""
    return None


def open_locator(path):
    """`locate(ip)` from the database at `path`: `{"country", "country_name", "city"}` or `None`.

    `country` is the two-letter ISO code. `None` comes back for an address the database
    does not place, and for every address when `path` is empty, missing, unreadable, or
    the reader is not installed (said once in the log).
    """
    if not path:
        return no_location
    try:
        import maxminddb
    except ImportError:
        logging.warning("peer locations are off: the maxminddb module is not installed")
        return no_location
    if not os.path.isfile(path):
        logging.warning("peer locations are off: %s does not exist", path)
        return no_location
    try:
        reader = maxminddb.open_database(path)
    except (OSError, ValueError) as error:
        logging.warning("peer locations are off: cannot read %s: %s", path, error)
        return no_location

    @functools.lru_cache(maxsize=CACHED_PEERS)
    def locate(ip):
        try:
            found = reader.get(ip)
        except ValueError:
            return None
        return location_of(found)

    return locate


def location_of(found):
    """The part of one database record the page shows, or `None` when it names no country."""
    if not isinstance(found, dict):
        return None
    country = found.get("country")
    code = country.get("iso_code") if isinstance(country, dict) else None
    if not isinstance(code, str) or len(code) != 2 or not code.isalpha():
        return None
    return {
        "country": code.upper(),
        "country_name": english_name(country) or code.upper(),
        "city": english_name(found.get("city")),
    }


def english_name(entry):
    names = entry.get("names") if isinstance(entry, dict) else None
    name = names.get("en") if isinstance(names, dict) else None
    return name[:MAX_NAME_LENGTH] if isinstance(name, str) and name else None
