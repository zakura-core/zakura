"""Shared, stdlib-only Zakura monitoring package.

The fleet watchdog on us-east-0 and the compatibility checker on zakura-compat
install the same package at the same commit:

- ``compat``: the local zcashd-compat sync checker (``probe`` and ``check``).
- ``remote``: bounded subprocess and SSH execution.
- ``monitor``: the fleet-side compatibility lane (bounded probe worker,
  untrusted outcome validation, notification text).
- ``slack``: Slack sanitization and webhook transport.
- ``state`` / ``delivery``: durable watchdog state and batched delivery.
- ``suppression``: deployment suppression markers.
- ``install``: versioned, atomic package installation shared by deployments.
"""

PACKAGE_NAME = "zakura_monitoring"
