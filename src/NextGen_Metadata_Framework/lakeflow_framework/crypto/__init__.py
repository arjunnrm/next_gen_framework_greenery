"""Cryptography: Unity Catalog secret resolution, column-level AES at rest, and PGP.

The surrogate-key hashing module (``crypto/hashing.py``) was removed in v1.4.0 along with the
rest of the surrogate-key engine. The framework's one remaining hash construction lives in
``cdc/hashing.py`` and produces ``__framework_hash_key``/``__framework_hash_value`` only.
"""
