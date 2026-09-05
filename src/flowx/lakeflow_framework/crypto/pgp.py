"""PGP encrypt/decrypt/sign, via ``PGPy`` -- pure Python, no external ``gpg`` binary.

Chosen deliberately over ``python-gnupg`` (which shells out to a system ``gpg`` binary --
not guaranteed present, or safely invokable across executors, on Databricks serverless
compute): ``PGPy`` is a normal PyPI wheel dependency with no external process/binary
dependency, so it works identically on serverless and classic clusters.

Keys are always resolved via Unity Catalog secrets (see ``crypto/secrets.py``) -- never a
literal key file path or inline key material in onboarding config. All functions here take
already-resolved key material (PEM/ASCII-armored text) as plain arguments; callers are
responsible for resolving the UC secret first.
"""

import logging
from typing import Optional

from flowx.lakeflow_framework.exceptions import CryptoError

logger = logging.getLogger("flowx.lakeflow_framework.crypto.pgp")

try:
    import pgpy

    _PGPY_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised only in environments missing the lib
    _PGPY_AVAILABLE = False


def _require_pgpy() -> None:
    if not _PGPY_AVAILABLE:
        raise CryptoError("PGPy is required for PGP encryption/decryption but is not installed.")


def pgp_decrypt(data: bytes, private_key_armored: str, passphrase: Optional[str] = None) -> bytes:
    """Decrypt a PGP-encrypted binary payload with an ASCII-armored private key.

    Parameters
    ----------
    data:
        The encrypted payload (an OpenPGP message, ASCII-armored or binary).
    private_key_armored:
        The recipient's ASCII-armored PGP private key.
    passphrase:
        The private key's passphrase, if it's passphrase-protected. ``None`` for an
        unprotected key.

    Returns
    -------
    bytes
        The decrypted plaintext.

    Raises
    ------
    CryptoError
        If PGPy is unavailable, the key can't be parsed, the passphrase is wrong, or
        decryption otherwise fails (e.g. the message wasn't encrypted for this key).
    """
    _require_pgpy()
    try:
        private_key, _ = pgpy.PGPKey.from_blob(private_key_armored)
        message = pgpy.PGPMessage.from_blob(data)

        if private_key.is_protected:
            if not passphrase:
                raise CryptoError("PGP private key is passphrase-protected but no passphrase was supplied.")
            with private_key.unlock(passphrase):
                decrypted_message = private_key.decrypt(message)
        else:
            decrypted_message = private_key.decrypt(message)

        payload = decrypted_message.message
        # PGPy returns a `bytearray` (not `bytes`) for a binary ("file=True") message's
        # decrypted payload -- confirmed live, not merely a docs quirk -- while a text
        # message decrypts to a plain `str`. This function's ONLY prior branch handled
        # `bytes` (pass through) or anything else (assumed `str`, `.encode()`'d) --
        # `bytearray` fell into that second branch and blew up with `AttributeError:
        # 'bytearray' object has no attribute 'encode'`. That second branch was
        # previously unreachable by any *working* real-world caller in this framework:
        # `pgp_encrypt` above always builds its message via `PGPMessage.new(data,
        # file=True)`, so every payload this framework itself ever PGP-encrypts (e.g. a
        # ZIP archive for `source_zip_handling.pre_extraction_decryption`) decrypts back
        # to a `bytearray`, not `bytes` -- meaning this exact bug made `pre_extraction_
        # decryption` fail on every real invocation until this fix (see test_specs/
        # spec_25_ingestion_pgp_zip_decrypt.json / tests/integration/
        # test_ingestion_pgp_zip_decrypt.py, which caught this live).
        if isinstance(payload, (bytes, bytearray)):
            return bytes(payload)
        return payload.encode("utf-8")
    except CryptoError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise CryptoError(f"PGP decryption failed: {exc}") from exc


def pgp_encrypt(
    data: bytes,
    recipient_public_key_armored: str,
    sign_with_private_key_armored: Optional[str] = None,
    sign_passphrase: Optional[str] = None,
) -> bytes:
    """Encrypt a binary payload for a recipient's PGP public key, optionally signing it.

    Parameters
    ----------
    data:
        The plaintext payload to encrypt (e.g. a ZIP archive's bytes).
    recipient_public_key_armored:
        The recipient's ASCII-armored PGP public key.
    sign_with_private_key_armored:
        Optional sender's ASCII-armored PGP private key -- when supplied, the message is
        signed before encryption (sign-then-encrypt).
    sign_passphrase:
        The signing key's passphrase, if it's passphrase-protected.

    Returns
    -------
    bytes
        The encrypted (optionally signed) message, ASCII-armored.

    Raises
    ------
    CryptoError
        If PGPy is unavailable, a key can't be parsed, or encryption/signing fails.
    """
    _require_pgpy()
    try:
        recipient_key, _ = pgpy.PGPKey.from_blob(recipient_public_key_armored)
        message = pgpy.PGPMessage.new(data, file=True)

        if sign_with_private_key_armored:
            signing_key, _ = pgpy.PGPKey.from_blob(sign_with_private_key_armored)
            if signing_key.is_protected:
                if not sign_passphrase:
                    raise CryptoError("PGP signing key is passphrase-protected but no sign_passphrase was supplied.")
                with signing_key.unlock(sign_passphrase):
                    message |= signing_key.sign(message)
            else:
                message |= signing_key.sign(message)

        encrypted_message = recipient_key.encrypt(message)
        # Real bug fixed here: PGPy's `bytes(PGPMessage)` serializes to the compact BINARY
        # OpenPGP packet format (no `-----BEGIN PGP MESSAGE-----` armor) -- confirmed live --
        # while `str(PGPMessage)` produces the ASCII-armored text this function's own
        # docstring has always promised. The two are NOT interchangeable for an external
        # recipient: this framework's own `pgp_decrypt` (via PGPy's `from_blob`) happily
        # parses either, so every round-trip test using only this codebase's own encrypt/
        # decrypt pair passed regardless -- but a real external consumer decrypting an
        # exported `.pgp` file (e.g. via a standard `gpg` CLI, or any tool/transport that
        # assumes armored text) could not reliably decrypt the previously-produced binary
        # output. `.encode("utf-8")` is safe here: PGP ASCII armor is pure 7-bit-safe base64
        # + headers.
        return str(encrypted_message).encode("utf-8")
    except CryptoError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise CryptoError(f"PGP encryption failed: {exc}") from exc


def pgp_verify(data: bytes, signed_message: bytes, signer_public_key_armored: str) -> bool:
    """Verify a PGP signature over ``data`` using the signer's ASCII-armored public key.

    Raises
    ------
    CryptoError
        If PGPy is unavailable, the key/message can't be parsed, or verification errors
        (as distinct from a clean "signature does not match" result, which returns
        ``False`` rather than raising).
    """
    _require_pgpy()
    try:
        signer_key, _ = pgpy.PGPKey.from_blob(signer_public_key_armored)
        message = pgpy.PGPMessage.from_blob(signed_message)
        verification = signer_key.verify(message)
        return bool(verification)
    except Exception as exc:  # noqa: BLE001
        raise CryptoError(f"PGP signature verification failed: {exc}") from exc


# ---------------------------------------------------------------------------------------
# Symmetric (passphrase-based) PGP -- added v1.7.4 for UC6.
#
# The four functions above are all ASYMMETRIC: they take ASCII-armored public/private key
# material and encrypt to a recipient's key. That is the right default for a supplier
# integration where each party publishes a key. It is NOT the only shape in the wild: an
# OpenPGP message may instead be protected by a Symmetric-Key Encrypted Session Key (SKESK)
# packet, where a passphrase plus a string-to-key derivation yields the session key directly
# and no keypair exists at all. That is what `gpg --symmetric --cipher-algo AES256` produces,
# and it is what UC6's Environment Agency feed and its two Fujitsu-equivalent outputs use.
#
# Round-tripped against the real GnuPG 2.4.9 CLI in BOTH directions before shipping (not just
# against this module's own encrypt/decrypt pair -- the trap `pgp_encrypt` above documents):
#   * `pgp_decrypt_symmetric` reads a file produced by `gpg --symmetric --cipher-algo AES256`.
#   * `gpg --decrypt` reads a file produced by `pgp_encrypt_symmetric`.
# Both verified with a gzip payload, so the compress-then-encrypt ordering UC6 needs is
# covered rather than assumed.
# ---------------------------------------------------------------------------------------


def pgp_decrypt_symmetric(data: bytes, passphrase: str) -> bytes:
    """Decrypt a passphrase-encrypted (symmetric) PGP payload.

    The counterpart to :func:`pgp_encrypt_symmetric`, and the reader for anything produced by
    ``gpg --symmetric``. No keypair is involved: the passphrase derives the session key via the
    message's own string-to-key specifier, so the cipher and S2K parameters come from the
    message rather than from this call.

    Parameters
    ----------
    data:
        The encrypted payload (an OpenPGP message, ASCII-armored or binary).
    passphrase:
        The shared passphrase. Always resolve this from a Unity Catalog secret at the call
        site (see ``crypto/secrets.py``); never a literal in an onboarding spec.

    Returns
    -------
    bytes
        The decrypted plaintext.

    Raises
    ------
    CryptoError
        If PGPy is unavailable, the payload can't be parsed, the passphrase is wrong, or the
        message is key-encrypted rather than passphrase-encrypted (use :func:`pgp_decrypt`).
    """
    _require_pgpy()
    if not passphrase:
        raise CryptoError("Symmetric PGP decryption requires a passphrase, but an empty one was supplied.")
    try:
        message = pgpy.PGPMessage.from_blob(data)
        decrypted_message = message.decrypt(passphrase)

        payload = decrypted_message.message
        # Same bytearray/str/bytes split `pgp_decrypt` documents at length above: PGPy returns
        # a `bytearray` for a binary ("file=True") message and a `str` for a text one. Both are
        # normalized to `bytes` here so callers never have to care which shape arrived.
        if isinstance(payload, (bytes, bytearray)):
            return bytes(payload)
        return payload.encode("utf-8")
    except CryptoError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise CryptoError(f"Symmetric PGP decryption failed: {exc}") from exc


def pgp_encrypt_symmetric(
    data: bytes,
    passphrase: str,
    cipher: str = "AES256",
) -> bytes:
    """Encrypt a payload under a shared passphrase (symmetric PGP), ASCII-armored.

    Parameters
    ----------
    data:
        The plaintext payload to encrypt (e.g. an already-gzipped CSV export).
    passphrase:
        The shared passphrase, resolved from a Unity Catalog secret by the caller.
    cipher:
        Symmetric cipher name, matching a :class:`pgpy.constants.SymmetricKeyAlgorithm`
        member. Defaults to ``"AES256"`` -- the cipher UC6's interface specification mandates
        and the one ``gpg --symmetric --cipher-algo AES256`` produces.

    Returns
    -------
    bytes
        The encrypted message, ASCII-armored (UTF-8 encoded).

    Raises
    ------
    CryptoError
        If PGPy is unavailable, ``cipher`` names no known algorithm, or encryption fails.

    Notes
    -----
    Returns **armored text**, not the compact binary packet format, for exactly the reason
    recorded in :func:`pgp_encrypt`: ``bytes(PGPMessage)`` serializes to binary while
    ``str(PGPMessage)`` produces the armored form an external ``gpg`` CLI recipient expects.
    ``.encode("utf-8")`` is safe because PGP ASCII armor is 7-bit-safe base64 plus headers.
    """
    _require_pgpy()
    if not passphrase:
        raise CryptoError("Symmetric PGP encryption requires a passphrase, but an empty one was supplied.")
    try:
        from pgpy.constants import SymmetricKeyAlgorithm

        try:
            cipher_algorithm = getattr(SymmetricKeyAlgorithm, cipher)
        except AttributeError as exc:
            known = sorted(member.name for member in SymmetricKeyAlgorithm)
            raise CryptoError(f"Unknown symmetric cipher {cipher!r} for PGP encryption (known: {known}).") from exc

        message = pgpy.PGPMessage.new(data, file=True)
        encrypted_message = message.encrypt(passphrase, cipher=cipher_algorithm)
        return str(encrypted_message).encode("utf-8")
    except CryptoError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise CryptoError(f"Symmetric PGP encryption failed: {exc}") from exc
