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
