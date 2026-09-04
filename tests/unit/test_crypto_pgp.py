"""Unit tests for crypto/pgp.py -- real PGPy round-trips, no live secrets/network needed.

Regression coverage for a real bug found live (via test_specs/spec_25_ingestion_pgp_zip_decrypt.json's
first live run): PGPy returns a ``bytearray`` (not ``bytes``) for a *binary* (``file=True``)
message's decrypted payload -- but ``pgp_encrypt`` (this framework's own encryptor) always
builds messages with ``file=True``, so every payload this framework ever PGP-encrypts (e.g. a
ZIP archive for ``source_zip_handling.pre_extraction_decryption``) previously blew up on
decrypt with ``AttributeError: 'bytearray' object has no attribute 'encode'``. This made
``pre_extraction_decryption`` broken on every real invocation until the fix -- these tests
exercise the real ``pgp_encrypt``/``pgp_decrypt`` pair (no mocking) specifically to catch this
exact class of bug if it ever regresses.
"""

import pytest

from flowx.lakeflow_framework.crypto.pgp import pgp_decrypt, pgp_encrypt, pgp_verify
from flowx.lakeflow_framework.exceptions import CryptoError

pgpy = pytest.importorskip("pgpy")
from pgpy.constants import CompressionAlgorithm, HashAlgorithm, KeyFlags, PubKeyAlgorithm, SymmetricKeyAlgorithm  # noqa: E402


def _generate_test_keypair(passphrase=None):
    key = pgpy.PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 1024)  # small key size -- fast in a unit test
    uid = pgpy.PGPUID.new("Test User", email="test@example.invalid")
    key.add_uid(
        uid,
        usage={KeyFlags.EncryptCommunications, KeyFlags.EncryptStorage, KeyFlags.Sign, KeyFlags.Certify},
        hashes=[HashAlgorithm.SHA256],
        ciphers=[SymmetricKeyAlgorithm.AES256],
        compression=[CompressionAlgorithm.ZIP, CompressionAlgorithm.Uncompressed],
    )
    if passphrase:
        # Real-world PGP private keys are routinely passphrase-protected (this project's own
        # throwaway keypairs above deliberately aren't) -- crypto/pgp.py's passphrase= param
        # exists specifically for this case (readers.py::_decrypt_pgp's passphrase_secret,
        # pgp_zip_sink.py's pgp_sign_passphrase_secret_value). key.protect() locks the private
        # key material the same way a real GPG-generated passphrase-protected key would be.
        key.protect(passphrase, SymmetricKeyAlgorithm.AES256, HashAlgorithm.SHA256)
    return str(key.pubkey), str(key)


@pytest.fixture(scope="module")
def keypair():
    return _generate_test_keypair()


@pytest.fixture(scope="module")
def signing_keypair():
    return _generate_test_keypair()


@pytest.fixture(scope="module")
def passphrase_protected_keypair():
    passphrase = "correct-horse-battery-staple"
    public_key, private_key = _generate_test_keypair(passphrase=passphrase)
    return public_key, private_key, passphrase


def test_binary_payload_round_trips_exactly(keypair):
    """The exact regression this bug caused: pgp_encrypt always builds a binary (file=True)
    message, so this is the realistic case every real caller in this framework hits (a ZIP
    archive's raw bytes)."""
    public_key, private_key = keypair
    original = b"PK\x03\x04 this is not really a zip but it has binary-looking bytes \x00\x01\xff\xfe"
    encrypted = pgp_encrypt(original, public_key)
    decrypted = pgp_decrypt(encrypted, private_key)
    assert decrypted == original
    assert isinstance(decrypted, bytes)


def test_binary_payload_result_type_is_plain_bytes_not_bytearray(keypair):
    """Pin the exact type, not just equality -- a caller (e.g. compress_and_encrypt_sink's
    callers, open(...).write()) may reject a bytearray in a context expecting bytes."""
    public_key, private_key = keypair
    encrypted = pgp_encrypt(b"some binary payload", public_key)
    decrypted = pgp_decrypt(encrypted, private_key)
    assert type(decrypted) is bytes


def test_large_binary_payload_round_trips(keypair):
    """A bigger payload (multi-KB) is more representative of a real ZIP archive than a few
    bytes -- also exercises PGPy's compression path."""
    public_key, private_key = keypair
    original = bytes(range(256)) * 100
    encrypted = pgp_encrypt(original, public_key)
    decrypted = pgp_decrypt(encrypted, private_key)
    assert decrypted == original


def test_encrypt_output_is_ascii_armored_not_raw_binary(keypair):
    """Regression guard for a real bug: pgp_encrypt used to return
    `bytes(pgpy_message)` -- PGPy's compact BINARY OpenPGP serialization -- despite this
    function's own docstring always promising ASCII-armored output. This framework's own
    pgp_decrypt (via PGPy's from_blob, which auto-detects either format) never noticed, so
    every round-trip test using only this codebase's own encrypt/decrypt pair passed
    regardless -- but a real external recipient decrypting an exported .pgp file (e.g. a
    standard `gpg` CLI, or any tool/transport that assumes armored text) could not reliably
    open the previously-produced binary output. Pin the actual wire format, not just the
    round-trip, so this can't silently regress back to binary."""
    public_key, _private_key = keypair
    encrypted = pgp_encrypt(b"some payload", public_key)
    assert encrypted.startswith(b"-----BEGIN PGP MESSAGE-----")
    assert encrypted.rstrip().endswith(b"-----END PGP MESSAGE-----")


def test_decrypt_with_wrong_key_raises_crypto_error(keypair):
    _public_key, _private_key = keypair
    other_public_key, _other_private_key = _generate_test_keypair()
    encrypted = pgp_encrypt(b"secret data", other_public_key)
    with pytest.raises(CryptoError):
        pgp_decrypt(encrypted, keypair[1])


def test_sign_then_encrypt_round_trips_and_verifies(keypair, signing_keypair):
    recipient_public, recipient_private = keypair
    signer_public, signer_private = signing_keypair
    original = b"signed and encrypted payload"

    encrypted = pgp_encrypt(original, recipient_public, sign_with_private_key_armored=signer_private)
    decrypted = pgp_decrypt(encrypted, recipient_private)
    assert decrypted == original


def test_pgp_verify_confirms_a_genuine_signature(signing_keypair):
    signer_public, signer_private = signing_keypair
    key, _ = pgpy.PGPKey.from_blob(signer_private)
    message = pgpy.PGPMessage.new("some text to sign")
    signed_message = key.sign(message)
    message |= signed_message

    assert pgp_verify(b"unused", bytes(message), signer_public) is True


def test_decrypt_with_correct_passphrase_succeeds(passphrase_protected_keypair):
    """The real-world case this framework's passphrase redesign exists for: a private key
    that's genuinely locked, unlocked with the correct passphrase -- see
    ingestion/readers.py::_decrypt_pgp's passphrase_secret and crypto/pgp.py::pgp_decrypt's
    is_protected check."""
    public_key, private_key, passphrase = passphrase_protected_keypair
    original = b"payload behind a passphrase-protected key"
    encrypted = pgp_encrypt(original, public_key)
    decrypted = pgp_decrypt(encrypted, private_key, passphrase=passphrase)
    assert decrypted == original


def test_decrypt_with_wrong_passphrase_raises_crypto_error(passphrase_protected_keypair):
    public_key, private_key, _correct_passphrase = passphrase_protected_keypair
    encrypted = pgp_encrypt(b"payload", public_key)
    with pytest.raises(CryptoError):
        pgp_decrypt(encrypted, private_key, passphrase="definitely-the-wrong-passphrase")


def test_decrypt_of_passphrase_protected_key_without_passphrase_raises_crypto_error(passphrase_protected_keypair):
    """crypto/pgp.py::pgp_decrypt must fail fast and clearly -- not attempt a silent unlock --
    when the key is protected but no passphrase was supplied at all (the
    passphrase_secret-omitted-for-a-protected-key misconfiguration)."""
    public_key, private_key, _passphrase = passphrase_protected_keypair
    encrypted = pgp_encrypt(b"payload", public_key)
    with pytest.raises(CryptoError, match="passphrase"):
        pgp_decrypt(encrypted, private_key)


def test_sign_with_passphrase_protected_signing_key_and_correct_sign_passphrase_verifies(keypair, passphrase_protected_keypair):
    """crypto/pgp.py::pgp_encrypt's sign_passphrase parameter (wired through
    engine/sink_registration.py's sign_passphrase_secret and archive/pgp_zip_sink.py's
    pgp_sign_passphrase_secret_value) -- a real sender signing key is routinely
    passphrase-protected too, independent of the recipient's own key."""
    recipient_public, recipient_private = keypair
    signer_public, signer_private, sign_passphrase = passphrase_protected_keypair
    original = b"signed with a passphrase-protected key, then encrypted"

    encrypted = pgp_encrypt(
        original, recipient_public, sign_with_private_key_armored=signer_private, sign_passphrase=sign_passphrase
    )
    decrypted = pgp_decrypt(encrypted, recipient_private)
    assert decrypted == original


def test_sign_with_passphrase_protected_signing_key_missing_sign_passphrase_raises_crypto_error(keypair, passphrase_protected_keypair):
    recipient_public, _recipient_private = keypair
    _signer_public, signer_private, _sign_passphrase = passphrase_protected_keypair
    with pytest.raises(CryptoError, match="sign_passphrase"):
        pgp_encrypt(b"payload", recipient_public, sign_with_private_key_armored=signer_private)


def test_missing_pgpy_raises_clear_crypto_error(monkeypatch):
    import flowx.lakeflow_framework.crypto.pgp as pgp_module

    monkeypatch.setattr(pgp_module, "_PGPY_AVAILABLE", False)
    with pytest.raises(CryptoError, match="PGPy"):
        pgp_encrypt(b"data", "not-a-real-key")
