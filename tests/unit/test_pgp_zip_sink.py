"""Unit tests for archive/pgp_zip_sink.py -- pure Python + filesystem, no live streaming
sink or Databricks secrets needed.

Mirrors the style in tests/unit/test_asn1_partition_decoder.py: the internal
`_PgpZipStreamWriter` class is exercised directly with small fake data (plain
`pyspark.sql.Row` objects, a local `tmp_path` standing in for a Unity Catalog Volume).
`pgp_encrypt` is monkeypatched (mirroring how `asn1tools.compile_files` is monkeypatched
there) so the PGP-encryption path can be verified without generating real PGP key material.

Every secret value this module consumes arrives as an already-resolved plain string option
(`zip_secret_value`/`pgp_recipient_secret_value`/`pgp_sign_secret_value`) -- see the module
docstring for why `write()`/`commit()` must never call `resolve_secret_ref`/`dbutils`
themselves (confirmed live: that fails from inside the dedicated "python streaming data
source runtime" worker process `commit()`/`abort()` actually execute in). Tests pass those
option values directly rather than monkeypatching any secret-resolution call.

Regression coverage for the design constraint this module's docstring documents at length:
`write()` (executor-side, per partition) and `commit()`/`abort()` (driver-side) are, per the
real PySpark Python Data Source Sink contract, independently (re)constructed writer
instances with no shared Python object state -- the only channel between them is the
`WriterCommitMessage` each `write()` call returns. These tests confirm `commit()` correctly
derives everything it needs (which files to archive, how many rows) purely from the messages
list it's handed, never from any staging-directory-wide glob that could pick up another
batch's leftovers.
"""

import json
import zipfile

import pytest
from pyspark.sql import Row

from NextGen_Metadata_Framework.lakeflow_framework.archive.pgp_zip_sink import (
    PgpZipCommitMessage,
    _PgpZipStreamWriter,
)
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import ArchiveError


def _writer(tmp_path, **extra_options):
    options = {
        "path": str(tmp_path / "staging"),
        "output_zip_path": str(tmp_path / "archives"),
    }
    options.update(extra_options)
    return _PgpZipStreamWriter(options)


# ---------------------------------------------------------------------------
# __init__ / options parsing
# ---------------------------------------------------------------------------


def test_missing_path_option_raises_archive_error(tmp_path):
    with pytest.raises(ArchiveError, match="path"):
        _PgpZipStreamWriter({"output_zip_path": str(tmp_path / "archives")})


def test_missing_output_zip_path_option_raises_archive_error(tmp_path):
    with pytest.raises(ArchiveError, match="output_zip_path"):
        _PgpZipStreamWriter({"path": str(tmp_path / "staging")})


def test_pgp_enabled_without_recipient_secret_raises_archive_error(tmp_path):
    with pytest.raises(ArchiveError, match="pgp_recipient"):
        _writer(tmp_path, pgp_enabled="true")


# ---------------------------------------------------------------------------
# write() -- executor side
# ---------------------------------------------------------------------------


def test_write_stages_rows_as_jsonlines_and_returns_commit_message(tmp_path):
    writer = _writer(tmp_path)
    rows = iter([Row(a=1, b="x"), Row(a=2, b="y")])

    message = writer.write(rows)

    assert isinstance(message, PgpZipCommitMessage)
    assert message.row_count == 2
    assert message.staged_file_path is not None
    with open(message.staged_file_path, encoding="utf-8") as staged_file:
        lines = [json.loads(line) for line in staged_file.read().splitlines()]
    assert lines == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]


def test_write_serializes_non_json_native_values_via_default_str(tmp_path):
    """A row can carry dates/decimals/binary columns json.dumps can't natively serialize --
    default=str must stringify rather than crash the whole microbatch over one column type."""
    import datetime

    writer = _writer(tmp_path)
    message = writer.write(iter([Row(event_date=datetime.date(2026, 1, 1))]))

    with open(message.staged_file_path, encoding="utf-8") as staged_file:
        decoded = json.loads(staged_file.read().splitlines()[0])
    assert decoded["event_date"] == "2026-01-01"


def test_write_empty_partition_removes_the_staged_file_and_reports_zero_rows(tmp_path):
    writer = _writer(tmp_path)
    message = writer.write(iter([]))

    assert message.row_count == 0
    assert message.staged_file_path is None
    staging_dir = tmp_path / "staging"
    assert not staging_dir.exists() or list(staging_dir.iterdir()) == []


def test_write_calls_from_different_partitions_produce_distinct_staged_files(tmp_path):
    """Two write() calls for the same microbatch (one per partition) must never collide on
    the same staged filename -- commit() relies on each commit message naming a distinct
    file."""
    writer = _writer(tmp_path)
    message_1 = writer.write(iter([Row(a=1)]))
    message_2 = writer.write(iter([Row(a=2)]))
    assert message_1.staged_file_path != message_2.staged_file_path


# ---------------------------------------------------------------------------
# commit() -- driver side
# ---------------------------------------------------------------------------


def test_commit_archives_exactly_this_batchs_staged_files_into_a_readable_zip(tmp_path):
    writer = _writer(tmp_path)
    message_1 = writer.write(iter([Row(a=1, b="x")]))
    message_2 = writer.write(iter([Row(a=2, b="y")]))

    writer.commit([message_1, message_2], batchId=7)

    archive_path = tmp_path / "archives" / "batch_7.zip"
    assert archive_path.exists()
    with zipfile.ZipFile(str(archive_path)) as archive:
        assert len(archive.namelist()) == 2
        contents = b"".join(archive.read(name) for name in archive.namelist())
        assert b'"a": 1' in contents
        assert b'"a": 2' in contents

    # The staged files were moved (not copied) into a batch-scoped commit directory and that
    # directory is removed once archived -- nothing should linger under the shared staging
    # root afterward.
    remaining = list((tmp_path / "staging").rglob("*"))
    assert remaining == []


def test_commit_uses_a_custom_export_file_name_format_when_configured(tmp_path):
    writer = _writer(tmp_path, export_file_name_format="sales_export_{batch_id}")
    message = writer.write(iter([Row(a=1)]))

    writer.commit([message], batchId=9)

    assert (tmp_path / "archives" / "sales_export_9.zip").exists()
    assert not (tmp_path / "archives" / "batch_9.zip").exists()


def test_commit_export_file_name_format_supports_a_timestamp_placeholder(tmp_path):
    writer = _writer(tmp_path, export_file_name_format="export_{timestamp}_{batch_id}")
    message = writer.write(iter([Row(a=1)]))

    writer.commit([message], batchId=1)

    matches = list((tmp_path / "archives").glob("export_*_1.zip"))
    assert len(matches) == 1


def test_commit_with_an_unknown_export_file_name_format_placeholder_raises_archive_error(tmp_path):
    writer = _writer(tmp_path, export_file_name_format="batch_{not_a_real_placeholder}")
    message = writer.write(iter([Row(a=1)]))

    with pytest.raises(ArchiveError, match="export_file_name_format"):
        writer.commit([message], batchId=1)


def test_commit_with_only_empty_partitions_is_a_noop(tmp_path):
    writer = _writer(tmp_path)
    empty_message = writer.write(iter([]))  # staged_file_path is None

    writer.commit([empty_message, None], batchId=1)

    assert not (tmp_path / "archives").exists()


def test_commit_two_batches_produce_two_independent_archives(tmp_path):
    """Regression guard for the driver/executor state-sharing constraint this module's
    docstring documents: each commit() call must only ever see (and archive) the messages
    it was explicitly handed for that microbatch, never files from a different batch."""
    writer = _writer(tmp_path)

    batch_1_message = writer.write(iter([Row(a=1)]))
    writer.commit([batch_1_message], batchId=1)

    batch_2_message = writer.write(iter([Row(a=2)]))
    writer.commit([batch_2_message], batchId=2)

    with zipfile.ZipFile(str(tmp_path / "archives" / "batch_1.zip")) as archive:
        assert b'"a": 1' in archive.read(archive.namelist()[0])
    with zipfile.ZipFile(str(tmp_path / "archives" / "batch_2.zip")) as archive:
        assert b'"a": 2' in archive.read(archive.namelist()[0])


def test_commit_pgp_wraps_the_zip_and_removes_the_plaintext_intermediate(tmp_path, monkeypatch):
    calls = {}

    def _fake_pgp_encrypt(data, recipient_public_key_armored, sign_with_private_key_armored=None, sign_passphrase=None):
        calls["encrypt_args"] = (data, recipient_public_key_armored, sign_with_private_key_armored)
        return b"PGP-ENCRYPTED:" + data

    monkeypatch.setattr("NextGen_Metadata_Framework.lakeflow_framework.archive.pgp_zip_sink.pgp_encrypt", _fake_pgp_encrypt)

    # Every secret value arrives pre-resolved -- see the module docstring for why commit()
    # must never call resolve_secret_ref/dbutils itself.
    writer = _writer(
        tmp_path,
        pgp_enabled="true",
        pgp_recipient_secret_value="armored-key-for-recipient_key",
        pgp_sign_secret_value="armored-key-for-sender_key",
    )
    message = writer.write(iter([Row(a=1)]))

    writer.commit([message], batchId=3)

    plain_zip = tmp_path / "archives" / "batch_3.zip"
    encrypted_zip = tmp_path / "archives" / "batch_3.zip.pgp"
    assert not plain_zip.exists(), "the un-encrypted intermediate zip must never be left behind once PGP-wrapped"
    assert encrypted_zip.exists()
    assert encrypted_zip.read_bytes().startswith(b"PGP-ENCRYPTED:PK")  # PK = the zip file magic bytes

    _data, recipient_arg, sign_arg = calls["encrypt_args"]
    assert recipient_arg == "armored-key-for-recipient_key"
    assert sign_arg == "armored-key-for-sender_key"


def test_commit_pgp_passes_the_signing_passphrase_through_when_configured(tmp_path, monkeypatch):
    """Regression coverage: a real signing private key is routinely passphrase-protected
    (unlike this project's own throwaway test keypairs) -- pgp_sign_passphrase_secret_value
    must reach pgp_encrypt's sign_passphrase parameter, only when a signing key is also set."""
    calls = {}

    def _fake_pgp_encrypt(data, recipient_public_key_armored, sign_with_private_key_armored=None, sign_passphrase=None):
        calls["sign_passphrase"] = sign_passphrase
        return b"PGP-ENCRYPTED:" + data

    monkeypatch.setattr("NextGen_Metadata_Framework.lakeflow_framework.archive.pgp_zip_sink.pgp_encrypt", _fake_pgp_encrypt)

    writer = _writer(
        tmp_path,
        pgp_enabled="true",
        pgp_recipient_secret_value="armored-key-for-recipient_key",
        pgp_sign_secret_value="armored-key-for-sender_key",
        pgp_sign_passphrase_secret_value="the-sender-key-passphrase",
    )
    message = writer.write(iter([Row(a=1)]))
    writer.commit([message], batchId=4)

    assert calls["sign_passphrase"] == "the-sender-key-passphrase"


def test_commit_pgp_ignores_a_signing_passphrase_option_when_no_signing_key_is_set(tmp_path, monkeypatch):
    """A pgp_sign_passphrase_secret_value with no pgp_sign_secret_value alongside it is
    rejected by the onboarding validator (see spec_validator.py's pgp_encryption check) --
    but this module defends independently too: __init__ only reads the passphrase option
    when a signing key is actually present (see self._pgp_sign_key_armored gating)."""
    calls = {}

    def _fake_pgp_encrypt(data, recipient_public_key_armored, sign_with_private_key_armored=None, sign_passphrase=None):
        calls["sign_passphrase"] = sign_passphrase
        calls["sign_key"] = sign_with_private_key_armored
        return b"PGP-ENCRYPTED:" + data

    monkeypatch.setattr("NextGen_Metadata_Framework.lakeflow_framework.archive.pgp_zip_sink.pgp_encrypt", _fake_pgp_encrypt)

    writer = _writer(
        tmp_path,
        pgp_enabled="true",
        pgp_recipient_secret_value="armored-key-for-recipient_key",
        pgp_sign_passphrase_secret_value="an-orphaned-passphrase-option",
    )
    message = writer.write(iter([Row(a=1)]))
    writer.commit([message], batchId=5)

    assert calls["sign_key"] is None
    assert calls["sign_passphrase"] is None


# ---------------------------------------------------------------------------
# abort()
# ---------------------------------------------------------------------------


def test_abort_discards_staged_files_referenced_by_the_commit_messages(tmp_path):
    import os

    writer = _writer(tmp_path)
    message = writer.write(iter([Row(a=1)]))
    assert os.path.exists(message.staged_file_path)

    writer.abort([message, None], batchId=9)

    assert not os.path.exists(message.staged_file_path)


def test_abort_tolerates_already_missing_files(tmp_path):
    writer = _writer(tmp_path)
    message = PgpZipCommitMessage(staged_file_path=str(tmp_path / "staging" / "does-not-exist.json"), row_count=1)
    # Must not raise even though the file was never actually created.
    writer.abort([message], batchId=1)
