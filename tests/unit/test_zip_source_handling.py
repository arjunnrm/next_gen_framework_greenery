"""Unit tests for ``ingestion/readers.py::_apply_source_zip_handling`` -- pure filesystem +
zipfile, no live Spark session or Databricks secrets needed (every scenario below omits
``secret``/``pre_extraction_decryption``, so ``_resolve_optional_passphrase`` never reaches
into ``spark``; see ``archive/zip_utils.py``).

Regression coverage for the v2 redesign: ``source_zip_handling.source_zip_path`` is a
*directory*, never a single file -- a real landing zone routinely accumulates more than one
archive between pipeline updates (e.g. several sibling ingestion flows sharing one incoming
folder, each picking out only its own file by name -- see
``test_specs/spec_16_bt_uc002_archive_autoloader_egress.json``). ``zip_file_pattern`` (a glob,
matched the same convention as ``file_pattern``/``pathGlobFilter``) selects which
archive(s) in that directory get extracted on a given pipeline update.
"""

import io
import os
import zipfile

import pytest

from flowx.lakeflow_framework.exceptions import ArchiveError, FrameworkConfigError
from flowx.lakeflow_framework.ingestion import readers as readers_module
from flowx.lakeflow_framework.ingestion.readers import _apply_source_zip_handling


def _make_zip(path, member_name="data.csv", content=b"a,b\n1,2\n"):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member_name, content)


def _zip_handling(tmp_path, **overrides):
    config = {
        "enabled": True,
        "source_zip_path": str(tmp_path / "incoming"),
        "zip_file_pattern": "*.zip",
        "target_volume_path": str(tmp_path / "extracted"),
    }
    config.update(overrides)
    return {"source_zip_handling": config}


def test_disabled_zip_handling_is_a_noop(tmp_path):
    source_config = {"source_zip_handling": {"enabled": False, "source_zip_path": str(tmp_path)}}
    _apply_source_zip_handling(None, source_config)  # must not touch spark or the filesystem


def test_missing_zip_handling_block_is_a_noop(tmp_path):
    _apply_source_zip_handling(None, {})


def test_missing_zip_file_pattern_raises_framework_config_error(tmp_path):
    source_config = {
        "source_zip_handling": {
            "enabled": True,
            "source_zip_path": str(tmp_path / "incoming"),
            "target_volume_path": str(tmp_path / "extracted"),
        }
    }
    with pytest.raises(FrameworkConfigError, match="zip_file_pattern"):
        _apply_source_zip_handling(None, source_config)


def test_missing_source_directory_is_tolerated_as_already_extracted(tmp_path):
    """A directory that doesn't exist at all (never landed / already cleaned up by a prior
    update) is expected steady state, not an error -- mirrors the single-file idempotency
    tolerance this replaced."""
    source_config = _zip_handling(tmp_path)  # tmp_path / "incoming" is never created
    _apply_source_zip_handling(None, source_config)  # must not raise


def test_directory_with_no_matching_files_is_tolerated_as_already_extracted(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "readme.txt").write_text("not a zip")
    source_config = _zip_handling(tmp_path)
    _apply_source_zip_handling(None, source_config)  # must not raise -- no *.zip present


def test_single_matching_file_is_extracted_and_removed(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    zip_path = incoming / "batch.zip"
    _make_zip(str(zip_path))

    source_config = _zip_handling(tmp_path)
    _apply_source_zip_handling(None, source_config)

    extracted = tmp_path / "extracted" / "data.csv"
    assert extracted.exists()
    assert not zip_path.exists(), "delete_source_after_extract defaults to True"


def test_only_files_matching_the_pattern_are_extracted(tmp_path):
    """The BT_Group real-world case (spec_16): several archives share one incoming directory,
    each flow's own pattern must extract only its own file, leaving siblings untouched."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _make_zip(str(incoming / "sales_north.zip"), content=b"north")
    _make_zip(str(incoming / "sales_south.zip"), content=b"south")

    source_config = _zip_handling(tmp_path, zip_file_pattern="sales_north.zip")
    _apply_source_zip_handling(None, source_config)

    assert not (incoming / "sales_north.zip").exists()
    assert (incoming / "sales_south.zip").exists(), "a non-matching sibling archive must be left alone"
    assert (tmp_path / "extracted" / "data.csv").exists()


def test_wildcard_pattern_extracts_every_matching_archive_in_one_update(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _make_zip(str(incoming / "part_1.zip"), member_name="part_1.csv")
    _make_zip(str(incoming / "part_2.zip"), member_name="part_2.csv")
    (incoming / "unrelated.txt").write_text("ignore me")

    source_config = _zip_handling(tmp_path, zip_file_pattern="part_*.zip")
    _apply_source_zip_handling(None, source_config)

    extracted = tmp_path / "extracted"
    assert (extracted / "part_1.csv").exists()
    assert (extracted / "part_2.csv").exists()
    assert not (incoming / "part_1.zip").exists()
    assert not (incoming / "part_2.zip").exists()
    assert (incoming / "unrelated.txt").exists(), "a non-.zip file must never be touched"


def test_delete_source_after_extract_false_leaves_the_archive_in_place(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    zip_path = incoming / "batch.zip"
    _make_zip(str(zip_path))

    source_config = _zip_handling(tmp_path, delete_source_after_extract=False)
    _apply_source_zip_handling(None, source_config)

    assert zip_path.exists()
    assert (tmp_path / "extracted" / "data.csv").exists()


def test_rerun_after_deletion_finds_nothing_left_to_extract(tmp_path):
    """Idempotency across repeated pipeline updates: once every matched archive is gone, a
    second call is a silent no-op, not an error."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _make_zip(str(incoming / "batch.zip"))
    source_config = _zip_handling(tmp_path)

    _apply_source_zip_handling(None, source_config)
    _apply_source_zip_handling(None, source_config)  # must not raise the second time


def test_source_zip_path_pointing_at_a_file_raises_framework_config_error(tmp_path):
    """A path that EXISTS but isn't a directory (e.g. an un-migrated v1-style spec that still
    names a single ZIP file, or a typo appending a filename) is a real misconfiguration, not
    "not yet landed" -- must not be silently swallowed by the same tolerance branch that
    handles a directory that simply hasn't been created yet (test above)."""
    not_a_directory = tmp_path / "incoming"
    not_a_directory.write_text("this is a file, not a directory")
    source_config = _zip_handling(tmp_path)

    with pytest.raises(FrameworkConfigError, match="not a directory"):
        _apply_source_zip_handling(None, source_config)


def test_zip_file_pattern_matching_is_case_sensitive_regardless_of_host_os(tmp_path):
    """Regression guard: fnmatch.fnmatch case-normalizes per the LOCAL os (case-insensitive on
    Windows, case-sensitive on Linux -- where Databricks compute actually runs), which would
    make a pattern that "works" in local Windows testing silently match zero files in
    production. fnmatchcase is used instead specifically so behavior is identical everywhere
    -- proven here by a differently-cased pattern that must NOT match, even though this test
    itself may run on a case-insensitive filesystem."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _make_zip(str(incoming / "SALES_NORTH.ZIP"))

    source_config = _zip_handling(tmp_path, zip_file_pattern="sales_north.zip")
    _apply_source_zip_handling(None, source_config)  # must NOT raise -- no case-insensitive match, tolerated as "nothing to do"

    assert (incoming / "SALES_NORTH.ZIP").exists(), "the differently-cased file must be left untouched, not matched"


def test_unreadable_directory_raises_archive_error_not_a_raw_os_error(tmp_path, monkeypatch):
    """os.listdir on the landing directory is unguarded against OSError/PermissionError (e.g.
    a Volume ACL issue) -- must be wrapped as the documented ArchiveError contract, not leak a
    raw OSError past this function's callers."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    source_config = _zip_handling(tmp_path)

    def _raise_permission_error(_path):
        raise PermissionError("Access is denied")

    monkeypatch.setattr(readers_module.os, "listdir", _raise_permission_error)

    with pytest.raises(ArchiveError):
        _apply_source_zip_handling(None, source_config)


def test_one_corrupt_archive_does_not_block_extraction_of_the_others_in_the_same_batch(tmp_path):
    """Regression guard: the loop over matched files must attempt every match even after one
    fails -- a single corrupt archive must not silently prevent its valid siblings (sorted
    after it) from being extracted in the same pipeline update. The function still raises at
    the end (the documented ArchiveError contract), but only after every match was tried."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "part_1_corrupt.zip").write_bytes(b"not a real zip file at all")
    _make_zip(str(incoming / "part_2_valid.zip"), member_name="part_2.csv")

    source_config = _zip_handling(tmp_path, zip_file_pattern="part_*.zip")

    with pytest.raises(ArchiveError):
        _apply_source_zip_handling(None, source_config)

    assert (tmp_path / "extracted" / "part_2.csv").exists(), "the valid sibling archive must still have been extracted"
    assert not (incoming / "part_2_valid.zip").exists(), "the valid archive was successfully processed and removed"


def test_a_corrupt_archive_is_not_deleted_after_a_failed_extraction(tmp_path):
    """Regression guard: on a FAILED extraction, the source archive must be preserved (so the
    operator can inspect/retry it), even though delete_source_after_extract defaults to True
    for the success path. Deleting the only copy of a file right as extraction fails would
    make the failure unrecoverable."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    corrupt_zip = incoming / "corrupt.zip"
    corrupt_zip.write_bytes(b"not a real zip file at all")
    source_config = _zip_handling(tmp_path, zip_file_pattern="corrupt.zip")

    with pytest.raises(ArchiveError):
        _apply_source_zip_handling(None, source_config)

    assert corrupt_zip.exists(), "the source archive must still be present after a failed extraction, for retry"


def test_original_pre_decryption_file_is_not_deleted_when_the_decrypted_zip_fails_to_extract(tmp_path, monkeypatch):
    """The exact bug this guards: with pre_extraction_decryption configured, the ORIGINAL
    (pre-decryption) source file is a distinct artifact from the decrypted `.decrypted`
    temp file -- only the temp file's cleanup may run unconditionally; deleting the original
    when the decrypted content turns out not to be a valid ZIP would destroy the only
    remaining copy right as the failure propagates, with no way to retry."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    original_source = incoming / "batch.zip.pgp"
    original_source.write_bytes(b"pretend-pgp-ciphertext")

    # Stub the "pgp" pre-extraction-decryption handler so it "succeeds" but produces bytes
    # that are not a valid ZIP -- forcing extract_encrypted_zip to fail on the *decrypted*
    # file, which is exactly the failure mode the original bug mishandled.
    monkeypatch.setitem(
        readers_module._PRE_EXTRACTION_DECRYPTION_HANDLERS, "pgp", lambda spark, data, config: b"not a valid zip"
    )

    source_config = _zip_handling(
        tmp_path,
        zip_file_pattern="batch.zip.pgp",
        pre_extraction_decryption={"type": "pgp", "private_key_secret": {"secret_catalog": "c", "secret_schema": "s", "secret_key": "k"}},
    )

    with pytest.raises(ArchiveError):
        _apply_source_zip_handling(None, source_config)

    assert original_source.exists(), "the original pre-decryption file must survive a failed extraction of the decrypted content"
    assert not (incoming / "batch.zip.pgp.decrypted").exists(), "the decrypted temp intermediate must still be cleaned up either way"


# --- pre_extraction_decryption reshape coverage --------------------------------------------
#
# ``source_zip_handling.secret`` (a top-level AES ZIP-archive password) was removed; the
# password now lives at ``pre_extraction_decryption.secret_passphrase`` (same 3-level UC
# secret-ref shape), independent of and combinable with ``pre_extraction_decryption.type``
# (now itself optional, previously required whenever ``pre_extraction_decryption`` was present
# at all). The three tests below pin down each of the resulting independent states by
# monkeypatching ``extract_encrypted_zip`` at the module level it's imported into (mirroring
# the ``os.listdir``/``_PRE_EXTRACTION_DECRYPTION_HANDLERS`` monkeypatches above) so the
# secret_catalog/secret_schema/secret_key actually threaded through can be asserted without a
# live Spark session or real Databricks secret.


def test_pre_extraction_decryption_omitted_is_a_plain_unzip_with_no_password(tmp_path, monkeypatch):
    """``pre_extraction_decryption`` entirely absent -> no PGP handler dispatch (no ``.decrypted``
    temp file ever created) and ``extract_encrypted_zip`` called with every secret_* arg None."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    zip_path = incoming / "batch.zip"
    _make_zip(str(zip_path))

    captured = {}

    def _fake_extract_encrypted_zip(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(readers_module, "extract_encrypted_zip", _fake_extract_encrypted_zip)

    source_config = _zip_handling(tmp_path)  # no pre_extraction_decryption key at all
    _apply_source_zip_handling(None, source_config)

    assert captured["source_zip_path"] == str(zip_path), "no decryption layer -> the original file is handed straight to extract_encrypted_zip"
    assert captured["secret_catalog"] is None
    assert captured["secret_schema"] is None
    assert captured["secret_key"] is None
    assert not (incoming / "batch.zip.decrypted").exists(), "no pre-extraction decryption handler should ever run"


def test_pre_extraction_decryption_empty_dict_is_a_plain_unzip_with_no_password(tmp_path, monkeypatch):
    """``pre_extraction_decryption: {}`` behaves identically to omitting it entirely."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    zip_path = incoming / "batch.zip"
    _make_zip(str(zip_path))

    captured = {}

    def _fake_extract_encrypted_zip(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(readers_module, "extract_encrypted_zip", _fake_extract_encrypted_zip)

    source_config = _zip_handling(tmp_path, pre_extraction_decryption={})
    _apply_source_zip_handling(None, source_config)

    assert captured["secret_catalog"] is None
    assert captured["secret_schema"] is None
    assert captured["secret_key"] is None


def test_secret_passphrase_only_no_type_skips_pgp_branch_but_passes_password_through(tmp_path, monkeypatch):
    """``secret_passphrase`` with no ``type`` sibling -> "just a password-protected ZIP, no
    outer decryption layer": the PGP handler registry must never be consulted (no ``.decrypted``
    temp file), yet the resolved secret_catalog/secret_schema/secret_key must still reach
    ``extract_encrypted_zip`` so the ZIP's own AES password is actually used."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    zip_path = incoming / "batch.zip"
    _make_zip(str(zip_path))

    captured = {}

    def _fake_extract_encrypted_zip(**kwargs):
        captured.update(kwargs)
        return []

    def _fail_if_called(*args, **kwargs):
        raise AssertionError("the pgp handler must not be invoked when 'type' is absent")

    monkeypatch.setattr(readers_module, "extract_encrypted_zip", _fake_extract_encrypted_zip)
    monkeypatch.setitem(readers_module._PRE_EXTRACTION_DECRYPTION_HANDLERS, "pgp", _fail_if_called)

    source_config = _zip_handling(
        tmp_path,
        pre_extraction_decryption={
            "secret_passphrase": {"secret_catalog": "cat", "secret_schema": "sec", "secret_key": "zip_password"}
        },
    )
    _apply_source_zip_handling(None, source_config)

    assert captured["source_zip_path"] == str(zip_path), "no 'type' -> no decrypted intermediate, the original file is extracted directly"
    assert captured["secret_catalog"] == "cat"
    assert captured["secret_schema"] == "sec"
    assert captured["secret_key"] == "zip_password"
    assert not (incoming / "batch.zip.decrypted").exists()


def test_pre_extraction_decryption_type_only_no_secret_passphrase_decrypts_then_extracts_unprotected_zip(tmp_path, monkeypatch):
    """``type`` alone (no ``secret_passphrase``) -> unchanged v1 behavior: the PGP handler
    decrypts the envelope, and the resulting (unprotected) ZIP is extracted with no archive
    password at all -- confirmed for real via ``extract_encrypted_zip`` (not mocked here),
    since a plain zip needs no secret resolution and so needs no live Spark session either."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    original_source = incoming / "batch.zip.pgp"
    original_source.write_bytes(b"pretend-pgp-ciphertext")

    plain_zip_bytes = io.BytesIO()
    with zipfile.ZipFile(plain_zip_bytes, "w") as archive:
        archive.writestr("data.csv", "a,b\n1,2\n")

    monkeypatch.setitem(
        readers_module._PRE_EXTRACTION_DECRYPTION_HANDLERS,
        "pgp",
        lambda spark, data, config: plain_zip_bytes.getvalue(),
    )

    source_config = _zip_handling(
        tmp_path,
        zip_file_pattern="batch.zip.pgp",
        pre_extraction_decryption={"type": "pgp", "private_key_secret": {"secret_catalog": "c", "secret_schema": "s", "secret_key": "k"}},
    )
    _apply_source_zip_handling(None, source_config)

    assert (tmp_path / "extracted" / "data.csv").exists(), "the PGP-decrypted, unprotected ZIP must still extract with no password"
    assert not original_source.exists(), "delete_source_after_extract defaults to True"
    assert not (incoming / "batch.zip.pgp.decrypted").exists(), "the decrypted temp intermediate must be cleaned up after success too"


# ---------------------------------------------------------------------------------------
# member_format: "gzip" -- v1.7.4, added for UC6's Environment Agency feed.
#
# The whole of source_zip_handling above assumes a ZIP *archive*: a container with N named
# members. A gzip file is a single compressed stream with no member table, which pyzipper
# cannot open at all. These tests cover the container dispatch and, most importantly, the
# landed FILENAME -- which is what Auto Loader's file_pattern glob subsequently matches, so a
# wrong name means a silent zero-row ingest rather than a loud failure.
# ---------------------------------------------------------------------------------------

import gzip as _gzip  # noqa: E402


def _make_gzip(path, content=b"a|b\n1|2\n"):
    with _gzip.open(path, "wb") as handle:
        handle.write(content)


def test_gzip_member_format_decompresses_into_the_landing_path(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _make_gzip(incoming / "CSS_account_20250127_00000008.dat.gz", b"S|2873|Henley\n")

    _apply_source_zip_handling(
        None,
        _zip_handling(tmp_path, zip_file_pattern="CSS_account_[0-9]*.dat.gz", member_format="gzip"),
    )

    landed = tmp_path / "extracted" / "CSS_account_20250127_00000008.dat"
    assert landed.exists(), "the .gz suffix must be stripped -- file_pattern globs match the landed name"
    assert landed.read_bytes() == b"S|2873|Henley\n"


def test_gzip_member_format_strips_the_encryption_envelope_from_the_landed_name(tmp_path):
    """UC6's real shape: <stem>.csv.gz.gpg decrypted to <stem>.csv.gz.gpg.decrypted.

    Regression test for a bug caught by a live run against UC6's own fixture: only
    ``.decrypted`` was stripped, so the name still ended ``.gpg``, the ``.gz`` test missed, and
    the file landed as ``EE_...csv.gz.gpg.decompressed`` -- matching no glob any spec would
    write.
    """
    from flowx.lakeflow_framework.archive.zip_utils import _decompressed_member_name

    assert _decompressed_member_name("/l/EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg.decrypted") == "EE_2026-08-20-REQUEST_1OF1.csv"
    assert _decompressed_member_name("/l/x.csv.gz.pgp.decrypted") == "x.csv"
    assert _decompressed_member_name("/l/CSS_account_1.dat.gz") == "CSS_account_1.dat"
    assert _decompressed_member_name("/l/y.CSV.GZ") == "y.CSV", "suffix match must be case-insensitive"
    assert _decompressed_member_name("/l/z.dat") == "z.dat.decompressed", "never collide with the source name"


def test_gzip_member_format_rejects_a_zip_only_secret_passphrase(tmp_path):
    """`secret_passphrase` is an AES password on a ZIP; a gzip stream has no password.

    Accepting it silently would let a spec assert protection that does not exist.
    """
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _make_gzip(incoming / "data.dat.gz")

    config = _zip_handling(
        tmp_path,
        zip_file_pattern="*.dat.gz",
        member_format="gzip",
        pre_extraction_decryption={
            "secret_passphrase": {"secret_catalog": "c", "secret_schema": "s", "secret_key": "k"}
        },
    )
    # FrameworkConfigError, not ArchiveError: this is a misconfigured spec, not a corrupt
    # file, and _extract_one_zip_file deliberately re-raises it undisguised.
    with pytest.raises(FrameworkConfigError, match="gzip"):
        _apply_source_zip_handling(None, config)


def test_unknown_member_format_is_rejected(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _make_gzip(incoming / "data.dat.gz")

    config = _zip_handling(tmp_path, zip_file_pattern="*.dat.gz", member_format="tar")
    with pytest.raises(FrameworkConfigError, match="member_format"):
        _apply_source_zip_handling(None, config)


def test_member_format_defaults_to_zip(tmp_path):
    """Absent member_format must behave exactly as before v1.7.4 -- no silent change."""
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    _make_zip(incoming / "orders.zip", member_name="orders.csv")

    _apply_source_zip_handling(None, _zip_handling(tmp_path, zip_file_pattern="*.zip"))

    assert (tmp_path / "extracted" / "orders.csv").exists()


def test_gzip_member_format_reports_a_corrupt_stream(tmp_path):
    incoming = tmp_path / "incoming"
    incoming.mkdir()
    (incoming / "broken.dat.gz").write_bytes(b"this is not gzip at all")

    config = _zip_handling(tmp_path, zip_file_pattern="*.dat.gz", member_format="gzip")
    with pytest.raises(ArchiveError, match="broken.dat.gz"):
        _apply_source_zip_handling(None, config)
