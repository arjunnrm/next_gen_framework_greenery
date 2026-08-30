"""Unit tests for archive/zip_utils.py::compress_and_encrypt_sink -- pure Python for the
unencrypted path (spark is only touched when a UC secret is configured).

Regression coverage for the in-memory-buffer fix: building the archive against a real
Unity Catalog Volume path directly used to fail with `OSError: [Errno 5] Input/output
error` (Volumes' FUSE mount doesn't support seeking on a file opened for write, which the
ZIP container format needs). These tests can't reproduce that Volumes-specific I/O
failure against a local filesystem (which is always seekable) -- what they verify is that
the fixed implementation (build in `io.BytesIO()`, write out once) still produces a
correct, readable archive with the right contents.
"""

import zipfile

import pytest

from NextGen_Metadata_Framework.lakeflow_framework.archive.zip_utils import compress_and_encrypt_sink
from NextGen_Metadata_Framework.lakeflow_framework.exceptions import ArchiveError


def test_compresses_eligible_files_into_a_readable_zip(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    # write_bytes, not write_text: Windows text-mode writes translate \n -> \r\n, which
    # would make the exact-bytes assertion below platform-dependent.
    (source_dir / "part-0001.csv").write_bytes(b"a,b\n1,2\n")
    (source_dir / "part-0002.csv").write_bytes(b"a,b\n3,4\n")
    (source_dir / "_SUCCESS").write_bytes(b"")  # Spark marker file -- must be excluded

    output_zip = tmp_path / "output.zip"
    result_path = compress_and_encrypt_sink(spark=None, source_dir=str(source_dir), output_zip_path=str(output_zip))

    assert result_path == str(output_zip)
    with zipfile.ZipFile(str(output_zip)) as archive:
        names = set(archive.namelist())
        assert names == {"part-0001.csv", "part-0002.csv"}
        assert archive.read("part-0001.csv") == b"a,b\n1,2\n"


def test_raises_archive_error_when_source_dir_missing(tmp_path):
    with pytest.raises(ArchiveError, match="does not exist"):
        compress_and_encrypt_sink(spark=None, source_dir=str(tmp_path / "missing"), output_zip_path=str(tmp_path / "out.zip"))


def test_raises_archive_error_when_no_eligible_files(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    (source_dir / "_SUCCESS").write_text("")
    with pytest.raises(ArchiveError, match="No eligible data files"):
        compress_and_encrypt_sink(spark=None, source_dir=str(source_dir), output_zip_path=str(tmp_path / "out.zip"))
