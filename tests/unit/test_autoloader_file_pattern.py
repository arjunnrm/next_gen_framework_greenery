"""``file_pattern`` must reach Spark as ``pathGlobFilter`` -- for every format.

This file exists because of a live failure, and the regression it guards is subtle enough to
be worth stating plainly. ``TC-ING-004`` is the *only* spec in ``metaflow_testing/`` that sets
``file_pattern``, so for a long time exactly one code path exercised this option and the other
one -- the branch every csv/json/parquet/avro flow would take -- was never executed by anything,
live or unit. It emitted ``cloudFiles.fileNamePattern``, which Auto Loader rejects outright:

    [CF_UNKNOWN_OPTION_KEYS_ERROR] Found unknown option keys: cloudFiles.filenamepattern

Auto Loader validates every ``cloudFiles.``-prefixed key against a closed whitelist, and that
check never consults ``cloudFiles.format`` -- so the prefixed spelling is wrong for ordinary
formats exactly as it is for ``binaryFile``. The correct key is the un-prefixed, generic
file-source option ``pathGlobFilter``, honoured by ``cloudFiles`` and ``binaryFile`` alike.

These tests use a recording double rather than a real reader precisely so they run in the
ordinary unit suite with no Spark session -- the live pipeline run that would have caught this
costs minutes and a workspace, which is why the bug survived to production in the first place.
"""

from NextGen_Metadata_Framework.lakeflow_framework.ingestion.readers import (
    _apply_common_autoloader_options,
)


class _RecordingReader:
    """Minimal stand-in for a Spark DataFrameReader that records every ``.option()`` call."""

    def __init__(self):
        self.options = {}

    def option(self, key, value):
        self.options[key] = value
        return self


def test_file_pattern_maps_to_path_glob_filter():
    reader = _apply_common_autoloader_options(_RecordingReader(), {"file_pattern": "*.ber"})
    assert reader.options == {"pathGlobFilter": "*.ber"}


def test_file_pattern_never_emits_a_cloudfiles_prefixed_key():
    """The specific spelling Auto Loader rejects must not reappear under any format."""
    for fmt in ("csv", "json", "parquet", "avro", "text", "binaryFile"):
        reader = _apply_common_autoloader_options(
            _RecordingReader(), {"format": fmt, "file_pattern": "orders_*.csv"}
        )
        assert reader.options["pathGlobFilter"] == "orders_*.csv"
        assert not any(key.lower().startswith("cloudfiles.") for key in reader.options), (
            f"format {fmt!r} emitted a cloudFiles.-prefixed file-selection key: {reader.options}"
        )


def test_absent_file_pattern_sets_no_selection_option():
    """An omitted file_pattern must not silently narrow the ingested file set."""
    reader = _apply_common_autoloader_options(_RecordingReader(), {"format": "csv"})
    assert reader.options == {}


def test_reader_options_pass_through_alongside_file_pattern():
    reader = _apply_common_autoloader_options(
        _RecordingReader(),
        {"file_pattern": "*.csv", "reader_options": {"header": "true", "delimiter": "|"}},
    )
    assert reader.options == {"pathGlobFilter": "*.csv", "header": "true", "delimiter": "|"}
