# #️⃣ Metaflow — Deterministic Hashing Standard

> **Audience**: Pipeline Developers & Data Stewards — anyone who needs to reason about, spot-check, or migrate `__framework_hash_key` or `__framework_hash_value`.

---

> [!IMPORTANT]
> **v1.4.0 — one construction, now with only two consumers of it.** `__framework_surrogate_key`
> and the `crypto/hashing.py` module that produced it are **removed** along with the rest of the
> surrogate-key engine (see [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) §2.7).
> The framework now materializes exactly two hash columns, `__framework_hash_key` and
> `__framework_hash_value`, plus reconciliation's transient batch fingerprint.
>
> Nothing about the *construction* changed in v1.4.0 — the digests below are byte-identical to
> v1.3.0's. What changed is that one of the three things it was used for no longer exists. The
> surrogate key is described in the past tense throughout this document where it explains *why*
> the current design is what it is; it is not a live feature.

## 1. Why One Implementation

Before v1.3.0 the framework computed "the same" hash three separate times, and the three copies had already drifted apart:

* `cdc/hashing.py` — the original `__framework_hash_key` / `__framework_hash_value` construction, coalescing each column to the literal `" NULL "` before hashing, with **no value normalization**.
* `crypto/hashing.py` — a second, independently-maintained copy for the since-removed `__framework_surrogate_key`, over a **hand-maintained exclusion list** that named only three `__framework_*` columns. (This module was deleted in v1.4.0.)
* `reconciliation/appender.py` — a **third** inline copy inside `compute_batch_fingerprint`.

The drift produced three real defects, not just duplication:

1. **No value normalization.** `"ACME "` and `"acme"` hashed to different digests, so a source row and its trivially-equivalent target row were reported as `VALUE_DRIFT`.
2. **A collidable NULL sentinel.** `" NULL "` is a perfectly legal string value — a source column that literally contained the text `" NULL "` was indistinguishable from a true SQL `NULL`.
3. **An incomplete surrogate-key exclusion list.** The old hand-maintained set omitted `__framework_source_file_name` (and several other `__framework_*` columns), so `__framework_surrogate_key` for a `FULL_SNAPSHOT_CDC_NO_PK` flow silently included the ingesting file's name in the hash basis. Re-delivering a byte-identical snapshot under a new filename produced a completely different surrogate key for every row — which `FULL_SNAPSHOT_CDC_NO_PK` then diffed as a full delete-and-reinsert.

v1.3.0 replaces all three constructions with **exactly one implementation**, in [`cdc/hashing.py`](reference/code/cdc.md). `crypto/hashing.py` and `reconciliation/appender.py` import it — nobody rebuilds the expression locally. Drift between hash implementations is a uniquely dangerous class of bug: it fails *silently*. Two sides of a reconciliation, or an ingestion-time hash and a CDC-time hash, simply disagree about what "the same row" hashes to, and nothing raises an error — the framework just reports drift or churn that isn't real. A single shared function is the only way to make that class of bug structurally impossible rather than merely rare.

---

## 2. The Exact Construction

```
sha2( concat_ws('||', coalesce(trim(lower(cast(c1 AS STRING))), '__NULL__'), ... ), 256 )
```

Built column-by-column from `normalized_hash_input(column)`:

```python
F.coalesce(F.trim(F.lower(F.col(column).cast("string"))), F.lit("__NULL__"))
```

and assembled by `deterministic_hash_expression(columns)`:

```python
F.sha2(F.concat_ws("||", *[normalized_hash_input(c) for c in columns]), 256)
```

Reading it left to right:

| Step | Expression | Purpose |
|---|---|---|
| 1. Cast | `cast(col AS STRING)` | Every participating column — regardless of its native type — is compared as text. |
| 2. Fold case | `lower(...)` | `"ACME"` and `"acme"` become the same input. |
| 3. Trim | `trim(...)` | `" ACME"` and `"ACME"` become the same input. |
| 4. NULL sentinel | `coalesce(..., '__NULL__')` | A true SQL `NULL` becomes the literal string `__NULL__`. |
| 5. Join columns | `concat_ws('||', ...)` | Per-column values are joined with `\|\|` into one string. |
| 6. Digest | `sha2(..., 256)` | The joined string is hashed to a 64-character lowercase hex digest. |

**Per-column normalization** (`trim(lower(cast(col AS STRING)))`) is applied identically to every column, in every module that computes a framework hash — CDC (`__framework_hash_key`/`__framework_hash_value`) and reconciliation (fingerprints and inline hashes) alike. There is no per-caller variation.

**Null sentinel ordering — `coalesce` runs *after* normalization, not before.** This order is load-bearing, for two independent reasons:

* *Why the sentinel must come after:* `trim(lower(cast(NULL AS STRING)))` is itself `NULL` — `NULL` propagates through `cast`, `lower`, and `trim` alike. Coalescing **after** those three functions is the only ordering that catches the `NULL` at all; coalescing **before** them would leave the value `NULL` all the way into `concat_ws`, which drops `NULL` arguments entirely rather than treating them as an empty string.
* *Why `__NULL__` cannot collide with a real value:* every real value that reaches the `coalesce` has, by that point, already been lowercased and trimmed — so no real value can ever be uppercase or carry edge whitespace. `__NULL__` is uppercase, and is therefore unreachable by any real, already-normalized value. Coalescing *before* normalization would instead have produced `trim(lower('__NULL__')) == '__null__'`, a lowercase string that a real source value of `"__NULL__"`, `" __null__ "`, or `"__Null__"` collides with exactly. (The pre-v1.3.0 sentinel, `" NULL "`, was collidable for the same reason — it was itself a legal, lowercasable, trimmable string.) The double-underscore spelling also matches the framework's own `__framework_*` reserved-namespace convention.

**Why `concat_ws` needs the coalesce for a second, independent reason.** `concat_ws` silently *skips* `NULL` arguments rather than including them as empty strings. Without the coalesce, a `NULL` column in the middle of a column list would shift every later value one separator to the left — so `(a, NULL, b)` would hash identically to `(a, b)`, silently colliding two rows that are not the same. Coalescing every column to `__NULL__` first guarantees `concat_ws` always sees exactly as many arguments as there are columns.

**Separator:** `||`, via `concat_ws`. **Digest:** `sha2(..., 256)` — a 64-character lowercase hex string, always.

---

## 3. Column Ordering Rules

`deterministic_hash_expression(columns)` hashes `columns` **in exactly the order given — it never sorts.** Sorting, where it happens at all, is entirely the caller's responsibility. This is the detail two independent implementations of "the same" hash are most likely to get wrong, because it is easy to assume the function normalizes order for you. It does not, on purpose: the framework's three hashes want three *different* orders, and a function that quietly sorted would make `__framework_hash_key` unable to distinguish `(customer_id, region)` from `(region, customer_id)`.

| Hash | Ordering | Resolved by |
|---|---|---|
| `__framework_hash_key` | The caller's `primary_keys` order, **not sorted** — the spec author's declared key order is the stable order. | `dq/quarantine.py` passes `target_config.primary_keys` (required for every CDC-dispatched strategy; before v1.4.0 a generated surrogate key could stand in when none were configured); `reconciliation/matcher.py` passes the dataset's configured `match_keys`. |
| `__framework_hash_value` | Alphabetically **sorted**, but the sort happens upstream — `deterministic_hash_expression` itself still doesn't sort. | `cdc/comparison_columns.py::resolve_comparison_columns` already returns a sorted list (`sorted({c for c in base if c not in excluded})`), so `__framework_hash_value` is deterministic regardless of the source DataFrame's physical column order without the hash function doing anything extra. |
| Reconciliation batch fingerprint (`compute_batch_fingerprint`) | The caller's `match_keys` order, per row; the *per-row* digests are then XOR-folded, which is order-**independent across rows** by construction. | `reconciliation/appender.py::compute_batch_fingerprint` passes `match_keys` straight to `deterministic_hash_expression`. |

**Practical consequence:** if you are building a new hash consumer, or hand-verifying a digest, you must know *which* of these orderings you are replicating. Getting the order wrong produces a digest that looks plausible (64 lowercase hex characters) but never matches the framework's own value for the same logical row.

### A parallel design discipline: absent is not the same as empty

The `include_columns` parameter this section used to describe belonged to
`crypto/hashing.py::resolve_surrogate_key_columns`, which is removed. The discipline it illustrated
is still framework-wide and still load-bearing, so it is worth keeping in one sentence:

> A present-but-empty list (`[]`) carries its own meaning and must never be collapsed into "absent".
> Test with `in`, not with `.get(..., default)`, wherever the two differ.

The live example is `source_config.explode_columns`: a present-but-empty `[]` means "auto-flatten
everything", while an absent or null `explode_columns` is a schema-preserving pass-through — see
[`02_ingestion_and_sources.md`](02_ingestion_and_sources.md).

The one place this discipline was *deliberately dropped* in v1.4.0 is
`ingestion/column_normalization.py::resolve_column_normalization`. It used to presence-test
`column_normalization.enabled` so that an object supplying only `case` could defer enablement to the
legacy `normalize_column_names` boolean. With that boolean removed there is no second declaration to
defer to, so the test is now a plain `.get("enabled", False)` — absent means off, exactly as it
reads. The distinction was never the point; having two synonyms was.

---

## 4. Reproducible Spark SQL

Run either of these against any table to reproduce the framework's own digest by hand — useful for spot-checking a reconciliation mismatch or a CDC merge decision without reading any framework code.

```sql
-- __framework_hash_key for primary_keys = (customer_id, region), in that exact order
SELECT
  sha2(
    concat_ws('||',
      coalesce(trim(lower(cast(`customer_id` AS STRING))), '__NULL__'),
      coalesce(trim(lower(cast(`region`      AS STRING))), '__NULL__')
    ),
    256
  ) AS __framework_hash_key
FROM my_catalog.my_schema.my_table;
```

```sql
-- __framework_hash_value for the resolved comparison columns, ALPHABETICALLY SORTED
SELECT
  sha2(
    concat_ws('||',
      coalesce(trim(lower(cast(`email`  AS STRING))), '__NULL__'),
      coalesce(trim(lower(cast(`status` AS STRING))), '__NULL__'),
      coalesce(trim(lower(cast(`tier`   AS STRING))), '__NULL__')
    ),
    256
  ) AS __framework_hash_value
FROM my_catalog.my_schema.my_table;
```

Column order matters (§3): the first query's two `coalesce(...)` lines must appear in `primary_keys` order exactly as configured on the flow; the second query's lines must be in alphabetical order by column name, because that is the order `resolve_comparison_columns` produces.

**When there are no comparison columns at all** (`resolve_comparison_columns` returns an empty list), `__framework_hash_value` is not computed as a zero-column hash — it is set to `CAST(NULL AS STRING)` directly (`F.lit(None).cast("string")` in `compute_hash_columns`). `reconciliation/matcher.py` already treats that case as `hash_equal = True` (key-presence-only matching), so an empty comparison-column set is a supported configuration, not an edge case that needs special SQL to reproduce.

---

## 5. What's Excluded, and Why

A column is excluded from the **comparison-column basis** of `__framework_hash_value` when it is a
framework technical column — resolved by `cdc/comparison_columns.py::resolve_comparison_columns`
against its own explicit `EXCLUDED_TECHNICAL_COLUMNS` tuple, minus the flow's `primary_keys`, and
narrowed further by `columns_to_check` / widened-away by `columns_to_exclude`.

`__framework_hash_key`'s basis is not "excluded from" anything: it is exactly the `primary_keys` the
spec declares, in the order declared.

### The prefix rule, and where it went

The surrogate-key hash used a different and stricter rule: exclude any column whose name starts with
`__framework_`, plus the four non-prefixed technical columns (`_rescued_data`, `_metadata`,
`_object_metadata`, `_asn1_decode_error`), plus the caller's own exclusions. That rule was the fix
for the third defect in §1 — a hand-maintained list rots, a prefix test cannot.

It is gone with the surrogate key in v1.4.0, and it is worth being clear about what that does and
does not mean:

- **It does not weaken `__framework_hash_value`.** That column never used the prefix rule;
  `resolve_comparison_columns` has always maintained its own explicit exclusion tuple, and it is
  unchanged. It still names `__framework_surrogate_key` — deliberately, because a table materialized
  before the upgrade physically carries that column and a comparison set that picked it up would
  compare a stale payload-wide hash against nothing.
- **It removes the defect class along with the feature.** The failure the prefix rule guarded
  against — a volatile technical column leaking into a payload-wide row identity and re-keying an
  entire table — only ever applied to a hash computed over *every* column. No framework hash is
  computed that way any more.

---

## 6. Breaking-Change Migration Note (v1.3.0)

**This is a breaking change with no opt-in flag and no legacy mode.** The `hashing` stream deliberately did not add a per-flow "use the old construction" switch — a legacy-mode toggle would guarantee that two sides of a reconciliation eventually end up computed on *different* constructions, which is exactly the failure mode this consolidation exists to eliminate. Every digest this framework has ever materialized changes value the moment the new wheel is deployed, because the construction changed (normalization added, sentinel changed, surrogate-key exclusion set widened) while the column names did not.

Work through this checklist before/around the first post-upgrade pipeline run:

* **Every existing table's `__framework_hash_key` and `__framework_hash_value` value is stale the instant the new wheel deploys.** These are materialized column values, not view expressions — an existing row keeps its *old* digest until that row is next rewritten.
* **SCD1 / SCD2 / SCD3 targets:** the first update after upgrade recomputes `__framework_hash_value` for every row and finds it differs from the stored value — because it does — and treats every row as changed. For SCD2 specifically, this produces one full extra historical version per row. This is expected, one-time, and should be called out to stakeholders reviewing SCD2 history around the upgrade date.
* **Former `FULL_SNAPSHOT_CDC_NO_PK` targets:** that strategy is removed in v1.4.0 and its diff key with it. Migrating such a flow to `FULL_SNAPSHOT_CDC` with real `primary_keys` re-keys the target by definition, so the first post-migration snapshot is diffed as a complete delete-and-reinsert — one time, expected. See [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) §2.7.
* **Reconciliation flows with `hash_precomputed: true`:** the target side's stored hashes were computed with the *old* construction, while the source side (and any non-precomputed target) is hashed inline with the *new* one on every run — so the first post-upgrade comparison reports drift on effectively every row. **Required operator action:** before the first post-upgrade reconciliation run, either re-materialize the target through its own pipeline (so its stored hashes are recomputed with the new construction), or temporarily set `hash_precomputed: false` for that side so it is hashed inline instead of trusting the stale stored value. See [`07_reconciliation_engine.md`](07_reconciliation_engine.md) for the full `hash_precomputed` configuration.
* **`reconciliation_run_log.source_batch_fingerprint` history stops matching.** The batch fingerprint construction changed along with everything else, so the first post-upgrade reconciliation run cannot short-circuit to `SKIPPED_ALREADY_PROCESSED` against a fingerprint recorded before the upgrade — it simply re-evaluates once. If the underlying miss set is genuinely empty this appends nothing; the practical cost is one extra full comparison, not duplicate appends or incorrect results.
* **Net improvement — but read the flip side.** Values that differ only by letter case or edge whitespace (`"ACME "` vs. `"acme"`) now hash equal, eliminating a class of false `VALUE_DRIFT` reports that existed under the old, unnormalized construction. The intended flip side of that same fix: two values differing *only* by case or whitespace are now treated by the framework as **the same row**. If a table's business logic actually depends on `"ACME"` and `"acme"` being distinct rows, that distinction is gone at the hash layer — this is deliberate, not a defect, and should be understood before relying on hash-based matching for such a column.

---

## 7. Which Modules Share This Implementation

| Module | What it imports / defines | Role |
|---|---|---|
| `cdc/hashing.py` | Defines `normalized_hash_input`, `deterministic_hash_expression`, `compute_hash_columns`, `FINGERPRINT_LANES`, `xor_fold_hex_digest`, `assemble_xor_folded_digest`. Imports nothing from the rest of the framework (only `typing` and `pyspark`). | **The single canonical implementation.** Computes `__framework_hash_key`/`__framework_hash_value` directly; supplies the XOR-fold helpers reconciliation's fingerprinting (§4, §6) and restartability check both build on. |
| `reconciliation/appender.py` | Imports `deterministic_hash_expression`, `xor_fold_hex_digest`, `assemble_xor_folded_digest` for `compute_batch_fingerprint`. | Builds the restartability fingerprint over a run's miss set, used to short-circuit a re-run to `SKIPPED_ALREADY_PROCESSED` (see §6). |
| `reconciliation/matcher.py` | Imports `xor_fold_hex_digest`, `assemble_xor_folded_digest` (for the Phase 1 per-side fingerprint over `__framework_hash_key`/`__framework_hash_value`), and `compute_hash_columns` directly (to hash a side inline when that dataset's `hash_precomputed` is `false`). | Consumes the shared construction on both the cheap Phase 1 fingerprint path and the Phase 2 hash-join path. See [`07_reconciliation_engine.md`](07_reconciliation_engine.md) for the two-tier verification flow. |
| `dq/quarantine.py` | Imports `compute_hash_columns` directly. | The single call site that materializes `__framework_hash_key`/`__framework_hash_value` onto a CDC-dispatched flow's clean upstream (`_apply_hash_columns`, renamed from `_apply_hash_and_surrogate_key_columns` in v1.4.0), before it reaches `dlt.apply_changes`/`apply_changes_from_snapshot`. See [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md). |

**Import direction is one-way.** `cdc/hashing.py` depends on nothing else in the framework. `reconciliation/*` and `dq/quarantine.py` import *from* `cdc/hashing.py` — never the reverse. There is exactly one place this construction can be edited, and every consumer is guaranteed to pick up a change to it automatically.

---

## See Also

* [`02_ingestion_and_sources.md`](02_ingestion_and_sources.md) — the `explode_columns` absent-vs-present-but-empty distinction, the same design discipline referenced in §3.
* [`03_transformation_and_cdc.md`](03_transformation_and_cdc.md) — CDC strategy configuration, `generate_hash_columns`, and §2.7 on the removal of `FULL_SNAPSHOT_CDC_NO_PK` and the surrogate-key engine.
* [`07_reconciliation_engine.md`](07_reconciliation_engine.md) — §9's exact `hash_precomputed` mechanics (it is an assertion that these columns already exist, never an instruction to compute them), the two-tier (Phase 1 fingerprint / Phase 2 hash-join) verification flow, and the reconciliation-specific migration consequence in §6 above.
* [`10_multi_role_faqs.md`](10_multi_role_faqs.md) — Developer/Data Architect/PM FAQ across the framework generally; it does not currently include a hashing-specific summary of the §6 migration checklist.
* [`00_master_reference_index.md`](00_master_reference_index.md) — the `__framework_hash_key`/`__framework_hash_value` rows in the Framework-Generated Columns reference.
