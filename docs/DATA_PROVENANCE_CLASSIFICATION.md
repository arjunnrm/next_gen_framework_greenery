# Data Provenance Classification — v0.0.4 Consolidation

Every data asset in the repository, classified as **[Customer-Provided]** or **[Simulated]**,
with the evidence for each call and the retention decision that follows from it.

Nothing in this document has been deleted yet. It is the approval gate for the deletion pass:
the classification is stated first so the deletions can be checked against it.

---

## 1. How each file was classified

Classification is by **provenance trace**, not by filename or guesswork:

| Signal | Meaning |
|---|---|
| A generator script in `scripts/` or `sample_data/` writes the path | **[Simulated]** — reproducible, safe to delete |
| Arrived as a supplied bundle, no generator writes it | **[Customer-Provided]** — never regenerable, must be retained |
| Read at runtime by a seed notebook (`FileNotFoundError` if absent) | **Load-bearing** — required regardless of provenance |

---

## 2. Use-case source data

### UC3 — Excalibur

| Asset | Class | Evidence |
|---|---|---|
| `docs/UC3/CUSTOMER_DDL.csv`, `SUBSCRIBER_DDL.csv`, `PHYSICAL_DEVICE_DDL.csv` | **[Customer-Provided]** | Excalibur governance sheets. `generate_uc3_test_data.py` *reads* these to learn column names/types — "No column list is hand-typed." A generator's **input** is by definition not its output. |
| `build/uc3_test_data/**` (15 CSVs) | **[Simulated]** | Written by `scripts/generate_uc3_test_data.py --out-dir build/uc3_test_data` (documented invocation, line 36). Fully reproducible. |

### UC6 — Flood Warning (EA → Leidos)

| Asset | Class | Evidence |
|---|---|---|
| `docs/UC6/sample_bundle/uc_6/raw/**` (6 files) | **[Customer-Provided]** — *sanitised* | The supplied `uc_6_poc_bundle.zip`. Its README states the data is synthetic/sanitised **at source**, per `Flood_Warning_System_POC_Interface_Specification.docx`. No repo script generates it. It is the customer's deliverable and the customer's sanitisation — not ours to regenerate. |
| `docs/UC6/test_fixture/uc_6/raw/**` (6 files) | **[Simulated]** | Written by `scripts/generate_uc6_test_data.py` (`DEFAULT_OUT = docs/UC6/test_fixture/uc_6`). Exists because the supplied bundle has **no postcode overlap and no CSS join-key overlap**, so it yields zero matches and cannot exercise the Found/Not Found/Bad OSAPR/Single Addr branches. |
| `docs/uc_6/**` (9 files) | **DUPLICATE** | Byte-identical (md5-verified) to `docs/UC6`. Pure duplication — collapse. |

> **Both UC6 bundles are retained.** The supplied bundle proves the real-world no-match path;
> the augmented fixture proves the decision table. Deleting either loses a distinct test.

### UC7 — CDR ASN.1

| Asset | Class | Evidence |
|---|---|---|
| `flowx_testing/BT_Testing/*.asn1` (EMSC, GGSN, PSGW, TAP.310, TAP.311) | **[Customer-Provided]** | Real ASN.1 protocol module definitions. No generator emits them; `generate_synthetic_ber.py` *reads* them as input. |
| `flowx_testing/BT_Testing/tap311_sample.ber` | **[Customer-Provided]** | Supplied sample payload; not written by any generator. |
| `flowx_testing/BT_Testing/EE_...csv.gz.gpg` | **[Customer-Provided]** | Supplied encrypted EA request file. |
| `flowx_testing/BT_Testing/synthetic/*.ber` (5 files) | **[Simulated]** | Written by `scripts/generate_synthetic_ber.py` → `OUTPUT_DIR = SCHEMA_DIR / "synthetic"`. Deterministic, 10 records per protocol. |

---

## 3. Repository sample data (`sample_data/`, 111 files)

| Group | Class | Referenced by code? | Decision |
|---|---|---|---|
| `sample_data/flowx_testing/**` | **[Simulated]** | **Yes — 32 references.** `02_seed_flowx_testing_data.py` reads it and raises `FileNotFoundError` if absent | **RETAIN** — load-bearing |
| `sample_data/asn1_schema/*.asn` | **[Simulated]** | **Yes.** `03_seed_asn1_gsm_cdr_fixture.py` copies `gsm_cdr.asn` verbatim to the volume | **RETAIN** — load-bearing |
| `sample_data/sample_raw_orders.csv` | **[Simulated]** | **Yes** (1 reference) | **RETAIN** — load-bearing |
| `sample_data/bt_group/**` (11 files) | **[Simulated]** | **No — 0 references** | **DELETE** — superseded by BT_Usecase |
| `sample_data/asn1_cdr_gsm/**`, `asn1_cdr_v2/**` (8 `.ber`) | **[Simulated]** | **No — 0 references** | **DELETE** — regenerable |
| `sample_data/zip_ingestion/**` | **[Simulated]** | 1 self-reference only | **DELETE** — regenerated on-cluster |
| Loose `sample_data/sample_*.csv/json` (~22) | **[Simulated]** | Only `sample_raw_orders.csv` referenced | **DELETE** the unreferenced remainder |

> **Why the sample jobs are unaffected.** The six sample-job specs read from
> `/Volumes/{{catalog}}/flowx_sample/...`, never from repo `sample_data/`. Their
> `04_seed_sample_*` notebooks generate data **inline on-cluster** (0 references to
> `sample_data/`). Binary fixtures (ZIP, ASN.1) are built directly into the Volume
> because Databricks workspace sync mangles binary uploads.

---

## 4. Deletion summary

| Action | Files | Recoverable |
|---|---|---|
| **RETAIN** — customer-provided | 13 | n/a |
| **RETAIN** — load-bearing simulated | ~60 | n/a |
| **DELETE** — unreferenced simulated | ~45 | Yes — git history + generator scripts |
| **COLLAPSE** — byte-identical duplicate | 9 (`docs/uc_6`) | Yes |

No **[Customer-Provided]** asset is deleted by this plan.
