# UC6 – Flood Warning System (EA → Leidos) – POC Sample Data Bundle

This bundle contains **synthetic / sanitised** sample source files for the Flood
Warning System UC6 proof-of-concept, laid out the way they should land in the
`uc_6` staging volume before the ingestion job runs.

⚠️ All data is dummy/synthetic (per `Flood_Warning_System_POC_Interface_Specification.docx`).
No production customer, address, MSISDN, account or OSAPR data is included.

## Contents

```
uc_6/
└── raw/
    ├── EE_2026-08-20-REQUEST_1OF1.csv.gz.gpg      <- EA input file: gzip THEN gpg symmetric (AES256)
    ├── CM_EXCALIBUR_ADDRESS_20250127_00000048.dat.gz
    ├── CM_JT_Customer_Details_20250127_12345678.dat.gz
    ├── CSS_account_20250127_00000008.dat.gz
    ├── CSS_account_address_20250127_00000008.dat.gz
    └── CSS_subscription_20250127_00000009.dat.gz
docs/
├── UC6_Usecase_Explanation.md          <- business/functional overview
└── UC6_Implementation_Design_flowx.md  <- technical design (jobs, pipeline, tables, observability)
```

Per the requested handling rules for this POC:
- **Every file is plain gzip (`.gz`) EXCEPT the EE/Environment-Agency request file**,
  which is gzip **+ GPG symmetric encryption** (passphrase-based, cipher AES256).
- Decrypt passphrase (POC/synthetic only): `EA-POC-Sample-2026!`
  This same passphrase should be stored in Databricks as secret
  `br_digital_poc.config.pgpkey` and referenced by the pipeline — never hardcoded in code.

## How to use

1. Upload the `uc_6/` folder as-is into the target Unity Catalog volume, e.g.:
   `/Volumes/<catalog>/<schema>/uc_6/raw/`
2. Trigger job `007_uc6_lfj_EA` — **orchestration only**; it simply kicks off
   the pipeline run (no file handling happens in the job itself).
3. The triggered pipeline `008_uc6_ldp_EA` (Lakeflow Declarative Pipeline)
   does all the actual work: validates the file-count/size gate, unzips and
   GPG-decrypts, archives the originals, joins/transforms, and writes the
   4 output files back into the `uc_6` volume — all via the framework's
   built-in zip/unzip/archive/GPG utilities.

See `claude_code_prompt_uc6_EA_flowx.md` (provided alongside this bundle) for
the full engineering brief to hand to Claude Code, and the `docs/` folder for
the business use-case explanation and the detailed technical design.
