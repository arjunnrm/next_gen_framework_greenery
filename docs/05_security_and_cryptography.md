# 🔐 FlowX — Security & Cryptography

> **Audience**: Security architects, compliance officers, and data engineers handling Personally Identifiable Information (PII), Payment Card Industry (PCI) data, and secret management.

---

## 1. Zero-Key-Leakage Cryptographic Architecture

FlowX incorporates a robust column-level encryption and decryption engine with strict zero-key-leakage guarantees:
- **No In-Code Keys**: Encryption keys are never hardcoded, never written to control tables, and never printed to logs.
- **Unity Catalog Secret Resolution**: All cryptographic keys are dynamically retrieved at runtime using the Unity Catalog 3-level secret namespace (`secret_catalog` / `secret_schema` / `secret_key`) or workspace scopes (`secret:<scope>:<key>`).
- **Deterministic Validation**: Invalid key lengths or unresolvable secrets raise immediate compile-time errors before data processing begins.

---

## 2. Column-Level AES Encryption

FlowX supports Advanced Encryption Standard (AES) encryption across 3 cipher modes:
- **`GCM`** (Galois/Counter Mode — Recommended): Authenticated encryption providing both confidentiality and integrity verification. Requires an Initialization Vector (IV) / nonce.
- **`CBC`** (Cipher Block Chaining): Standard block cipher with PKCS7 padding.
- **`ECB`** (Electronic Codebook): Deterministic encryption. Useful when encrypted columns must support equality joins across tables without prior decryption.

### Configuration Schema
```json
"target_config": {
  "encrypted_columns": [
    {
      "column_name": "customer_ssn",
      "algorithm": "AES",
      "mode": "GCM",
      "secret": {
        "secret_catalog": "poc",
        "secret_schema": "security",
        "secret_key": "pii_aes_gcm_key"
      }
    },
    {
      "column_name": "card_number",
      "algorithm": "AES",
      "mode": "CBC",
      "secret": {
        "secret_catalog": "poc",
        "secret_schema": "security",
        "secret_key": "pci_aes_cbc_key"
      }
    }
  ]
}
```

### Key Length Requirements
AES keys must be provisioned with exact byte lengths:
- **AES-128**: Exactly 16 raw bytes (or 32 hex characters).
- **AES-192**: Exactly 24 raw bytes (or 48 hex characters).
- **AES-256**: Exactly 32 raw bytes (or 64 hex characters).

---

## 3. Transformation Input Decryption

When a downstream transformation flow needs to process previously encrypted columns, declare `decrypted_columns` on the corresponding `source_inputs` entry:

```json
"transformation_flows": [
  {
    "dataflow_id": "tf_customer_scoring",
    "flow_step_id": "step_score_calc",
    "source_inputs": [
      {
        "table": "poc.silver.customers_encrypted",
        "alias": "cust",
        "decrypted_columns": [
          {
            "column_name": "customer_ssn",
            "target_name": "decrypted_ssn",
            "algorithm": "AES",
            "mode": "GCM",
            "secret": {
              "secret_catalog": "poc",
              "secret_schema": "security",
              "secret_key": "pii_aes_gcm_key"
            }
          }
        ]
      }
    ],
    "transformation_sql": "SELECT cust.id, compute_risk_score(cust.decrypted_ssn) AS score FROM cust"
  }
]
```

---

## 4. PGP Decryption & Digital Signatures

For secure file ingest and egress:
- **Pre-Extraction PGP Decryption**: Ingest encrypted `.pgp` or `.gpg` archives, decrypting in-memory before unzipping or reading.
- **PGP-Signed Egress Sinks**: Export Delta tables as PGP-encrypted, compressed ZIP archives to external Volumes or cloud buckets.

### 4.1 Asymmetric vs symmetric — two different messages, not two settings

OpenPGP can protect a message in two structurally different ways, and the framework supports
both. **They are not interchangeable**: a message encrypted one way cannot be opened the other
way, which is why the choice is an explicit `type` rather than an optional extra secret.

| | **Asymmetric** (recipient keypair) | **Symmetric** (shared passphrase) |
|---|---|---|
| Packet in the message | PKESK — addressed to a key | SKESK — derived from a passphrase |
| Produced by | `gpg --encrypt --recipient you@example.com` | `gpg --symmetric --cipher-algo AES256` |
| Ingest `pre_extraction_decryption.type` | `"pgp"` | `"pgp_symmetric"` **(v1.7.4)** |
| Ingest secret | `private_key_secret` (+ optional `passphrase_secret` to unlock that key) | `passphrase_secret` — the passphrase the **message** was encrypted with |
| Egress `pgp_encryption` secret | `recipient_public_key_secret` | `passphrase_secret` **(v1.7.4)** |
| Signing available? | **Yes** — `sign_with_private_key_secret` | **No.** Signing needs a sender keypair, which symmetric encryption has none of. |
| Who can decrypt | Only the holder of the private key | Anyone holding the passphrase |
| Who can *forge* | Nobody (a signature proves origin) | **Anyone holding the passphrase** |

The last row is the one that decides the choice. Symmetric encryption gives you
confidentiality but **not provenance**: the same secret both encrypts and decrypts, so every
party who can read a file can also produce an identical one. Prefer a recipient key plus a
signature wherever the receiver must be able to prove who sent the file. Symmetric is the right
answer when the two parties already share a passphrase out of band and the exchange is
point-to-point — which is exactly the case where demanding a keypair means managing one purely
as ceremony.

At the spec level the two egress secrets are **mutually exclusive — set exactly one**.
Supplying both is rejected at onboarding rather than resolved by precedence, because either
resolution order would silently encrypt to something the author did not choose.

```json
// Ingest: an encrypted gzip, e.g. EA_REQUEST_20260901.csv.gz.gpg
"source_zip_handling": {
  "enabled": true,
  "member_format": "gzip",
  "pre_extraction_decryption": {
    "type": "pgp_symmetric",
    "passphrase_secret": {
      "secret_catalog": "{{catalog}}", "secret_schema": "config", "secret_key": "pgpkey"
    }
  }
}

// Egress: the same passphrase, producing <stem>.csv.gz.gpg
"pgp_encryption": {
  "enabled": true,
  "passphrase_secret": {
    "secret_catalog": "{{catalog}}", "secret_schema": "config", "secret_key": "pgpkey"
  }
}
```

Both directions are AES256 by default and were verified against the **GnuPG 2.4.9 CLI in both
directions**: the framework decrypts what `gpg` wrote, and `gpg` decrypts what the framework
wrote. Implementation: `crypto/pgp.py::pgp_decrypt_symmetric` / `pgp_encrypt_symmetric`.

!!! warning "The passphrase is a secret reference, never a literal"
    `passphrase_secret` takes a Unity Catalog secret *reference*
    (`secret_catalog`/`secret_schema`/`secret_key`) and the resolved value never enters the
    spec, the control tables, or a log line. The Spec Builder additionally blocks
    `passphrase` as a literal key name. See §1 and §5.

---

### 4.2 When Unity Catalog secrets are not enabled

UC secrets are an **opt-in metastore capability**. On a metastore where they are switched
off, `dbutils.secrets.get(catalog=, schema=, key=)` raises:

```
[UC_SECRETS_NOT_ENABLED] Support for Unity Catalog Secrets is not enabled. SQLSTATE: 56038
```

`CREATE SECRET flowx.config.pgpkey VALUE '...'` fails the same way — the syntax parses, so
this is a *feature flag*, not a permissions or naming problem. Before v1.7.4 that made every
crypto-bearing spec unrunnable on such a workspace, with no configuration-level workaround.

`resolve_secret_value` now tries the UC lookup first and, **only if it raises**, falls back to
a classic workspace scope, trying these names in order:

| Order | Scope name for `flowx.config.pgpkey` |
|---|---|
| 1 | `flowx.config` — catalog and schema, dot-joined |
| 2 | `flowx_config` — underscore-joined |
| 3 | `config` — schema alone |
| 4 | `flowx` — catalog alone |

The key name is always `secret_key` unchanged. Create one with:

```bash
databricks secrets create-scope flowx.config
databricks secrets put-secret flowx.config pgpkey   # prompts; the value never hits your shell history
```

Three properties are worth being explicit about, because each is a place this could have gone
wrong:

- **The spec does not change.** A secret reference keeps exactly one shape —
  `{secret_catalog, secret_schema, secret_key}`. Only *resolution* is widened, so there is no
  second spelling to keep in sync, no new attribute in the JSON schema, and nothing for the
  Spec Builder to render. A spec written for a UC-secrets workspace runs unmodified on one
  without them, and vice versa.
- **A healthy workspace never consults a scope.** The fallback is reached only after the UC
  lookup has already raised, so where UC secrets work, behaviour is byte-for-byte what it was.
- **The fallback cannot tell "UC is off" from "UC is on and the secret is missing"**, because
  distinguishing them means parsing vendor error text. So in the second case it *does* try the
  scopes, and a scope of a matching name holding that key **will** be used. This is deliberate
  and logged at `WARNING` naming both the UC label and the scope it resolved from. If that
  ambiguity is unacceptable in your environment, do not create scopes named after a catalog or
  schema.

When nothing resolves, the error names the qualified UC label, preserves the original UC
cause, and lists every scope tried — so the failure says which of the two systems was
misconfigured rather than only that a secret was missing.

---

## 5. Secret Provisioning & Key Rotation Runbook

### Step 1: Provision Secrets in Databricks CLI
```bash
# 1. Create a security secret scope
databricks secrets create-scope security

# 2. Generate a cryptographically secure 256-bit key and store as secret
databricks secrets put-secret security pii_aes_gcm_key \
  --string-value "$(openssl rand -hex 16)"
```

### Step 2: Key Rotation Workflow
1. Provision the new key version under a new secret key (e.g. `pii_aes_gcm_key_v2`).
2. Update the onboarding spec `secret.secret_key` attribute to point to `pii_aes_gcm_key_v2`.
3. Re-onboard the spec with `action_type: "update"`.
4. Trigger the pipeline update. New records are encrypted with Key V2. Existing historical records can be batch-re-encrypted using a backfill migration script.

---

## 6. Official Databricks Security References
- [Databricks Secrets CLI & API Reference](https://docs.databricks.com/aws/en/security/secrets/)
- [Unity Catalog Secrets & Access Management](https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets)
