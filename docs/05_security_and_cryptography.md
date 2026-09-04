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
