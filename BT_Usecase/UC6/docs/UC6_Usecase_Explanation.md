# UC6 — Flood Warning System: Environment Agency → Leidos Address/MSISDN Matching
## Use Case Explanation

| | |
|---|---|
| **Use case ID** | UC6 |
| **Domain** | Flood Warning System |
| **Owning team** | Business Data & AI (BT) |
| **Status** | POC (proof of concept), synthetic data only |
| **Related legacy asset** | `EnvAgency_address_MSISDN_report_subscribe` (Ab Initio) |

---

## 1. Background

The Environment Agency (EA) has a statutory responsibility to warn people who
may be affected by flooding in their area. To do this, EA needs to know which
mobile phone numbers (MSISDNs) belong to residents at addresses inside a
flood-risk zone.

EE (the mobile network) already performs this address-to-MSISDN matching on
a periodic basis and has historically supplied the results to **Fujitsu**,
who deliver the actual warnings on EA's behalf. Fujitsu's contract for this
service is ending, and the Environment Agency has appointed a new supplier,
**Leidos**, to take over.

This means EE must now produce the same matched output for Leidos instead of
(or, during transition, in parallel with) Fujitsu — without any gap in
service and without materially changing what EA/Leidos receive.

Separately, the legacy solution (Ab Initio on a batch ETL server, files
moved via a Google Cloud "Active Intelligence" project and a shared "G:
Drive") is not considered strategic. This use case is also the first step of
moving the underlying processing onto a modern, supported platform
(`flowx` framework on Databricks) rather than simply re-pointing the old
Ab Initio job at a new destination.

## 2. Problem statement

1. **Continuity risk:** if the Fujitsu→Leidos cutover isn't handled
   carefully, there's a risk of a gap in flood warnings to the public.
2. **Technical debt:** the current Ab Initio solution works, but is legacy,
   not strategic, and increasingly hard to support (the team that
   historically owned it, Active Intelligence, does not consider itself the
   right owner going forward).
3. **New requirements without a full spec:** at the time this transition was
   proposed, there was no fresh set of functional/non-functional
   requirements from Leidos — the design has been built from historical
   documentation and institutional knowledge of the existing Fujitsu flow.
4. **No existing PIA:** there was no Privacy Impact Assessment covering the
   as-is solution; one has since been raised (PIA 9586) so privacy/legal can
   review the data sharing.

## 3. Business objective

Produce, on a recurring (currently weekly, previously monthly) basis, two
output files containing:
- **Telephone numbers** for flood-risk areas where enough distinct addresses
  matched to be useful (an anti-tracking-style privacy safeguard — see §7).
- **OSAPR (address) match status** per flood-risk area, so EA/Leidos know
  how well each area's addresses were matched.

...and deliver both to Leidos (in addition to, then eventually instead of,
Fujitsu), while re-platforming the processing onto the `flowx` framework as
a POC ahead of a fuller strategic migration.

## 4. Scope

**In scope:** high-level and POC-level proposals and build to move the
existing mobile-data matching solution (currently serving Fujitsu) so it
also/instead serves Leidos.

**Out of scope:** fixed-line data; the full "Phase 3 strategic" architecture
(this UC6 build corresponds to the earlier POC/parallel-running phases, not
the final target-state batch/API redesign).

## 5. Stakeholders

| Name | Role | Company |
|---|---|---|
| Balaji Mahalingam | Specialist Solution Architect – Business Data & AI | BT |
| Tim Rawling | Principal Architect – Network Applications | BT |
| Jon Cole | Director, Defence: Business | BT |
| Caitlin Chinnock | Account Management Professional, Defence: Business | BT |
| Andy Hepburn | Tech Lead | Leidos |
| James Hope | Principal Architect – Business Data & AI | BT |
| Timir Talukdar | Technology Delivery | BT |
| Andy Bygrave | Product Owner | BT |
| Luis Costa | (outgoing supplier contact) | Fujitsu |

## 6. AS-IS process (legacy Ab Initio, serving Fujitsu)

1. EA deposits its flood-risk address file on a BT shared drive ("G: Drive")
   via a Google Cloud "Active Intelligence" project.
2. The file is made available to the EE Ab Initio ETL as one input source.
3. Ab Initio joins the EA address feed with EE's own internal customer
   address sources (Excalibur, JT, CSS) on postcode.
4. Two output files are generated and encrypted for security.
5. Outputs are pushed back to the G: Drive, then picked up (via EFB and the
   Active Intelligence GCS bucket) by Fujitsu.

This has been running largely unchanged for around 10 years.

## 7. TO-BE / UC6 process (this build)

The same conceptual matching still happens, but:
- The EA input file arrives (in this POC) as a **GPG-encrypted, gzipped CSV**
  rather than via the old GCP/G-Drive route.
- All five internal address sources (CSS Account, CSS Account Address, CSS
  Subscription, JT Customer Details, Excalibur Address) are loaded the same
  way EE already has them, but processed on the `flowx`/Databricks platform.
- The matching, business rules, and output file shapes are preserved from
  the legacy design (so Leidos/EA see no functional change), but the
  **encryption, orchestration, data-quality gating, and observability** are
  modernised.
- Two of the four output files are new "Leidos-only" variants that mirror
  what Fujitsu already receives, enabling a parallel-running transition
  period with no service gap.

### Inputs (6 files per cycle)
- EA flood-risk request file (from Environment Agency) — GPG + gzip.
- CSS Account, CSS Account Address, CSS Subscription — EE's pay-monthly
  (OUK) customer/address data.
- JT Customer Details — EE's pay-as-you-go (OUK) customer data.
- Excalibur Address — EE/T-Mobile UK address data.

### Outputs (4 files per cycle)
- Leidos Telephone list (plain gzip).
- Leidos OSAPR match-status list (plain gzip).
- Telephone list for the existing Fujitsu-equivalent flow (GPG + gzip).
- OSAPR match-status list for the existing Fujitsu-equivalent flow (GPG +
  gzip).

## 8. Business rules (plain-language summary)

- An address only counts as a genuine match if its **match strength** score
  is above a configurable threshold (currently 50).
- **Telephone numbers are only released for a flood-risk area if more than
  one address in that area was matched** — this is a privacy safeguard so a
  single matched address (and therefore a specific household) can't be
  singled out from the output.
- Each matched address is labelled with a status:
  - **Found** — multiple addresses matched for that area, this one included.
  - **Not Found** — only one address matched for that area (so it's
    withheld from the telephone list).
  - **Bad OSAPR** — the match strength was too low to trust.
  - **Single Addr** — there was only ever one address candidate for that
    area at all (no other candidates to compare against).

## 9. Security & compliance notes

- The EA source file and two of the four outputs are encrypted end-to-end
  using GPG (in this POC, symmetric/passphrase-based; production may use
  asymmetric public-key encryption per supplier).
- A Privacy Impact Assessment (PIA 9586) has been raised to ensure
  privacy/legal sign-off on this data-sharing arrangement.
- Only synthetic/dummy data is used in this POC bundle — no real customer
  names, addresses, MSISDNs, account IDs, or OSAPR identifiers.

## 10. Success criteria

- The UC6 build reproduces the legacy Ab Initio matching logic exactly
  (verified against the worked examples in the design documentation).
- Leidos receives both output files on the agreed cadence with no
  degradation to what Fujitsu currently receives.
- The pipeline fails safely and visibly (not silently) if any expected
  input file is missing or empty.
- All processing is observable (queryable run history, failure reasons,
  row/file counts) without needing to open the underlying job/pipeline UI.

## 11. Glossary

| Term | Meaning |
|---|---|
| EA | Environment Agency |
| EE | Everything Everywhere (the mobile network, part of BT) |
| MSISDN | Mobile Subscriber ISDN Number — i.e. a phone number |
| OSAPR | Ordnance Survey UPRN-style address reference used to identify a matched address |
| PAF | Postcode Address File — the standard UK address format all sources are normalised into |
| ETL | Extract, Transform & Load |
| DLT / LDP | Delta Live Tables / Lakeflow Declarative Pipeline — Databricks' declarative data pipeline product |
| Lakeflow Job / LFJ | Databricks' orchestration/scheduling product (formerly "Jobs") |
| EFB | Enterprise File Broker |
| PIA | Privacy Impact Assessment |
