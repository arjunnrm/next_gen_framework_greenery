# 03 — Platform Best Practice and Naming Standards

**FlowX (NextGen Metadata Framework) · UC3, UC6, UC7**
**Framework release:** v1.7.4 · **Wheel artefact version:** 0.0.3
**Document date:** 5 September 2026
**Status:** For review and sign-off prior to rollout
**Audience:** Platform Architects, Framework Engineering, Front-End Engineers, Release Management

---

## What this document is

The conventions and practices that apply across the platform rather than to any one use case:
how objects are named, how a pre-production release signals that it is pre-production, and the
build and deployment practices that this repository has learnt the hard way.

Three parts:

| Clause | Covers |
|---|---|
| 1 | **Naming standards** — the conventions actually in force, the forward-looking standard for new assets, and why no renaming of existing assets is recommended |
| 2 | **Beta release governance and UI components** — the banner, pill, disclaimer, the release-stage flag that governs them, and the obligations attached to the disclaimer text |
| 3 | **Build and deployment practice** — the repository-specific traps that cost time when they are rediscovered |

For the assets these conventions name, see document 01. For the notebook whose folder layout
follows them, see document 02. For the order in which everything is applied, see document 04.

## Findings that need a decision

Three of the six cross-cutting findings in this delivery set belong to the owner of this document.
The other three are governance findings and are stated in document 01.

| # | Finding | Evidence | Recommended action | Owner role |
|---|---|---|---|---|
| **4** | **The app reports a stale framework version.** `settings.py` `AppInfo.framework_version` defaults to `"1.5.0"` while the repository is at v1.7.4. The Beta provenance block reads this field, so shipping as-is puts a demonstrably wrong version in front of every user, in the one component whose entire purpose is to state what they are running. | `databricks-app/server/settings.py` line 15. | Correct the default to `1.7.4` and add a release check so the value cannot drift again. Must be fixed before the app is deployed with the Beta banner. | Application Owner |
| **5** | **Support contact conflict.** `settings.py` defaults `support_contact` to `data-platform@example.com` — a placeholder domain — while the canonical Beta disclaimer directs users to Hoonartek. Two different answers to "who do I contact" would ship in the same interface. | `databricks-app/server/settings.py` line 17 against the disclaimer text in clause 2 below. | Reconcile to **one** address before deployment. Recommendation: set `support_contact` to the Hoonartek address used in the disclaimer, so the banner and the metadata block agree. | Application Owner |
| **6** | **Sticky-banner layout collision.** `Shell.jsx` sets the navigation rail to `top: var(--headerH)` and `height: calc(100vh - var(--headerH))` with `--headerH: 58px`. A *sticky* Beta banner adds roughly 38px below the header that the rail's height calculation does not know about, so the rail overhangs the viewport. | `databricks-app/web/src/Shell.jsx` lines 14, 50, 54; `--headerH` token in `theme.css`. | Ship the banner **non-sticky** — drop `position: sticky`, `top` and `z-index`. The header pill is the persistent signal and is unaffected. Avoids touching the rail's layout maths entirely. | Front-End Engineer |

All three have small, well-understood fixes and should simply be done.

---

## A note on the catalog name

Throughout this delivery set the Unity Catalog name is **always a parameter, never a literal**.
Two names appear, and the distinction matters:

| Name | What it is |
|---|---|
| `flowx` | The **live catalog** on the `metaflow_v7` workspace. Every asset the repository currently owns lives here, and the asset register in document 01 describes this estate as it actually stands. |
| `bt_digital_poc` | A **worked example** of a different deployment target, used to show how the same artefacts are pointed at another catalog without editing them. |

Wherever a path is written as `/Volumes/<catalog>/...`, substitute whichever applies. In the setup
notebook this is the `target_catalog` widget; in a bundle it is `${var.catalog}`; in an onboarding
spec it is the `{{catalog}}` token. So the UC7 landing path resolves as:

```text
/Volumes/flowx/landing/uc_7/raw/EMSC/            <- current metaflow_v7 estate
/Volumes/bt_digital_poc/landing/uc_7/raw/EMSC/   <- e.g. a POC deployment
```

**One caveat that is not cosmetic.** `resources/uc7/*.yml` currently hardcodes `catalog: flowx`
rather than using `${var.catalog}`. Until that is parameterised, UC7 cannot be deployed into
`bt_digital_poc` or any other catalog by changing a variable alone. This is tracked as gap G-05 in
document 01, and it is a prerequisite for any catalog migration.

---

## The delivery set

| # | Document | Covers |
|---|---|---|
| 01 | [Use case asset inventory and governance](01_usecase_asset_inventory_and_governance.md) | Asset register, tagging strategy, data sensitivity, access control, governance gaps |
| 02 | [Environment deployment and setup](02_environment_deployment_and_setup.md) | The setup and staging notebook, its design, and how to run it |
| 03 | [Platform best practice and naming standards](03_platform_best_practice_and_naming_standards.md) | Naming conventions, Beta release governance, UI components, build and deploy practice |
| 04 | [Consolidated rollout runbook](04_consolidated_rollout_runbook.md) | The ordered end-to-end sequence, cross-alignment record, open assumptions |

Each document stands alone. Where one depends on another it links rather than repeats.

---

## Clause 1 — Naming standards


This clause is a reconciliation, not an aspiration. The repository has real conventions already in
use, and a standards document that contradicts them would simply be ignored.

#### 1 Approach

The naming standards clause 1.2 in [document 03](03_platform_best_practice_and_naming_standards.md) records what the repository actually does today and treats it as authoritative.
Clause 1.3 in [document 03](03_platform_best_practice_and_naming_standards.md) proposes a forward-looking standard for **new** assets only. Clause 1.4 in [document 03](03_platform_best_practice_and_naming_standards.md) explains why
nothing existing should be renamed.

#### 2 Authoritative in-place conventions

| Asset type | Convention | Example |
|---|---|---|
| Job and pipeline files | `<NNN>_<lfj\|ldp>_<uc>_<descriptor>` — number first, all lowercase | `007_lfj_uc6_ea_flood_warning` |
| Dataflow group | `dfg_` prefix | `dfg_uc6_ea_flood_warning` |
| Dataflow | `df_` prefix | `df_uc6_css_account` |
| Reconciliation flow | `rf_` prefix | `rf_uc3_excalibur` |
| Transformation step | `ts_` prefix | `ts_uc6_conform_address` |
| Volume | `uc_<N>` with an underscore | `uc_3`, `uc_6`, `uc_7` |
| Schema-config folder | `_schemas/<source>/` | `_schemas/css_account/` |
| Extraction target | `_extracted/<source>/` | `_extracted/ea_request/` |

**A worked precedent.** The brief that produced UC6 proposed `007_uc6_lfj_EA`. This was
**explicitly rejected** in favour of `007_lfj_uc6_ea_flood_warning`, which follows the repository's
number-first, lowercase convention. That rejection is the precedent: the repository convention wins
over a proposed one.

#### 3 Live sequence-number registry

| Number | Assigned to |
|---|---|
| 001 | UC7 CDR ASN.1 |
| 002 | *(unassigned)* |
| 003–006 | UC3 |
| 007–008 | UC6 |
| **009** | **Next free** |

This registry currently lives only in prose, which is gap G-06.

#### 4 Three table-naming conventions coexist in `flowx.bronze`

An honest finding rather than a recommendation:

| Convention | Example | Used by |
|---|---|---|
| Prefixed with the use case | `uc6_css_account` | UC6 |
| Descriptive with a type suffix | `emsc_cdr_raw` | UC7 |
| Bare entity name | `customer` | UC3 |

Because all three share one schema, only the first makes a collision structurally impossible. This
is gap G-09.

#### 5 Forward-looking standard for new assets

For **new** assets only, adopt UC6's pattern: **prefix every Bronze table with `uc<N>_`.** It is the
only one of the three conventions under which two use cases cannot collide on a shared schema, and
it is already in production use, so it needs no new tooling or validation.

#### 6 Migration note — rename nothing

**No renaming of existing assets is recommended.** The reasons are concrete, not cautious:

- **Renaming a target orphans its checkpoint** and forces a full refresh. For UC7's SGSN table, that
  is 175,048 records re-decoded; for a streaming CDC target, it is a rebuild of the entire history.
- **Renaming a job or pipeline breaks `${resources.jobs.*.id}` references** across the bundle, which
  fail at deploy time and, worse, can resolve to the wrong resource if a name is reused.

The migration is therefore **convention-forward only**: new assets follow C.3.5; existing assets
keep the names they have. The inconsistency in C.3.4 is documented rather than fixed.


---

## Clause 2 — Beta release governance and UI components

This part supplies the components that mark the FlowX application as Beta, the release-stage flag
that governs them, and the governance obligations attached to the disclaimer text.

### 1 Design basis

The components are built against the application as it actually is, not against a generic
React application. The following characteristics were established from the source and every
component in this part depends on them.

| Characteristic | Actual value in this repository | Consequence for the components |
|---|---|---|
| Stack | FastAPI backend, React with Vite front end | Components are plain `.jsx` with no additional build step |
| Header | `Shell.jsx`, sticky, 58px tall | `--headerH: 58px` is the offset every layout decision works from |
| Theming | `data-mfl` attribute on the root element | Light theme is targeted via `:root[data-mfl="light"]`, **not** `prefers-color-scheme` |
| Styling helper | `sx()` inline-style helper | React components use `sx()` and `var(--token)`, matching existing code |
| Attention colour | `--req` = `#f0a35e` (dark) / `#a85f18` (light) | See A.1.1 |

#### A.1.1 There is no `--warn` token

`theme.css` defines `--ac`, `--ac2`, `--ac3`, `--acbd`, `--actrack`, `--ok` and `--req`. It defines
**no** `--warn` token. Rather than introduce one, the components reuse `--req` — the token already
used for "requires attention" — which is semantically the closest match and, importantly, already
has values tuned for both themes.

Contrast was checked rather than assumed:

| Theme | `--req` value | Contrast against its background | Rating |
|---|---|---|---|
| Dark | `#f0a35e` | approximately 8.9:1 | AAA |
| Light | `#a85f18` | approximately 5.4:1 | AA |

Both pass for body text at the sizes used. If a dedicated Beta colour is later wanted, add a
`--beta` token to **both** theme blocks — see the known constraints in A.8.

### 2 The canonical disclaimer

The following text is **change-controlled**. It appears across seven surfaces and must read
identically on all of them.

> This application is currently in Beta and is not approved for production workloads. For support,
> customisation, or production enablement, please contact Hoonartek.

Two standardised short forms are permitted where the full text does not fit:

| Form | Text | Where used |
|---|---|---|
| Pill label | `Beta` | Header, beside the product name |
| Tooltip | `Beta — not approved for production workloads` | `title` attribute on the pill |

No other abbreviation, paraphrase or re-wording is permitted. See the governance obligations in A.7.

### 3 Deliverable A — Markdown snippets

For documentation surfaces. Four variants, because the target renderers differ in what they support.

#### A.3.1 MkDocs Material admonition

```markdown
!!! warning "Beta — not approved for production workloads"

    This application is currently in Beta and is not approved for production workloads.
    For support, customisation, or production enablement, please contact Hoonartek.
```

#### A.3.2 GitHub blockquote alert

```markdown
> [!WARNING]
> **Beta — not approved for production workloads.**
> This application is currently in Beta and is not approved for production workloads.
> For support, customisation, or production enablement, please contact Hoonartek.
```

#### A.3.3 Plain-Markdown fallback

For renderers that support neither of the above.

```markdown
---

**⚠ Beta — not approved for production workloads**

This application is currently in Beta and is not approved for production workloads.
For support, customisation, or production enablement, please contact Hoonartek.

---
```

#### A.3.4 Badge form

For a README header line.

```markdown
![Beta](https://img.shields.io/badge/status-Beta-f0a35e)
![Not for production](https://img.shields.io/badge/production-not%20approved-a85f18)
```

#### A.3.5 About / version metadata table

This block is **not** dismissible and **persists after general availability** — only the status row
changes. It is the provenance record, not the Beta notice.

```markdown
| Field | Value |
|---|---|
| Product | FlowX Metadata Framework |
| Framework version | 1.7.4 |
| Wheel artefact | 0.0.3 |
| Environment | dev |
| Release stage | Beta |
| Support | Hoonartek |
```

> **Note.** The framework version above is the correct one. The application's `settings.py`
> currently defaults to `1.5.0` — see critical finding 4, which must be resolved before this block
> is rendered from configuration.

### 4 Deliverable B — Self-contained CSS and HTML

For plain HTML surfaces and static pages. All classes are prefixed `.flowx-beta-*` and all custom
properties `--flowx-beta-*`, so nothing here can collide with application styles.

#### A.4.1 CSS

Every token carries a literal hex fallback, so the block renders correctly even on a standalone page
that never loads `theme.css`.

```css
/* FlowX Beta notice — self-contained. Safe to inline on any page. */
.flowx-beta-banner,
.flowx-beta-pill,
.flowx-beta-meta {
  --flowx-beta-accent: var(--req, #a85f18);
  --flowx-beta-fg: var(--fg, #1b1b1b);
  --flowx-beta-bg: var(--bg2, #fdf6ee);
  --flowx-beta-border: var(--flowx-beta-accent);
}

/* Dark theme: the application sets data-mfl on the root element. */
:root[data-mfl="dark"] .flowx-beta-banner,
:root[data-mfl="dark"] .flowx-beta-pill,
:root[data-mfl="dark"] .flowx-beta-meta {
  --flowx-beta-accent: var(--req, #f0a35e);
  --flowx-beta-fg: var(--fg, #e8e8e8);
  --flowx-beta-bg: var(--bg2, #2a2118);
}

/* Light theme, explicitly selected. */
:root[data-mfl="light"] .flowx-beta-banner,
:root[data-mfl="light"] .flowx-beta-pill,
:root[data-mfl="light"] .flowx-beta-meta {
  --flowx-beta-accent: var(--req, #a85f18);
  --flowx-beta-fg: var(--fg, #1b1b1b);
  --flowx-beta-bg: var(--bg2, #fdf6ee);
}

/* Standalone pages that never set data-mfl fall back to the OS preference.
   Guarded with :root:not([data-mfl]) so it can never override an explicit choice. */
@media (prefers-color-scheme: dark) {
  :root:not([data-mfl]) .flowx-beta-banner,
  :root:not([data-mfl]) .flowx-beta-pill,
  :root:not([data-mfl]) .flowx-beta-meta {
    --flowx-beta-accent: #f0a35e;
    --flowx-beta-fg: #e8e8e8;
    --flowx-beta-bg: #2a2118;
  }
}

.flowx-beta-banner {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  padding: 10px 14px;
  background: var(--flowx-beta-bg);
  color: var(--flowx-beta-fg);
  border-bottom: 2px solid var(--flowx-beta-border);
  font-size: 13px;
  line-height: 1.45;
}

.flowx-beta-banner__tag {
  flex: 0 0 auto;
  font-weight: 700;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  color: var(--flowx-beta-accent);
}

.flowx-beta-banner__text { flex: 1 1 auto; }

.flowx-beta-banner__dismiss {
  flex: 0 0 auto;
  background: none;
  border: 1px solid var(--flowx-beta-border);
  border-radius: 4px;
  color: var(--flowx-beta-fg);
  cursor: pointer;
  font-size: 12px;
  padding: 2px 8px;
}

.flowx-beta-banner__dismiss:focus-visible,
.flowx-beta-pill:focus-visible {
  outline: 2px solid var(--flowx-beta-accent);
  outline-offset: 2px;
}

.flowx-beta-pill {
  display: inline-block;
  padding: 1px 7px;
  border: 1px solid var(--flowx-beta-accent);
  border-radius: 999px;
  color: var(--flowx-beta-accent);
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  vertical-align: middle;
}

.flowx-beta-meta { font-size: 12px; }
.flowx-beta-meta th { text-align: left; font-weight: 600; padding-right: 12px; }

/* Printed output keeps the notice and drops the control. */
@media print {
  .flowx-beta-banner__dismiss { display: none !important; }
  .flowx-beta-banner { border-bottom: 2px solid #000; }
}
```

#### A.4.2 Banner HTML

```html
<div class="flowx-beta-banner" role="region" aria-label="Beta release notice">
  <span class="flowx-beta-banner__tag" aria-hidden="true">Beta</span>
  <span class="flowx-beta-banner__text">
    This application is currently in Beta and is not approved for production workloads.
    For support, customisation, or production enablement, please contact Hoonartek.
  </span>
  <button type="button"
          class="flowx-beta-banner__dismiss"
          data-flowx-beta-dismiss
          aria-label="Dismiss the Beta notice for this session">Dismiss</button>
</div>
```

**Two accessibility decisions worth recording, because both look wrong at a glance:**

- The container is `role="region"` with an `aria-label`, **not** `role="status"`. The notice is
  present at page load rather than arriving in response to an action. `role="status"` is a live
  region and would re-announce the text on every re-render, which is noise, not information.
- The `Beta` tag carries `aria-hidden="true"`. The word "Beta" already appears in the sentence that
  follows it, and in the region's own label. Without the attribute a screen reader says "Beta"
  three times before reaching any content.

#### A.4.3 Session dismissal script

Dismissal is **session-scoped and never persistent** — see the placement rules in A.6. Every storage
access is wrapped, because `sessionStorage` throws outright in some embedded and privacy contexts
rather than merely returning null.

```html
<script>
(function () {
  var KEY = "flowx-beta-notice-dismissed";

  function isDismissed() {
    try { return sessionStorage.getItem(KEY) === "1"; }
    catch (e) { return false; }
  }

  function remember() {
    try { sessionStorage.setItem(KEY, "1"); }
    catch (e) { /* Storage unavailable: dismissal simply does not persist. */ }
  }

  var banner = document.querySelector(".flowx-beta-banner");
  if (!banner) { return; }

  if (isDismissed()) {
    banner.hidden = true;
    return;
  }

  var button = banner.querySelector("[data-flowx-beta-dismiss]");
  if (button) {
    button.addEventListener("click", function () {
      banner.hidden = true;
      remember();
    });
  }
})();
</script>
```

#### A.4.4 About / metadata HTML

```html
<table class="flowx-beta-meta">
  <tbody>
    <tr><th scope="row">Product</th><td>FlowX Metadata Framework</td></tr>
    <tr><th scope="row">Framework version</th><td>1.7.4</td></tr>
    <tr><th scope="row">Wheel artefact</th><td>0.0.3</td></tr>
    <tr><th scope="row">Environment</th><td>dev</td></tr>
    <tr><th scope="row">Release stage</th><td>Beta</td></tr>
    <tr><th scope="row">Support</th><td>Hoonartek</td></tr>
  </tbody>
</table>
```

### 5 Deliverable C — React components and wiring

#### A.5.1 `BetaNotice.jsx`

Place at `databricks-app/web/src/BetaNotice.jsx`. Uses the existing `sx()` helper and `var(--token)`
references so it inherits theming automatically.

```jsx
import React, { useState } from "react";
import { sx } from "./sx";

const DISCLAIMER =
  "This application is currently in Beta and is not approved for production workloads. " +
  "For support, customisation, or production enablement, please contact Hoonartek.";

const STORAGE_KEY = "flowx-beta-notice-dismissed";

function readDismissed() {
  try { return sessionStorage.getItem(STORAGE_KEY) === "1"; }
  catch (e) { return false; }
}

function writeDismissed() {
  try { sessionStorage.setItem(STORAGE_KEY, "1"); }
  catch (e) { /* Storage unavailable: dismissal does not persist. */ }
}

/**
 * The persistent Beta signal in the header. Deliberately NOT dismissible:
 * when the banner is dismissed for the session, this is what remains.
 */
export function BetaPill() {
  return (
    <span
      className="flowx-beta-pill"
      title="Beta — not approved for production workloads"
      style={sx({
        display: "inline-block",
        padding: "1px 7px",
        border: "1px solid var(--req)",
        borderRadius: 999,
        color: "var(--req)",
        fontSize: 11,
        fontWeight: 600,
        letterSpacing: "0.04em",
        textTransform: "uppercase",
        verticalAlign: "middle",
      })}
    >
      Beta
    </span>
  );
}

/**
 * The full disclaimer. Non-sticky by design — a sticky banner collides with the
 * navigation rail's height calculation in Shell.jsx. See the known-constraints clause in [document 03](03_platform_best_practice_and_naming_standards.md).
 */
export function BetaBanner() {
  const [dismissed, setDismissed] = useState(readDismissed);
  if (dismissed) { return null; }

  return (
    <div
      role="region"
      aria-label="Beta release notice"
      className="flowx-beta-banner"
      style={sx({
        display: "flex",
        alignItems: "flex-start",
        gap: 10,
        padding: "10px 14px",
        background: "var(--bg2)",
        color: "var(--fg)",
        borderBottom: "2px solid var(--req)",
        fontSize: 13,
        lineHeight: 1.45,
      })}
    >
      <span
        aria-hidden="true"
        style={sx({
          flex: "0 0 auto",
          fontWeight: 700,
          letterSpacing: "0.04em",
          textTransform: "uppercase",
          color: "var(--req)",
        })}
      >
        Beta
      </span>
      <span style={sx({ flex: "1 1 auto" })}>{DISCLAIMER}</span>
      <button
        type="button"
        className="flowx-beta-dismiss"
        aria-label="Dismiss the Beta notice for this session"
        onClick={() => { setDismissed(true); writeDismissed(); }}
        style={sx({
          flex: "0 0 auto",
          background: "none",
          border: "1px solid var(--req)",
          borderRadius: 4,
          color: "var(--fg)",
          cursor: "pointer",
          fontSize: 12,
          padding: "2px 8px",
        })}
      >
        Dismiss
      </button>
    </div>
  );
}

/**
 * Provenance block. Never dismissible; persists after general availability,
 * with only the release-stage row changing.
 */
export function BetaMeta({ info }) {
  const rows = [
    ["Product", "FlowX Metadata Framework"],
    ["Framework version", info?.framework_version ?? "unknown"],
    ["Wheel artefact", info?.wheel_version ?? "unknown"],
    ["Environment", info?.environment_label ?? "unknown"],
    ["Release stage", info?.release_stage ?? "ga"],
    ["Support", info?.support_contact ?? "Hoonartek"],
  ];

  return (
    <table className="flowx-beta-meta" style={sx({ fontSize: 12 })}>
      <tbody>
        {rows.map(([label, value]) => (
          <tr key={label}>
            <th scope="row" style={sx({ textAlign: "left", fontWeight: 600, paddingRight: 12 })}>
              {label}
            </th>
            <td>{value}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

#### A.5.2 The five wiring points

| # | File | Location | Change |
|---|---|---|---|
| 1 | `web/src/Shell.jsx` | Import block | `import { BetaPill, BetaBanner } from "./BetaNotice";` |
| 2 | `web/src/Shell.jsx` | Line 14, after the `Spec Builder` span | `{isBeta && <BetaPill />}` |
| 3 | `web/src/Shell.jsx` | Line 50, immediately after `</header>` | `{isBeta && <BetaBanner />}` |
| 4 | `web/src/theme.css` | End of file | The `:focus-visible` ring rule from A.4.1 |
| 5 | `web/src/Builder.jsx` | View-model fields | Read `cfg.app.release_stage`, defaulting to `"ga"` |

**On the default in wiring point 5.** The default is `"ga"`, not `"beta"`. This is deliberate and
fail-closed: if the configuration fails to load or a key is renamed, the application hides the Beta
banner on what may be a Beta deployment — an under-warning. The alternative default would show a
"not approved for production" banner on a genuine production deployment, which is an actively
misleading statement about the system. Of the two failure modes, the silent one is preferable.

```jsx
// Builder.jsx — view-model fields
const releaseStage = cfg?.app?.release_stage ?? "ga";
const isBeta = releaseStage === "beta" || releaseStage === "rc";
```

### 6 Deliverables D, E and F — other front-end frameworks

Provided for surfaces outside the main React application. The disclaimer text is identical in all
of them; only the mechanics differ.

#### A.6.1 Streamlit

```python
import streamlit as st

DISCLAIMER = (
    "This application is currently in Beta and is not approved for production workloads. "
    "For support, customisation, or production enablement, please contact Hoonartek."
)

# Native variant — preferred. Inherits the Streamlit theme, no custom CSS.
st.warning(f"**Beta — not approved for production workloads.**\n\n{DISCLAIMER}", icon="⚠")

# Styled variant, if the native look is not wanted.
# NOTE: unsafe_allow_html strips <script>, so this variant CANNOT self-dismiss.
st.markdown(
    f"""
    <div style="padding:10px 14px;border-bottom:2px solid #a85f18;background:#fdf6ee;
                color:#1b1b1b;font-size:13px;line-height:1.45;">
      <strong style="color:#a85f18;text-transform:uppercase;letter-spacing:0.04em;">Beta</strong>
      &nbsp;{DISCLAIMER}
    </div>
    """,
    unsafe_allow_html=True,
)

# Sidebar provenance block.
with st.sidebar:
    st.caption("FlowX Metadata Framework")
    st.caption("Framework 1.7.4 · wheel 0.0.3 · dev · Beta")
    st.caption("Support: Hoonartek")
```

#### A.6.2 Dash

```python
from dash import html
import dash_bootstrap_components as dbc

DISCLAIMER = (
    "This application is currently in Beta and is not approved for production workloads. "
    "For support, customisation, or production enablement, please contact Hoonartek."
)


def beta_banner(is_beta: bool):
    """Bootstrap variant. Returns None when not Beta -- filter None from the layout list."""
    if not is_beta:
        return None
    return dbc.Alert(
        [html.Strong("Beta — not approved for production workloads. "), DISCLAIMER],
        color="warning",
        dismissable=True,
        className="flowx-beta-banner",
    )


def beta_banner_plain(is_beta: bool):
    """No-dependency variant. Custom CSS belongs in the app's assets/ folder."""
    if not is_beta:
        return None
    return html.Div(
        [
            html.Span("Beta", className="flowx-beta-banner__tag"),
            html.Span(DISCLAIMER, className="flowx-beta-banner__text"),
        ],
        className="flowx-beta-banner",
        role="region",
        **{"aria-label": "Beta release notice"},
    )


def beta_meta():
    return html.Div(
        "FlowX Metadata Framework · framework 1.7.4 · wheel 0.0.3 · dev · Beta · Support: Hoonartek",
        className="flowx-beta-meta",
    )


# In the layout, drop the Nones:
# app.layout = html.Div([c for c in [beta_banner(is_beta), beta_meta(), body] if c is not None])
```

#### A.6.3 Gradio

```python
import gradio as gr

DISCLAIMER = (
    "This application is currently in Beta and is not approved for production workloads. "
    "For support, customisation, or production enablement, please contact Hoonartek."
)

# Gradio targets light mode via :not(.dark) rather than a media query.
BETA_CSS = """
.flowx-beta-banner {
  padding: 10px 14px; border-bottom: 2px solid #f0a35e;
  background: #2a2118; color: #e8e8e8; font-size: 13px; line-height: 1.45;
}
:not(.dark) .flowx-beta-banner {
  border-bottom-color: #a85f18; background: #fdf6ee; color: #1b1b1b;
}
.flowx-beta-banner strong { text-transform: uppercase; letter-spacing: 0.04em; }
"""


def is_beta() -> bool:
    import os
    return os.environ.get("FLOWX_RELEASE_STAGE", "ga") in ("beta", "rc")


with gr.Blocks(css=BETA_CSS) as demo:
    # Always construct the component and toggle `visible`. Conditional construction
    # changes the component tree, which breaks Gradio's event wiring.
    gr.HTML(
        f'<div class="flowx-beta-banner" role="region" aria-label="Beta release notice">'
        f'<strong>Beta</strong> &nbsp;{DISCLAIMER}</div>',
        visible=is_beta(),
    )
    gr.Markdown(
        "FlowX Metadata Framework · framework 1.7.4 · wheel 0.0.3 · dev · Support: Hoonartek",
        visible=True,
    )
```

### 7 Placement rules and governance obligations

#### A.7.1 Placement summary

| Framework | Pill | Banner | Provenance block |
|---|---|---|---|
| React (primary app) | Header, after the product name | After `</header>`, non-sticky | About panel |
| Markdown / MkDocs | Badge in the page header | Admonition at the top of the page | About page table |
| Streamlit | — | `st.warning` at the top of the script | Sidebar caption |
| Dash | — | First element of the layout | Footer division |
| Gradio | — | First block, `visible=is_beta()` | Trailing Markdown |

#### A.7.2 Five universal placement rules

1. **Above the fold on every route.** Not only the landing page. A user who deep-links into a
   sub-page must see the notice.
2. **Never inside a collapsible, tab or modal.** A notice the user must open is not a notice.
3. **Dismissal is session-scoped, never persistent.** A new session shows it again. Use
   `sessionStorage`, never `localStorage` and never a cookie.
4. **The header pill is not dismissible.** It is the residual signal that survives dismissal of the
   banner, and it is why session dismissal is acceptable at all.
5. **Print keeps the banner and drops the button.** A printed or exported page must carry the
   disclaimer; a "Dismiss" control on paper is meaningless.

#### A.7.3 Release-stage governance clause

The Beta state is controlled by **one** variable, so that reaching general availability is a
one-line reviewable difference rather than a code removal.

```yaml
# app.yaml
env:
  - name: FLOWX_RELEASE_STAGE
    value: ${var.release_stage}   # beta | rc | ga
```

```python
# databricks-app/server/settings.py
class AppInfo(BaseModel):
    framework_version: str = "1.7.4"          # corrected; see critical finding 4
    environment_label: str = "dev"
    support_contact: str = "Hoonartek"        # reconciled; see critical finding 5
    release_stage: str = "ga"                 # fail-closed default
```

```yaml
# Per-target DABs variables
targets:
  dev:
    variables:
      release_stage: beta
  prod:
    variables:
      release_stage: ga
```

#### A.7.4 The four-step general-availability checklist

1. Flip `release_stage` from `beta` to `ga` in the target's variables.
2. Run **both** `databricks bundle deploy` **and** `databricks bundle run`. Deploy alone leaves the
   application serving its previous code — see the build note in A.9.
3. Remove the documentation partial and rebuild MkDocs.
4. Add an entry to `RELEASE_NOTES.md`.

#### A.7.5 Six governance obligations

1. **Do not delete the components at general availability.** Flip the flag. Deleting them means
   rebuilding from scratch for the next pre-release.
2. **The disclaimer text is change-controlled across seven surfaces.** Any change to the wording is
   a change to all of them, in one commit.
3. **Dismissal is never persistent.** Restated here as an obligation, not merely a rule.
4. **The provenance block is never dismissible and persists at general availability.** Only the
   release-stage row changes.
5. **Screenshots inherit the notice.** Documentation screenshots taken from a Beta deployment carry
   the banner, and that is correct — do not crop it out.
6. **Definition of Done mapping.** This work touches steps 5 (application), 6 (documentation),
   8 (enhancement log) and 9 (release notes). Steps 2, 3, 4 and 7 are **explicitly not applicable**:
   no onboarding-spec attribute is added, so there is no validator change, no JSON schema change, no
   attribute delta and no agent-skill update.

### 8 Known constraints


---

## Clause 3 — Build and deployment practice


These are not general advice; they are failure modes this repository has actually hit.

- **Run `npm run build` in `databricks-app/web` after touching `web/src`.** Databricks Apps does
  **not** build at deploy time. An un-rebuilt `web/dist/` keeps serving the previous bundle, so the
  Beta banner will simply not appear and the deployment will look successful.
- **Run `node --check` on any file edited under `web/src`.** This repository has twice had a
  broad regular-expression edit damage source — once swallowing roughly 650 lines of `Builder.jsx`.
  Anchor edits on exact line prefixes; never bulk-edit with a broad pattern.
- **`bundle deploy` alone does not update the application.** `bundle run` is required. A deploy that
  reports success while the application serves old code is the single most common false positive in
  this repository.


---

*End of document 03. See [04 — Consolidated rollout runbook](04_consolidated_rollout_runbook.md) for where these practices are applied in sequence.*
