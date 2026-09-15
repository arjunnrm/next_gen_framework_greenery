# 3 · Using the Spec Builder app

A Databricks App that renders the whole attribute registry as a guided form, validates as you go,
and writes the spec where you tell it.

!!! tip "Tab-by-tab walkthrough"
    This page gets you running. Every tab, phase, action and mock screen is walked through in
    [Console → Spec Builder app · tab by tab](../console/spec_builder.md).

## Run it locally

```bash
cd databricks-app
pip install -r requirements.txt
cd web && npm install && npm run build && cd ..
FLOWX_FAKE_DBX=1 uvicorn server.app:app --port 8000
```

`FLOWX_FAKE_DBX=1` stubs every workspace call, so the whole UI works with no Databricks
connection. Drop it to talk to a real workspace.

## Deploy it

```bash
databricks bundle deploy -t dev_flowx
```

!!! warning "Build the frontend before deploying"
    Databricks Apps does not run `npm run build`. The committed `web/dist/` **is** the frontend.

## The layout

| Area | What it does |
|---|---|
| Header tabs | Switch between ingestion, transformation, reconciliation, observability |
| Left rail | Spec root, the flows in the current tab, the not-applicable toggle |
| Centre | The form for the current step, in phases |
| Right | Live JSON/YAML preview of exactly what will be written |
| **Open spec** | Load an existing spec — upload, or browse a Volume/Workspace path |
| **Save** | Download, or write to a Volume or Workspace path |

## Things worth knowing

**The attribute inspector.** The **i** beside any field opens a panel with what the attribute does,
why it matters, a JSON sample, best practice, known errors, and links to Databricks documentation —
the same content as the [JSON reference](../reference/json/index.md), without leaving the form.

**Show attributes not applicable.** Off by default, the form shows only what applies to your current
selections. Turn it on to see everything, with each inapplicable field greyed, marked `N/A`,
read-only, and labelled with the reason. Read-only is deliberate: those values are not written to
the spec, so accepting typing into them would silently lose your input.

**Open an existing spec.** Upload a `.json`/`.yaml`, or type a path and hit **Browse** to navigate,
or **Validate** to parse and check a file without loading it. Import is a lossless inverse of save:
attributes the builder does not recognise are preserved rather than dropped.

**Templates.** The browser lists both built-in presets and everything discovered under
`databricks-app/templates/`. Drop a `.json` file in that directory and it appears on the next load —
no rebuild, no registration. Each entry describes itself from its own content.

## Configuration

`databricks-app/config/index.json` holds storage roots, actions and docs settings.

```json
{
  "spec_storage": {
    "roots": [
      { "id": "vol_specs", "kind": "volume",
        "path": "/Volumes/{{catalog}}/framework/onboarding_specs/",
        "read": true, "write": true }
    ],
    "default_root": "vol_specs",
    "allowed_extensions": [".json", ".yaml", ".yml"]
  }
}
```

Paths support `{{catalog}}` and `{{env}}`. Only listed roots are reachable, and path traversal is
rejected server-side.
