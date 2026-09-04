# `resources/flowx_bootstrap/` — the UC containers this bundle's resources live in

This group exists because a first deploy to a fresh workspace failed on its very first resource:

```
Error: cannot create resources.volumes.framework_wheels_volume:
       Schema 'flowx.config' does not exist
```

The bundle declared its Volumes but nothing declared the **schemas** holding them. Those are now
declared in `flowx_schemas.yml`, and DABs rewrites each volume's `schema_name` into a
`${resources.schemas.*.name}` reference — a real dependency edge, so create order schema → volume
is enforced by DABs itself and that error cannot recur.

## The catalog is a PREREQUISITE, not a resource — and this was learned the hard way

`resources/flowx_bootstrap/flowx_catalog.yml` existed briefly on 2026-09-02 and was
**removed the same day**. Declaring the catalog as a bundle resource looks like the natural
completion of the hierarchy. It cannot work on any of this project's workspaces:

```
Error: cannot create resources.catalogs.flowx_catalog:
       Metastore storage root URL does not exist. Default Storage is enabled in your account.
       You can use the UI to create a new catalog using Default Storage, or please provide a
       storage location for the catalog
       (for example 'CREATE CATALOG myCatalog MANAGED LOCATION '<location-path>').
       (400 INVALID_STATE)
```

Two independent reasons, either one sufficient:

1. **These accounts use UC Default Storage.** The metastore has no storage root URL, so
   `CREATE CATALOG` with no `MANAGED LOCATION` is rejected outright. Supplying one is not a fix
   either: a Default-Storage catalog's `storage_root` is an **account-managed** bucket path
   (verified on `arjun_2`:
   `s3://dbstorage-prod-ycljl/uc/9acb37d2-.../3c07299f-.../`) containing metastore and catalog
   UUIDs that are generated at create time. It is not a value that can be written into
   version-controlled YAML, and it differs per workspace.

2. **The catalog already exists on every target anyway** — checked 2026-09-02 on all three:
   `dev_flowx`, `arjun_2` and `hoonartek` all have `flowx`, each created outside this bundle
   (by the UI Default-Storage path). So the create would be wrong even if it were possible.

A resource that can only ever fail is worse than no resource: it fails the deploy *and*, because
DABs propagates the failure down the dependency edges it just created, it takes the schemas with
it — `Error: cannot create resources.schemas.config_schema: dependency failed:
resources.catalogs.flowx_catalog`. Declaring the catalog made the original problem strictly
worse.

**So `${var.catalog}` is a documented prerequisite.** Create it once per workspace in the UI
(Catalog → Create catalog → Default storage), or by SQL if the metastore does have a storage root:

```sql
CREATE CATALOG IF NOT EXISTS flowx;
```

`tests/unit/test_resource_layout.py::test_no_catalog_is_declared_as_a_bundle_resource` asserts the
catalog stays undeclared, and names this file, so the next person to have the same good idea finds
out in a unit test rather than a failed deploy.

## What IS declared here

| File | Resources |
|---|---|
| `flowx_schemas.yml` | `config_schema` (literal `config`), `working_schema` (`${var.schema}`), `sample_suite_schema` (`flowx_sample`) — all `prevent_destroy` |
| `flowx_sample_volumes.yml` | the sample suite's four Volumes: `landing`, `exports`, `observability`, `sample_configs` |

Deliberately **not** declared: the ~40 `bronze_*` / `silver_*` schemas the feature-test and
BT-fixture pipelines name. A Lakeflow pipeline creates its own target schema on first update, so
their absence fails a *pipeline run* at worst, never a deploy — and declaring 40 of them would burn
the schema quota the `dev` target has nearly exhausted (51 schemas, 50-Volume metastore ceiling;
see `databricks.yml`'s LIVE CAVEAT). The declared set is exactly the set whose absence **fails a
deploy**.

## Binding a schema that already exists

`flowx.config` on `dev_flowx` and `hoonartek` predates this group and was created outside the
bundle, so DABs does not own it and a deploy attempting a create fails with
`Schema 'config' already exists`. Bind once per workspace:

```
databricks bundle deployment bind schemas.config_schema flowx.config -t dev_flowx -p dev_flowx
```

`bundle deployment unbind` reverses it without deleting anything. On `arjun_2` no bind is needed —
`config_schema` was created by this bundle on 2026-09-02 and DABs owns it.
