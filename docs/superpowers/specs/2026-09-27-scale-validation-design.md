# pipetree scale validation — design

## Context

pipetree (Phases A–F) is feature-complete and unit-tested (317 tests), but
almost none of that testing touched real infrastructure: `sqlserver` and
`kusto` sources are verified only against hand-written fakes, the
Databricks/Fabric platform seam has never run against a real workspace,
and nothing has exercised more than a handful of tables or more than a
few megabytes of data. This project closes that gap: a realistic
enterprise-scale pipeline — ~100 source tables across ~10 systems, 1TB
and 10TB of data, five layers ending in a proper Kimball star schema —
run for real on both Databricks and Fabric.

It also directly serves the blog series: parts 3 and 4 compare pipetree
on Fabric vs. Databricks, and this project is where that comparison
actually happens, with a real report to draw on instead of a
hand-waved claim.

**Non-goals:** changing pipetree itself (this is purely a consumer of
what's already built - if it uncovers a real bug, that's a pipetree
fix tracked separately, not a reason to redesign this harness); a
polished, reusable "load testing framework" for arbitrary packages;
covering every possible Azure service (the topology below is
deliberately bounded to what exercises pipetree's existing source
types).

## Repository

A **new, private** repository (suggested name: `pipetree-scale-bench`;
easy to rename before creation) depending on `pipetree` as a package.
Kept separate from `pipetree` itself because its lifecycle (generate
once, validate repeatedly, tear down) and its dependencies (OpenTofu,
dbldatagen, cloud SDKs, real multi-TB fixtures) have nothing to do with
pipetree's own release cycle - the same "one repo per real thing"
convention already used for pipetree.

**Why private:** the repo holds infrastructure tied to a real Azure
subscription. Even with OpenTofu state kept entirely out of git (a
remote backend, never local state files), `.tfvars` files and module
outputs tend to accumulate subscription/tenant/resource-naming details
that are easy to miss before a public push. This is different from
pipetree, which *is* the thing being open-sourced; this is validation
tooling around it. Nothing prevents making it public later once
confirmed clean of anything subscription-specific - that direction is
reversible, the other isn't.

Top-level layout:

```
pipetree-scale-bench/
  generation/          dbldatagen table specs, one module per system
  infra/               OpenTofu (see "Infrastructure")
  pipelines/           pipetree YAML configs (one per scale tier, sharing
                        table definitions via YAML anchors or a small
                        generator script - see "Execution")
  runners/
    databricks/        DAB + entrypoint, modeled on pipetree's own
                        examples/databricks/
    fabric/             notebook, modeled on pipetree's own
                        examples/fabric/
  reports/             generated load-test reports (see "Validation")
  scripts/             generate.py, run.py, teardown.py - the operator
                        entrypoints tying the above together
```

## Source topology

~10 systems, ~100 tables, each mapped to whichever real Azure service
lets it exercise a specific pipetree source type:

| System | Source type(s) | Real service | Tables (~) | Domain |
|---|---|---|---|---|
| `erp` | `d365_export` | ADLS Gen2 (files) | 10 | Sales orders, GL, inventory, vendors |
| `crm` | `synapse_link` | ADLS Gen2 (files) | 10 | Accounts, contacts, opportunities, cases |
| `hr` | `sqlserver` | Azure SQL DB, schema `hr` | 8 | Employees, departments, payroll |
| `finance` | `sqlserver` | Azure SQL DB, schema `finance` | 8 | Invoices, payments, budgets |
| `support` | `sqlserver` | Azure SQL DB, schema `support` | 6 | Tickets, SLAs, agents |
| `web_events` | `kusto` **and** `storage_stream` | Event Hub → Log Analytics Workspace (hot path) **and** → ADLS via Event Hub Capture (cold path) | 6 | Clickstream, sessions |
| `iot_telemetry` | `kusto` **and** `storage_stream` | Event Hub → Log Analytics Workspace **and** → ADLS via Capture | 6 | Device/sensor readings |
| `marketing` | `custom` | Azure Function (Consumption), a tiny mock REST API | 6 | Campaigns, ad spend, email events |
| `supply_chain` | `storage_stream` | ADLS Gen2 (files) | 8 | Shipments, warehouses, purchase orders |
| `reference` | `csv` | ADLS Gen2 (files) | 8 | Currencies, calendar, product/org hierarchy |

Total: ~76 base tables; the two `kusto`/`storage_stream` doubled systems
(`web_events`, `iot_telemetry` each counted once above but readable two
ways) and headroom in `erp`/`crm` bring the practical total to ~100
`pipetree` table blocks once bronze definitions for both read paths are
counted.

One Azure SQL Database (three schemas) and one Log Analytics Workspace
(multiple custom tables via Data Collection Rules) rather than one
service per system - cheaper, and realistic: consolidating multiple
line-of-business systems onto shared infrastructure is exactly what
real organizations do.

`web_events`/`iot_telemetry` reading the *same* underlying stream two
ways (`kusto` against Log Analytics, `storage_stream` against the
Capture output in ADLS) is deliberate: it's a real side-by-side
correctness check between two of pipetree's source types against
identical data, not just two independent smoke tests.

`marketing`'s mock API gives the `custom` `SourceReader` seam (built in
Phase D, never exercised against anything real) an actual workout
without needing a real Microsoft Graph tenant.

## Layers

Five layers, matching pipetree's own layer-as-namespace model (arbitrary
top-level YAML keys, never execution barriers):

1. **`bronze`** - raw per-source landing, one table per source table.
2. **`silver`** - cleansed and conformed per source system (dedup, type
   normalization, business keys standardized) - still one silver table
   per source, not yet merged across systems.
3. **`integration`** - cross-source identity resolution: `erp`'s
   customer, `crm`'s account, and `web_events`' visitor become one
   conformed `customer` entity via match keys. This is the layer real
   enterprises almost always have and the layer most likely to stress
   `depends_on: auto` the hardest, with many-to-one fan-in across
   systems.
4. **`gold`** - the star schema. Dimensions: `dim_customer`,
   `dim_product`, `dim_employee`, `dim_date`, `dim_geography`,
   `dim_supplier` - all `scd2` with `unknown_member: true`. Facts:
   `fact_sales`, `fact_orders`, `fact_web_events`,
   `fact_support_tickets`, `fact_inventory_movements`,
   `fact_marketing_spend` - resolving dimension foreign keys through the
   unknown-member pattern rather than leaving nulls.
5. **`mart`** - domain-specific aggregates on top of the shared gold
   layer (e.g. `sales_mart.monthly_revenue_by_region`,
   `finance_mart.cash_flow_summary`) - the presentation/semantic tier a
   real BI consumer would actually query.

## Data generation

**Tool:** [dbldatagen](https://github.com/databrickslabs/dbldatagen)
(Databricks Labs), run as a Databricks job - distributed generation is
the only realistic way to reach 1-10TB in reasonable time. Referential
integrity comes from dbldatagen's join-based FK generation (a fact
table's foreign keys sampled from a dimension's already-generated key
range), not randomly assigned orphaned keys.

**Sizing model.** `scale_factor` is the approximate total data size in
GB (`5` for dev, `1000` for the 1TB tier, `10000` for the 10TB tier),
split across three non-uniform size classes rather than spread evenly
across all ~100 tables - real data never distributes evenly either:

- **Huge** (~70% of `scale_factor`, ~15-20 tables: `web_events`,
  `iot_telemetry`, `erp.SalesOrderHeader`/lines, high-volume
  transactional tables) - the tables that actually make this a
  multi-terabyte exercise.
- **Medium** (~25%, ~40 tables: CRM opportunities, support tickets,
  finance invoices, supply-chain shipments) - realistic transactional
  volume, growing with scale but far more slowly than the huge class.
- **Small** (remaining ~5%, ~35 tables: `hr`, `reference`, `marketing`)
  - fixed row counts (roughly 1K-100K rows) regardless of scale factor.
  Dimension and reference data doesn't grow 2000x just because the
  event stream did, and `marketing`'s mock API specifically never
  serves TB-scale payloads - real REST APIs don't either.

**Destinations**, per system: `hr`/`finance`/`support` write via JDBC to
Azure SQL DB; `erp`/`crm`/`supply_chain`/`reference` write as files to
ADLS Gen2; `web_events`/`iot_telemetry` are produced onto Event Hub,
which fans out to Log Analytics (via Data Collection Rules) and to ADLS
(via Event Hub Capture); `marketing`'s small fixed dataset seeds the
mock Function once.

**Generation is a separate, resumable step from running pipetree.**
Each scale tier's data is generated once, real money and time, and then
pipetree runs against that fixed dataset as many times as needed while
fixing bugs - re-running pipetree never re-generates data.

## Infrastructure (OpenTofu)

One parameterized root module, `scale_tier` (`dev` / `tier1` / `tier2`)
selecting SKUs and sizes throughout:

```
infra/
  backend.tf        remote state - an Azure Storage container dedicated
                     to state, never local state files
  main.tf
  variables.tf       scale_tier, location, ...
  outputs.tf
  modules/
    storage/         ADLS Gen2 (hierarchical namespace) + Unity Catalog
                      external location over it
    sql/             Azure SQL Server + Database (SKU by scale_tier:
                      Standard/GP for dev, Business Critical/Hyperscale
                      for tier1/tier2)
    loganalytics/    workspace + Data Collection Rules for the custom
                      web_events/iot_telemetry tables
    eventhub/        namespace + hubs + Capture-to-ADLS configuration
    function/        Consumption-plan Function App, the mock marketing API
    databricks/      workspace (or points at an existing one - open
                      question, see "Open risks")
    fabric/          capacity + Lakehouse + a OneLake shortcut to the
                      ADLS storage account - **not** a second copy of
                      the data
  environments/
    dev.tfvars
    tier1.tfvars
    tier2.tfvars
```

**Single copy of the data across both platforms.** Fabric reads the
same ADLS Gen2 account via a OneLake shortcut rather than re-landing
1-10TB into OneLake natively - this is the single biggest cost lever in
the whole design, since generating and storing the data twice would
roughly double both generation time and storage cost for no benefit.

**Cost control.** `scale_tier=dev` stays up for iteration; `tier1`/
`tier2` environments are stood up, validated, their report captured, and
torn down (`tofu destroy`, or at minimum pausing the SQL DB, stopping
the Databricks cluster, and pausing the Fabric capacity) before moving
on. Nothing runs unattended at 1TB/10TB scale.

## Execution

**No changes to pipetree.** One set of `pipeline.yaml` table
definitions (shared across scale tiers via a small generator script or
YAML anchors, since only data volume changes between tiers, not schema)
describes all ~100 tables using pipetree's existing schema exactly as
designed.

**Databricks run** reuses the pattern from pipetree's own
`examples/databricks/`: a Databricks Asset Bundle with a
`spark_python_task`, `DatabricksPlatform` wired to the real Unity
Catalog catalog and a Unity-Catalog-backed secret scope, sized to a real
job cluster matching the scale tier.

**Fabric run** reuses the pattern from `examples/fabric/`: a notebook,
`FabricPlatform` wired to a real Key Vault, reading the identical lake
data via the OneLake shortcut Databricks also reads.

Both platforms are expected to produce the **same** bronze-through-mart
output from the same YAML - that comparison *is* the validation parts
3/4 of the blog series want, not a side effect of this project.

This is also the first real-workspace exercise of the `sqlserver` and
`kusto` readers, and of `DatabricksPlatform`/`FabricPlatform` themselves
- expect to find and fix real issues here that no fake could have
caught, particularly around JDBC connection specifics and the exact
cluster URI format for querying a Log Analytics Workspace through the
Kusto Spark connector (see "Open risks").

## Validation & reporting

Three things captured per scale tier, into one report
(`reports/<scale_tier>-<date>.md`):

- **Correctness**: row counts and referential integrity hold through to
  `gold`/`mart`; the star schema is actually queryable (a handful of
  representative BI-style queries against `gold`/`mart` as a sanity
  check, not just "the job exited zero"); the run digest shows zero
  unexpected `failed`/`upstream_failed` (deliberately injected
  unknown-member cases aside).
- **Performance**: per-layer and total wall-clock time from
  `_meta.pipeline_run_log`; how the executor's retry/backoff behaves
  under genuine transient conditions (Azure SQL throttling, Event Hub
  backpressure) rather than `FaultInjectingAdapter`'s simulated ones -
  this is real evidence for part 2's retry design, not just the local
  demo's.
- **Cost**: actual Azure spend for the run (Cost Management API, or SKU
  × duration if that's simpler), captured alongside the results so
  "did it work" and "what did it cost" are answered together.

## Rollout

1. **`dev` on Databricks only** - fastest iteration loop; where
   `sqlserver`/`kusto` connectivity problems get found and fixed cheaply,
   before any real money is at stake.
2. **`dev` on Fabric** - same generated data, prove the Fabric path,
   diff its output against Databricks'.
3. **`tier1` (~1TB) on both platforms** - first real-scale run, full
   report, then tear down.
4. **`tier2` (~10TB) on both platforms** - final validation, larger
   SKUs, full report, then tear down.

Each tier only starts once the previous one's correctness checks pass -
scaling up a broken pipeline just makes the bug more expensive to find.

## Open risks (flagged, not resolved here)

- **Fabric's OpenTofu/Terraform provider (`microsoft/fabric`) is
  relatively new.** Capacity and workspace provisioning are likely
  supported; whether the OneLake shortcut itself can be created via the
  provider, or needs the Fabric REST API / manual setup as a fallback,
  needs checking against current provider docs before the `fabric`
  module is built.
- **The Kusto Spark connector against a Log Analytics Workspace** uses
  a cross-service query URI (the `ade.loganalytics.io/...` form) rather
  than a standalone ADX cluster's URI - `KustoSource` should work
  unchanged once that URI is used as `cluster`, but this hasn't been
  tried against a real workspace yet.
- **Whether to provision a new Databricks workspace or reuse an
  existing one** is left as a variable in the `databricks` module
  rather than decided here.
- **Exact dbldatagen specs for ~100 tables** are real, substantial
  authoring work not detailed in this design - the implementation plan
  should treat "write the generation spec for system X" as its own
  right-sized unit of work per system, not one monolithic task.
