# sf-data-mover

A Salesforce CLI plugin for migrating configuration data between orgs. Auto-discovers schemas, builds dependency graphs, exports/imports CSVs via Bulk API 2.0 with external ID resolution, and supports rollback.

Built for Salesforce CPQ (SBQQ) and Advanced Approvals (sbaa) but works with any standard or custom objects.

## Why This Exists

Migrating CPQ configuration between sandboxes (dev → int → uat → production) typically involves:
- Salesforce Inspector exports with manual column editing
- Data Loader sheets with hand-resolved IDs
- Praying you loaded parents before children

This tool automates all of that — schema discovery, dependency ordering, ID resolution, bulk loading, and rollback — into a single CLI workflow.

## Features

- **Schema Discovery** — Introspect any org to find objects, relationships, external IDs, and dependency tiers
- **Recipe-based Configuration** — JSON files define what to migrate, with built-in presets for CPQ
- **Dependency-aware Export** — Topological sort ensures parents export before children; self-references handled via two-pass strategy
- **External ID Resolution** — Salesforce IDs automatically replaced with external key references for cross-org portability
- **Bulk API 2.0 Import** — Upsert/insert with automatic handling of auto-number fields, null external IDs, duplicates, and CPQ validation rules
- **Org Comparison** — Diff two orgs record-by-record; export only the delta
- **Rollback** — Every import is tracked; roll back by deleting loaded records in reverse dependency order
- **Retry Logic** — Transient failures (lock contention, batch save errors) automatically retried with backoff

## Prerequisites

- **Node.js** >= 18
- **Salesforce CLI** (`sf`) installed and authenticated to your orgs
- Target objects must have an **external ID field** (e.g., `CPQ_External_ID__c`, `ATGExternalID__c`) for upsert operations

## Getting Started

### 1. Clone and Install

```bash
git clone <repo-url> sf-data-mover
cd sf-data-mover
npm install
```

### 2. Build

```bash
npm run build
```

### 3. Link as an SF Plugin

```bash
sf plugins link .
```

This registers `sf data-mover` commands in your Salesforce CLI. Verify with:

```bash
sf data-mover --help
```

### 4. Authenticate Your Orgs

Make sure you have at least a source and target org authenticated.

**Production / Developer Edition** (login.salesforce.com):

```bash
sf org login web --alias production
```

**Sandboxes** (test.salesforce.com):

```bash
sf org login web --alias UAT --instance-url https://test.salesforce.com
sf org login web --alias ppdev --instance-url https://test.salesforce.com
```

**Custom / My Domain** (e.g., `mycompany.my.salesforce.com`):

```bash
sf org login web --alias UAT --instance-url https://mycompany--uat.sandbox.my.salesforce.com
```

Verify your authenticated orgs:

```bash
sf org list
```

### 5. Discover Your Source Org

See what objects, relationships, and dependency tiers exist:

```bash
sf data-mover discover --target-org UAT --filter "SBQQ__,sbaa__,Product2"
```

This outputs a tiered dependency view showing record counts, external ID fields, and self-references.

### 6. Plan the Migration

Preview what would be exported using a recipe:

```bash
sf data-mover plan --recipe recipes/cpq-rules.json --target-org UAT
```

### 7. Export

Export data from the source org as dependency-ordered CSVs:

```bash
sf data-mover export --target-org UAT --recipe recipes/cpq-rules.json --output-dir ./exports/uat-rules/
```

This creates:
- Tiered CSV files with external ID references (no Salesforce IDs)
- A `_manifest.json` describing load order, operations, and record counts

### 8. Import

Load the exported CSVs into the target org:

```bash
sf data-mover import --target-org ppdev --export-dir ./exports/uat-rules/ --continue-on-error
```

The import command automatically:
- Deactivates rules before loading (if recipe specifies `preImport.deactivate`)
- Handles auto-number external ID fields (maps source→target IDs)
- Splits records with null external IDs into separate insert jobs
- Deduplicates rows by external ID
- Sanitizes CPQ "Custom" conditions (2-pass: load as "All", restore after conditions are loaded)
- Retries transient failures (lock contention) with exponential backoff
- Reactivates rules after loading
- Writes an `_import-log.json` for rollback and an `_results/` directory with per-object success/failure CSVs

Use `--dry-run` to preview without loading:

```bash
sf data-mover import --target-org ppdev --export-dir ./exports/uat-rules/ --dry-run
```

### 9. Compare Orgs

Diff two orgs to see what's different:

```bash
sf data-mover compare --source-org UAT --target-org ppdev --recipe recipes/cpq-rules.json
```

Export only the delta (new + modified records):

```bash
sf data-mover compare --source-org UAT --target-org ppdev --recipe recipes/cpq-rules.json --export-delta ./exports/delta/
```

### 10. Rollback

If something goes wrong, roll back an import using the tracked log:

```bash
sf data-mover rollback --target-org ppdev --import-log ./exports/uat-rules/_import-log.json
```

Use `--dry-run` first to see what would be deleted:

```bash
sf data-mover rollback --target-org ppdev --import-log ./exports/uat-rules/_import-log.json --dry-run
```

## Commands

| Command | Description |
|---------|-------------|
| `sf data-mover discover` | Discover schemas, relationships, and dependency tiers in an org |
| `sf data-mover plan` | Dry-run: show what would be exported and in what order |
| `sf data-mover export` | Export data as dependency-ordered CSVs with external ID references |
| `sf data-mover import` | Import CSVs via Bulk API 2.0 with rollback tracking |
| `sf data-mover compare` | Compare two orgs record-by-record, optionally export delta |
| `sf data-mover rollback` | Delete previously imported records in reverse dependency order |
| `sf data-mover recipe create` | Create a recipe from a preset or custom object selection |
| `sf data-mover recipe validate` | Validate a recipe JSON file |

Run any command with `--help` for full flag documentation.

## Recipes

Recipes are JSON files that define what to migrate. They specify:
- Which objects to include
- External ID fields for upsert
- Optional filters (SOQL WHERE clauses)
- Pre/post import actions (deactivate/reactivate rules)
- Settings like batch size and self-reference strategy

### Built-in Presets

| Preset | Objects | Description |
|--------|---------|-------------|
| `cpq-full` | 31 | Full CPQ config: products, pricing, rules, templates, approvals |
| `cpq-rules` | 9 | Price Rules + Product Rules and their children |
| `cpq-products` | — | Product catalog, features, options, pricebook entries |
| `cpq-templates` | — | Quote templates, sections, content, line columns |
| `approvals` | — | Advanced Approvals rules, conditions, chains, approvers |

Create a recipe from a preset:

```bash
sf data-mover recipe create --target-org UAT --preset cpq-rules --output recipes/my-rules.json
```

Or from a custom object list:

```bash
sf data-mover recipe create --target-org UAT --objects "Product2,SBQQ__PriceRule__c" --output recipes/custom.json
```

### Recipe Format

```json
{
  "name": "CPQ Rules",
  "version": "1.0",
  "description": "Price Rules + Product Rules and their children.",
  "objects": [
    {
      "sobject": "SBQQ__PriceRule__c",
      "externalIdField": "CPQ_External_ID__c",
      "preImport": { "deactivate": "SBQQ__Active__c" },
      "postImport": { "reactivate": "SBQQ__Active__c" }
    },
    {
      "sobject": "SBQQ__PriceCondition__c",
      "externalIdField": "ATGExternalID__c"
    }
  ],
  "settings": {
    "defaultExcludeFields": ["Id", "IsDeleted", "CreatedDate", "CreatedById", "LastModifiedDate", "LastModifiedById", "SystemModstamp"],
    "batchSize": 200,
    "selfReferenceStrategy": "two-pass"
  }
}
```

## External ID Setup

Objects need an external ID field for upsert operations. If your objects don't have one:

1. **Deploy the field** — Create a custom text field marked as "External ID" and "Unique" (e.g., `CPQ_External_ID__c`)
2. **Backfill existing records** — Run the included Apex script to populate the field:

```bash
sf apex run --file scripts/backfill-external-ids.apex --target-org UAT
```

The backfill script generates IDs in the format `<PREFIX>-<15charSalesforceId>`, ensuring uniqueness across orgs.

For objects with auto-number external IDs (like `Name` on `SBQQ__SummaryVariable__c`), the tool automatically detects non-writable fields and uses an ID-mapping strategy instead of direct upsert.

## Architecture

```
src/
├── commands/data-mover/     # CLI command definitions (oclif)
│   ├── compare.ts           # Org-to-org diff
│   ├── discover.ts          # Schema introspection
│   ├── export.ts            # CSV export with ID resolution
│   ├── import.ts            # Bulk API 2.0 import orchestrator
│   ├── plan.ts              # Dry-run planning
│   ├── rollback.ts          # Reverse-order deletion
│   └── recipe/              # Recipe management commands
└── lib/
    ├── types.ts             # Shared TypeScript interfaces
    ├── schema/              # Schema discovery and dependency graphing
    │   ├── describer.ts     # Salesforce describeGlobal/describeSObject
    │   ├── graph.ts         # Relationship graph builder
    │   ├── sorter.ts        # Topological sort for load order
    │   └── analyzer.ts      # External ID gap analysis
    ├── data/                # Data fetching and ID resolution
    │   ├── data-fetcher.ts  # SOQL query execution
    │   ├── id-resolver.ts   # Salesforce ID → external key mapping
    │   └── query-builder.ts # Dynamic SOQL generation
    ├── import/              # Bulk API import pipeline
    │   ├── bulk-loader.ts   # Bulk API 2.0 wrapper
    │   ├── csv-preprocessor.ts  # CSV transforms (auto-numbers, dedup, split)
    │   ├── import-tracker.ts    # Import log for rollback
    │   └── retry-handler.ts     # Transient failure retry with backoff
    ├── compare/             # Org comparison engine
    │   ├── diff-engine.ts   # Record-level diff by external ID
    │   ├── diff-reporter.ts # Summary tables and reports
    │   └── delta-exporter.ts # Export only changed records
    ├── output/              # Export file writers
    │   ├── csv-writer.ts    # CSV file output
    │   └── manifest-writer.ts # _manifest.json generation
    └── recipe/              # Recipe management
        ├── builder.ts       # Recipe construction from schema
        ├── presets.ts       # Built-in recipe presets
        └── schema.ts        # Recipe JSON validation
```

## Typical Workflow

```
Source Org (UAT)                           Target Org (ppdev)
      │                                          │
      ├── discover ──→ understand schema          │
      ├── plan ──────→ preview export scope        │
      ├── export ────→ CSVs + manifest             │
      │                    │                       │
      │                    └──→ import ──────────→ │
      │                                            │
      ├── compare ←──────────────────────────────→ │
      │     └── export-delta → incremental CSVs    │
      │                            │               │
      │                            └──→ import ──→ │
      │                                            │
      │                    rollback ←─────────────→ │
```

## Development

```bash
npm run build      # Compile TypeScript
npm run lint       # Type-check without emitting
npm run clean      # Remove dist/
```

To test changes without rebuilding each time, use the dev entry point:

```bash
./bin/dev.ts data-mover discover --target-org UAT --filter "SBQQ__"
```

## License

MIT
