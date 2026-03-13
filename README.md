# sf-data-mover

A Salesforce CLI plugin for migrating data between orgs. Auto-discovers schemas, builds dependency graphs, exports/imports CSVs via Bulk API 2.0 with external ID resolution, and supports rollback.

Built for Salesforce CPQ (SBQQ) and Advanced Approvals (sbaa) configuration, and extended to handle transactional data — Opportunities, Quotes, Quote Lines, Contracts, Orders, OLIs, Contacts, Accounts, and custom objects like DocuSign Status and Shipping Addresses.

## Why This Exists

Migrating Salesforce data between orgs typically involves:
- Salesforce Inspector exports with manual column editing
- Data Loader sheets with hand-resolved IDs
- Praying you loaded parents before children
- Hours of CSV surgery to handle CPQ validation rules, multi-currency PricebookEntries, and cross-org schema differences

This tool automates all of that — schema discovery, dependency ordering, ID resolution, bulk loading, and rollback — into a single CLI workflow. It's been battle-tested across CPQ config migrations and multi-object transactional migrations (100+ Opportunities with full related record trees).

## Features

- **Schema Discovery** — Introspect any org to find objects, relationships, external IDs, and dependency tiers
- **Recipe-based Configuration** — JSON files define what to migrate, with built-in presets for CPQ and transactional data
- **Dependency-aware Export** — Topological sort ensures parents export before children; self-references handled via two-pass strategy
- **External ID Resolution** — Salesforce IDs automatically replaced with external key references for cross-org portability
- **Resolve-Only Objects** — Include objects in export for ID map building without exporting their full data (e.g., `Product2` with auto-number external IDs)
- **Bulk API 2.0 Import** — Upsert/insert with automatic handling of auto-number fields, null external IDs, duplicates, and CPQ validation rules
- **Email Masking** — `--mask-emails` flag appends `.invalid` to all email fields during import, preventing outbound emails from sandboxes. Uses 3-layer detection: schema type, name heuristic, and pattern scan
- **Multi-Currency Support** — Preserves `CurrencyIsoCode` for PricebookEntry resolution in multi-currency orgs, even though the field is non-createable on some objects
- **Cross-Org Schema Handling** — Automatically strips columns missing from the target org schema; compares only fields that exist in both source and target
- **CSV Preprocessing** — Strips non-writable fields, fixes standard reference field prefixes (e.g., `Pricebook2Id`), handles per-object auto-number mappings, and splits PricebookEntry loads by standard vs. custom
- **Cross-Org SF ID Mapping** — For records without external IDs, exports `__sourceId` columns and builds source→target ID maps via row-order correlation during import
- **Org Comparison** — Diff two orgs record-by-record; export only the delta. Smart fingerprint matching for auto-number IDs
- **Selective Rule Export** — Export specific rules by Name or ID along with all related child and upstream records
- **Rollback** — Every import is tracked; roll back by deleting loaded records in reverse dependency order
- **Retry Logic** — Transient failures (lock contention, batch save errors) automatically retried with backoff
- **SOQL Query Chunking** — Splits wide SELECT queries to avoid HTTP 431 errors on objects with many fields

## Prerequisites

- **Node.js** >= 18
- **Salesforce CLI** (`sf`) installed and authenticated to your orgs
- Target objects must have an **external ID field** (e.g., `CPQ_External_ID__c`, `ATGExternalID__c`) for upsert operations

## Getting Started

### 1. Clone and Install

```bash
git clone https://github.com/adityabalaji-gtms/sf-data-mover.git
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

### 8a. Selective Export (Specific Rules)

Export only specific rules and their related records:

```bash
# Export 2 approval rules by name, plus all related conditions, variables, chains, etc.
sf data-mover export --target-org UAT --recipe recipes/approvals.json --output-dir ./exports/selected/ \
  --select-object sbaa__ApprovalRule__c \
  --select "Volume Discount,Tiered Pricing" \
  --match-by name

# Export specific price rules by Salesforce ID
sf data-mover export --target-org UAT --recipe recipes/cpq-rules.json --output-dir ./exports/selected/ \
  --select-object SBQQ__PriceRule__c \
  --select "a0x1P000000ABC,a0x1P000000DEF" \
  --match-by id
```

The `--select` flags work by walking the recipe's dependency graph to discover all child records (conditions, actions) and upstream dependencies (variables, approvers, templates) related to the selected root rules. The resulting CSVs are importable with the standard `import` command — no additional flags needed.

### 8. Import

Load the exported CSVs into the target org:

```bash
sf data-mover import --target-org ppdev --export-dir ./exports/uat-rules/ --continue-on-error
```

For sandbox imports, mask emails to prevent outbound messages:

```bash
sf data-mover import --target-org UAT --export-dir ./exports/prod-opps/ --mask-emails --continue-on-error
```

The import command automatically:
- Deactivates rules before loading (if recipe specifies `preImport.deactivate`)
- Handles auto-number external ID fields (maps source→target IDs)
- Splits records with null external IDs into separate insert jobs
- Deduplicates rows by external ID
- Strips columns that don't exist in the target org or are non-writable
- Preserves `CurrencyIsoCode` for multi-currency PricebookEntry resolution
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

Compare specific rules only (combines `--select` with `--export-delta` for surgical delta migrations):

```bash
sf data-mover compare --source-org UAT --target-org ppdev --recipe recipes/approvals.json \
  --select-object sbaa__ApprovalRule__c \
  --select "Volume Discount" \
  --export-delta ./exports/delta-rules/
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
| `sf data-mover export` | Export data as dependency-ordered CSVs with external ID references (supports `--select` for specific rules) |
| `sf data-mover import` | Import CSVs via Bulk API 2.0 with rollback tracking (supports `--mask-emails`, `--continue-on-error`, `--dry-run`) |
| `sf data-mover compare` | Compare two orgs record-by-record, optionally export delta (supports `--select` for specific rules) |
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
| `cpq-full` | 32 | Full CPQ config: products, pricing, rules, templates, approvals |
| `cpq-rules` | 9 | Price Rules + Product Rules and their children |
| `cpq-products` | — | Product catalog, features, options, pricebook entries |
| `cpq-templates` | — | Quote templates, sections, content, line columns |
| `approvals` | — | Advanced Approvals rules, conditions, chains, approvers |

### Transactional Data Recipes

The `recipes/` directory also includes recipes for migrating transactional/operational data:

| Recipe | Description |
|--------|-------------|
| `opp-migration.json` | Base recipe for Opportunity migrations with related objects |
| `opp-migration-batch2.json` | 50 Closed Won Opportunities with per-object WHERE filters, Contract/Order Status fixups, and Product2 CPQ_External_ID__c auto-number remapping |
| `opp-migration-batch3.json` | 34 specific Opportunities by ID with full related record tree (Account, Contact, Shipping Address, Quote, QuoteLine, OLI, Contract, Order, DocuSign Status) |

These recipes demonstrate the `resolveOnly` and `filter` features for targeted data pulls from Production into sandboxes.

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

#### Object Properties

| Property | Type | Description |
|----------|------|-------------|
| `sobject` | string | API name of the Salesforce object |
| `externalIdField` | string | External ID field for upsert operations |
| `filter` | string | SOQL WHERE clause to limit exported records (e.g., `"Id IN ('001..','001..)')"`) |
| `resolveOnly` | boolean | If `true`, the object is included in the export only for ID map building — no CSV data is exported. Useful for objects like `Product2` or `SBQQ__ProductOption__c` that already exist in both orgs |
| `excludeFields` | string[] | Fields to exclude from export for this object |
| `preImport` | object | Pre-import actions (e.g., `{ "deactivate": "SBQQ__Active__c" }`) |
| `postImport` | object | Post-import actions (e.g., `{ "reactivate": "SBQQ__Active__c" }`) |

#### Transactional Recipe Example

For migrating Opportunities with related records, use `resolveOnly` for reference objects and `filter` for targeted selection:

```json
{
  "name": "Opportunity Migration",
  "version": "1.0",
  "objects": [
    { "sobject": "Product2", "externalIdField": "CPQ_External_ID__c", "resolveOnly": true },
    { "sobject": "SBQQ__ProductOption__c", "externalIdField": "CPQ_External_ID__c", "resolveOnly": true },
    { "sobject": "Account", "externalIdField": "Netsuite_Id__c",
      "filter": "Id IN (SELECT AccountId FROM Opportunity WHERE Id IN ('006...'))" },
    { "sobject": "Opportunity", "externalIdField": "Netsuite_Id__c",
      "filter": "Id IN ('006...')" },
    { "sobject": "SBQQ__Quote__c", "externalIdField": "Netsuite_Id__c",
      "filter": "SBQQ__Opportunity2__r.Id IN ('006...')" },
    { "sobject": "SBQQ__QuoteLine__c", "externalIdField": "Netsuite_Id__c",
      "filter": "SBQQ__Quote__r.SBQQ__Opportunity2__r.Id IN ('006...')" }
  ]
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

## Transactional Data Migration

Beyond CPQ configuration, the tool supports migrating transactional data like Opportunities, Quotes, Contracts, and Orders from Production to sandboxes. This workflow involves additional considerations:

### Typical Workflow

1. **Create a recipe** with targeted `filter` clauses and `resolveOnly` for reference objects
2. **Export** from Production: `sf data-mover export --target-org production --recipe recipes/opp-migration.json --output-dir ./data/batch/`
3. **Clean CSVs** — embedded newlines in text fields can break Bulk API parsing; use a preprocessor to replace `\n`/`\r` with spaces
4. **Pre-process for sandbox** — set `SBQQ__Primary__c` to `FALSE` on Quotes, `Status` to `Draft` on Contracts/Orders, `SBQQ__Contracted__c` to `FALSE` on Orders
5. **Bypass automation** — disable custom triggers, validation rules, and flows via `Global_Automation_Settings__c` or similar before import
6. **Import in phases** — load parent objects first (Accounts, Contacts), then Opportunities, then Quotes/QuoteLines, then OLIs/Contracts/Orders
7. **Post-import fixups** — assign Pricebooks, set Primary Quotes via anonymous Apex
8. **Re-enable automation** — revert bypass settings

### Email Masking

When importing into sandboxes, use `--mask-emails` to append `.invalid` to all email fields. The masking uses three detection layers:
- Schema type (fields typed as `email`)
- Name heuristic (field names containing `email`)
- Pattern scan (values matching email regex)

```bash
sf data-mover import --target-org UAT --export-dir ./data/batch/ --mask-emails --continue-on-error
```

### Common CPQ Gotchas

| Issue | Cause | Resolution |
|-------|-------|------------|
| "Primary quote cannot be changed" | Opportunity has existing Orders | Delete Orders first, or split Quotes into safe/blocked sets |
| "Choose a valid status" on Orders | Orders exported as Activated/Fulfilled | Set `Status` to `Draft` in CSV before import |
| "Can't create a contracted order" | `SBQQ__Contracted__c` is TRUE | Set to `FALSE` in CSV before import |
| "Pricebook entry currency code mismatch" | `CurrencyIsoCode` stripped during preprocessing | Tool now preserves `CurrencyIsoCode` for PBE resolution |
| "You can't select products" on OLIs | Opportunity missing `Pricebook2Id` | Assign Pricebook via post-import Apex fixup |
| STRING_TOO_LONG on text fields | Field limits differ between Production and sandbox | Truncate values in CSV to match target org limits |

## Utility Scripts

| Script | Description |
|--------|-------------|
| `scripts/backfill-external-ids.apex` | Apex script to populate external ID fields on existing records |
| `scripts/strip-columns.py` | Python utility to strip problematic columns from exported CSVs when recipe `excludeFields` weren't applied at export time |

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
    ├── rules/               # Selective rule filtering
    │   └── rule-filter.ts   # Graph-walking filter for specific rules
    ├── output/              # Export file writers
    │   ├── csv-writer.ts    # CSV file output
    │   └── manifest-writer.ts # _manifest.json generation
    └── recipe/              # Recipe management
        ├── builder.ts       # Recipe construction from schema
        ├── presets.ts       # Built-in recipe presets
        └── schema.ts        # Recipe JSON validation
```

## Typical Workflows

### Config Migration (CPQ Rules, Approvals)

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

### Transactional Migration (Opportunities → Sandbox)

```
Production                                 Target Sandbox (UAT/INT)
      │                                          │
      ├── export (recipe w/ filters) ──→ CSVs    │
      │                                   │      │
      │                         preprocess CSVs   │
      │                         (clean, fixup)    │
      │                                   │      │
      │                         bypass automation  │
      │                                   │      │
      │               Phase 1: import parents ──→ │ (Account, Contact, Opp)
      │               Phase 2: import children ──→│ (Quote, QL, OLI, Order)
      │                                   │      │
      │                         post-import fixup  │
      │                         (Pricebook, PQ)    │
      │                                   │      │
      │                         re-enable automtn  │
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

## Changelog

### v3 — Batch Transactional Migrations (March 2026)
- Opportunity migration recipes with `filter` and `resolveOnly` support
- `--mask-emails` flag for sandbox-safe imports
- `CurrencyIsoCode` preservation for multi-currency PricebookEntry resolution
- CSV `strip-columns.py` utility for post-export column removal
- Per-object WHERE filter support in recipes
- Embedded newline handling in CSV preprocessing

### v2 — Production-to-Sandbox + Selective Export (Feb–March 2026)
- Cross-org schema-aware CSV preprocessing (strip missing/non-writable fields)
- `resolveOnly` recipe flag for ID map building without data export
- Selective rule export: `--select-object`, `--select`, `--match-by` flags
- Cross-org SF ID mapping via `__sourceId` columns
- SOQL query chunking to avoid HTTP 431
- Production-to-sandbox CPQ migration fixes (4.3% → 98% success rate)
- Smart diff with fingerprint matching for auto-number IDs

### v1 — Initial Release (Feb 2026)
- Schema discovery, dependency graphing, topological sort
- Recipe-based export/import with Bulk API 2.0
- External ID resolution and auto-number field handling
- Org comparison with delta export
- Rollback support with import tracking
- Built-in CPQ and Advanced Approvals presets

## License

MIT
