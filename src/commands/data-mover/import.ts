import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { resolve, basename } from 'node:path';
import { SfCommand, Flags } from '@salesforce/sf-plugins-core';
import { Connection, Org } from '@salesforce/core';
import { BulkLoader } from '../../lib/import/bulk-loader.js';
import { ImportTracker } from '../../lib/import/import-tracker.js';
import { CsvPreprocessor, AutoNumberMappings } from '../../lib/import/csv-preprocessor.js';
import { RetryHandler } from '../../lib/import/retry-handler.js';
import { ExportManifest, ManifestFile, ImportLog, DeferredConditionsUpdate, BulkJobInfo } from '../../lib/types.js';

export default class Import extends SfCommand<ImportLog> {
  public static readonly summary = 'Import exported CSVs into a target org via Bulk API 2.0.';

  public static readonly description =
    'Reads a _manifest.json from a previous export, then loads each CSV in tier order ' +
    'using Bulk API 2.0 upsert jobs. Tracks every loaded record for safe rollback. ' +
    'Automatically handles auto-number external ID fields, null external IDs, ' +
    'duplicate external IDs, and CPQ Custom conditions validation.';

  public static readonly examples = [
    'sf data-mover import --target-org ppdev --export-dir ./exports/uat-rules/',
    'sf data-mover import --target-org ppdev --export-dir ./exports/uat-rules/ --dry-run',
    'sf data-mover import --target-org ppdev --export-dir ./exports/uat-rules/ --continue-on-error',
  ];

  public static readonly flags = {
    'target-org': Flags.requiredOrg({
      summary: 'Org to import data into.',
      char: 'o',
      required: true,
    }),
    'export-dir': Flags.string({
      summary: 'Path to the export directory containing _manifest.json and CSV files.',
      char: 'd',
      required: true,
    }),
    'dry-run': Flags.boolean({
      summary: 'Show what would be loaded without actually loading.',
      default: false,
    }),
    'continue-on-error': Flags.boolean({
      summary: 'Continue loading subsequent objects even if one fails.',
      default: false,
    }),
    'max-retries': Flags.integer({
      summary: 'Max retry attempts for transient failures (lock contention, batch save errors).',
      default: 2,
      min: 0,
      max: 5,
    }),
  };

  public async run(): Promise<ImportLog> {
    const { flags } = await this.parse(Import);
    const exportDir = resolve(flags['export-dir']);
    const dryRun = flags['dry-run'];
    const continueOnError = flags['continue-on-error'];

    // 1. Read manifest
    const manifestPath = resolve(exportDir, '_manifest.json');
    let manifest: ExportManifest;
    try {
      manifest = JSON.parse(readFileSync(manifestPath, 'utf-8'));
    } catch {
      this.error(`Could not read manifest at ${manifestPath}. Run 'data-mover export' first.`);
    }

    const org: Org = flags['target-org'];
    const conn = org.getConnection();
    const alias = org.getUsername() ?? 'unknown';

    this.log(`\nTarget org: ${alias}`);
    this.log(`Source:     ${manifest.sourceOrg} (${manifest.recipe})`);
    this.log(`Records:    ${manifest.totalRecords} across ${manifest.tiers.flatMap((t) => t.files).length} files\n`);

    const allFiles = manifest.tiers
      .sort((a, b) => a.tier - b.tier)
      .flatMap((t) => t.files.sort((a, b) => a.order - b.order));

    if (dryRun) {
      this.log('── Dry Run ──');
      this.printPlan(allFiles);
      return { generated: '', targetOrg: alias, recipe: manifest.recipe, sourceExportDir: exportDir, objects: [] };
    }

    const maxRetries = flags['max-retries'];
    const loader = new BulkLoader(conn);
    const preprocessor = new CsvPreprocessor(conn);
    const retryHandler = new RetryHandler(loader, maxRetries);
    const tracker = new ImportTracker(alias, manifest.recipe, exportDir);
    const resultsDir = resolve(exportDir, '_results');
    mkdirSync(resultsDir, { recursive: true });

    const autoNumberMappings: AutoNumberMappings = new Map();
    const deferredConditionsUpdates: DeferredConditionsUpdate[] = [];

    // Pre-import: deactivate rules — extract field name from manifest notes
    const deactivateMap = new Map<string, string>();
    for (const f of allFiles) {
      if (f.notes) {
        const match = f.notes.match(/deactivate (\S+)/);
        if (match) deactivateMap.set(f.sobject, match[1]);
      }
    }

    if (deactivateMap.size > 0) {
      this.log('── Pre-import: Deactivating rules ──');
      for (const [sobject, activeField] of deactivateMap) {
        await this.deactivateRecords(loader, sobject, conn, activeField);
      }
      this.log('');
    }

    // Load each CSV
    let hasFailure = false;
    for (const file of allFiles) {
      const csvPath = resolve(exportDir, file.filename);
      let csvContent: string;
      try {
        csvContent = readFileSync(csvPath, 'utf-8');
      } catch {
        this.warn(`CSV not found: ${csvPath} — skipping`);
        continue;
      }

      const recordCount = countCsvRows(csvContent);
      this.spinner.start(`[${file.order}/${allFiles.length}] ${file.sobject} (${recordCount} records)`);

      try {
        // Step 1: Sanitize Custom conditions (PriceRule, ProductRule)
        const condResult = preprocessor.sanitizeConditionsMet(csvContent, file.sobject, file.externalIdField);
        csvContent = condResult.csvContent;
        if (condResult.deferredUpdates.length > 0) {
          deferredConditionsUpdates.push(...condResult.deferredUpdates);
          this.log(`  deferred ${condResult.deferredUpdates.length} Custom→All conditions (will restore after tier 2)`);
        }

        // Step 2: Main preprocessing (auto-number ext IDs, stripping, rewriting)
        const pp = await preprocessor.preprocess(
          file.sobject, file.externalIdField, csvContent, autoNumberMappings,
        );
        if (pp.strategy === 'id-mapped') {
          this.spinner.status = `${file.sobject} (${pp.existingRecordCount} update / ${pp.newRecordCount} insert)`;
        }
        if (pp.strippedColumns.length > 0) {
          this.log(`  stripped: ${pp.strippedColumns.join(', ')}`);
        }

        // Step 3: Deduplicate by external ID
        const dedup = preprocessor.deduplicateByExternalId(pp.csvContent, pp.externalIdField);
        if (dedup.duplicatesRemoved > 0) {
          this.log(`  deduped: removed ${dedup.duplicatesRemoved} duplicate rows`);
        }

        // Step 4: Split by null ext ID and upload
        const needsSplit = pp.externalIdField
          && pp.externalIdField !== 'Id'
          && pp.strategy === 'direct-upsert';

        if (needsSplit) {
          await this.loadSplitCsv(
            file, dedup.csvContent, pp.externalIdField, loader, preprocessor,
            tracker, retryHandler, resultsDir, autoNumberMappings, recordCount,
          );
        } else {
          await this.loadSingleCsv(
            file, dedup.csvContent, pp, loader, preprocessor,
            tracker, retryHandler, resultsDir, autoNumberMappings, recordCount,
          );
        }

        const lastEntry = tracker.getEntries().at(-1);
        if (lastEntry && lastEntry.status === 'failed') {
          this.spinner.stop(`FAILED — job ${lastEntry.jobId}`);
          hasFailure = true;
          if (!continueOnError) {
            this.error(`Import stopped: ${file.sobject} job failed. Use --continue-on-error to proceed past failures.`);
          }
        } else if (lastEntry && lastEntry.status === 'partial') {
          this.spinner.stop(
            `PARTIAL — ${lastEntry.recordsSucceeded} ok / ${lastEntry.recordsFailed} failed`,
          );
          hasFailure = true;
          if (!continueOnError) {
            this.error(
              `Import stopped: ${file.sobject} had ${lastEntry.recordsFailed} failures. ` +
              `Check _results/ for details. Use --continue-on-error to proceed.`,
            );
          }
        } else {
          const entries = tracker.getEntries().filter((e) => e.csvFile === file.filename);
          const totalOk = entries.reduce((s, e) => s + e.recordsSucceeded, 0);
          const totalFail = entries.reduce((s, e) => s + e.recordsFailed, 0);
          if (totalFail > 0) {
            this.spinner.stop(`${totalOk} ok / ${totalFail} failed`);
            hasFailure = true;
          } else {
            this.spinner.stop(`${totalOk} records loaded`);
          }
        }
      } catch (err) {
        this.spinner.stop('ERROR');
        hasFailure = true;
        const msg = err instanceof Error ? err.message : String(err);
        this.warn(`${file.sobject}: ${msg}`);
        if (!continueOnError) {
          const logPath = tracker.writeLog(exportDir);
          this.log(`\nPartial import log written to ${logPath}`);
          this.error(`Import stopped at ${file.sobject}. Use --continue-on-error to proceed past errors.`);
        }
      }
    }

    // Post-import: restore Custom conditions
    if (deferredConditionsUpdates.length > 0) {
      this.log('\n── Post-import: Restoring Custom conditions ──');
      await this.applyDeferredConditionsUpdates(
        deferredConditionsUpdates, autoNumberMappings, loader, conn,
      );
    }

    // Post-import: reactivate rules
    if (deactivateMap.size > 0) {
      this.log('\n── Post-import: Reactivating rules ──');
      for (const [sobject, activeField] of deactivateMap) {
        await this.reactivateRecords(loader, sobject, conn, activeField);
      }
    }

    // Write import log
    const logPath = tracker.writeLog(exportDir);

    // Print summary table
    this.log('\n── Import Summary ──');
    const entries = tracker.getEntries();
    const tableData = entries.map((e) => ({
      '#': e.order,
      Object: e.sobject,
      Mode: e.externalIdField ? 'upsert' : 'insert',
      Submitted: e.recordsSubmitted,
      Succeeded: e.recordsSucceeded,
      Failed: e.recordsFailed,
      Status: e.status.toUpperCase(),
    }));
    this.table({ data: tableData });

    const totalSucceeded = entries.reduce((s, e) => s + e.recordsSucceeded, 0);
    const totalFailed = entries.reduce((s, e) => s + e.recordsFailed, 0);
    this.log(
      `\nTotal: ${totalSucceeded} succeeded, ${totalFailed} failed`,
    );

    if (hasFailure) {
      this.log(`\n── Failure Details (for admin review) ──`);
      this.log(`Results directory: ${resultsDir}`);
      this.log(`Failure CSVs contain sf__Error column with Salesforce error messages.`);
      const failedEntries = entries.filter((e) => e.recordsFailed > 0);
      for (const e of failedEntries) {
        const mode = e.externalIdField ? 'upsert' : 'insert';
        const tag = basename(e.csvFile, '.csv');
        this.log(`  ${e.sobject} (${mode}): ${e.recordsFailed} failed → ${tag}_${mode}_failed.csv`);
      }
    }

    this.log(`\nImport log: ${logPath}`);

    return JSON.parse(readFileSync(logPath, 'utf-8'));
  }

  /**
   * Load a file that needs splitting: upsert rows with ext ID, insert rows without.
   */
  private async loadSplitCsv(
    file: ManifestFile,
    csvContent: string,
    extIdField: string,
    loader: BulkLoader,
    preprocessor: CsvPreprocessor,
    tracker: ImportTracker,
    retryHandler: RetryHandler,
    resultsDir: string,
    autoNumberMappings: AutoNumberMappings,
    totalRecordCount: number,
  ): Promise<void> {
    const split = preprocessor.splitByExternalId(csvContent, extIdField);
    const tag = basename(file.filename, '.csv');

    // Part A: Upsert rows with ext ID
    if (split.withExtIdCount > 0) {
      this.log(`  upsert: ${split.withExtIdCount} rows (with ${extIdField})`);
      const { jobId, info } = await loader.runUpsert(file.sobject, extIdField, split.withExtId);
      const successCsv = await loader.getSuccessResults(jobId);
      let failedCsv = await loader.getFailedResults(jobId);

      // Retry transient failures
      const retryResult = await this.retryIfNeeded(
        retryHandler, file.sobject, extIdField, failedCsv, info, `${tag}_upsert`,
      );
      if (retryResult) {
        failedCsv = retryResult.permanentFailureCsv;
      }

      tracker.addResult(
        file.order, file.sobject, extIdField, file.filename,
        split.withExtIdCount, jobId, info, successCsv,
      );
      if (retryResult && retryResult.recoveredSfIds.length > 0) {
        tracker.appendSfIds(retryResult.recoveredSfIds);
      }
      tracker.saveResultCsv(resultsDir, `${tag}_upsert_success.csv`, successCsv);
      this.saveAdminFailureCsv(resultsDir, `${tag}_upsert_failed.csv`, failedCsv);
    }

    // Part B: Insert rows without ext ID
    if (split.withoutExtIdCount > 0) {
      this.log(`  insert: ${split.withoutExtIdCount} rows (no ${extIdField})`);
      const { jobId, info } = await loader.runUpsert(file.sobject, null, split.withoutExtId);
      const successCsv = await loader.getSuccessResults(jobId);
      let failedCsv = await loader.getFailedResults(jobId);

      const retryResult = await this.retryIfNeeded(
        retryHandler, file.sobject, null, failedCsv, info, `${tag}_insert`,
      );
      if (retryResult) {
        failedCsv = retryResult.permanentFailureCsv;
      }

      tracker.addResult(
        file.order, file.sobject, null, file.filename,
        split.withoutExtIdCount, jobId, info, successCsv,
      );
      if (retryResult && retryResult.recoveredSfIds.length > 0) {
        tracker.appendSfIds(retryResult.recoveredSfIds);
      }
      tracker.saveResultCsv(resultsDir, `${tag}_insert_success.csv`, successCsv);
      this.saveAdminFailureCsv(resultsDir, `${tag}_insert_failed.csv`, failedCsv);
    }
  }

  /**
   * Load a file as a single upsert job (no splitting needed).
   */
  private async loadSingleCsv(
    file: ManifestFile,
    csvContent: string,
    pp: { externalIdField: string; strategy: string; sourceExtIds: string[] },
    loader: BulkLoader,
    preprocessor: CsvPreprocessor,
    tracker: ImportTracker,
    retryHandler: RetryHandler,
    resultsDir: string,
    autoNumberMappings: AutoNumberMappings,
    recordCount: number,
  ): Promise<void> {
    const { jobId, info } = await loader.runUpsert(file.sobject, pp.externalIdField, csvContent);
    const successCsv = await loader.getSuccessResults(jobId);
    let failedCsv = await loader.getFailedResults(jobId);

    // Build auto-number mapping for child objects if this was an id-mapped parent
    if (pp.strategy === 'id-mapped' && pp.sourceExtIds.length > 0 && file.externalIdField) {
      const objMapping = await preprocessor.buildAutoNumberMapping(
        file.sobject, file.externalIdField, pp.sourceExtIds, successCsv,
      );
      if (objMapping.size > 0) {
        const existing = autoNumberMappings.get(file.externalIdField) ?? new Map();
        for (const [src, tgt] of objMapping) existing.set(src, tgt);
        autoNumberMappings.set(file.externalIdField, existing);
        this.log(`  auto-number mapping: ${objMapping.size} source→target entries`);
      }
    }

    // Retry transient failures
    const tag = basename(file.filename, '.csv');
    const retryResult = await this.retryIfNeeded(
      retryHandler, file.sobject, pp.externalIdField, failedCsv, info, tag,
    );
    if (retryResult) {
      failedCsv = retryResult.permanentFailureCsv;
    }

    tracker.addResult(
      file.order, file.sobject, file.externalIdField, file.filename,
      recordCount, jobId, info, successCsv,
    );
    if (retryResult && retryResult.recoveredSfIds.length > 0) {
      tracker.appendSfIds(retryResult.recoveredSfIds);
    }
    tracker.saveResultCsv(resultsDir, `${tag}_success.csv`, successCsv);
    this.saveAdminFailureCsv(resultsDir, `${tag}_failed.csv`, failedCsv);
  }

  /**
   * Retry failed records if there are transient (retryable) failures.
   */
  private async retryIfNeeded(
    retryHandler: RetryHandler,
    sobject: string,
    extIdField: string | null,
    failedCsv: string,
    info: BulkJobInfo,
    logPrefix: string,
  ) {
    if (info.numberRecordsFailed === 0 || !failedCsv?.trim()) return null;

    const result = await retryHandler.retryFailedRecords(sobject, extIdField, failedCsv);
    if (result.retriedCount > 0) {
      if (result.recoveredCount > 0) {
        this.log(`  retry: recovered ${result.recoveredCount}/${result.retriedCount} transient failures`);
      }
      if (result.stillFailedCount > 0) {
        this.log(`  retry: ${result.stillFailedCount} records still failed after retries`);
      }
    }
    return result;
  }

  /**
   * Save a failure CSV for admin review with a clear filename.
   */
  private saveAdminFailureCsv(resultsDir: string, filename: string, failedCsv: string): void {
    if (!failedCsv || typeof failedCsv !== 'string' || failedCsv.trim().length === 0) return;
    const filePath = resolve(resultsDir, filename);
    writeFileSync(filePath, failedCsv, 'utf-8');
  }

  /**
   * After all tiers are loaded, update rules that had ConditionsMet temporarily
   * changed from 'Custom' to 'All'. Resolves source ext IDs to target IDs
   * via the auto-number mappings built during import.
   */
  private async applyDeferredConditionsUpdates(
    updates: DeferredConditionsUpdate[],
    autoNumberMappings: AutoNumberMappings,
    loader: BulkLoader,
    conn: Connection,
  ): Promise<void> {
    // Group by sobject
    const bySobject = new Map<string, DeferredConditionsUpdate[]>();
    for (const u of updates) {
      const arr = bySobject.get(u.sobject) ?? [];
      arr.push(u);
      bySobject.set(u.sobject, arr);
    }

    for (const [sobject, objUpdates] of bySobject) {
      this.spinner.start(`Restoring ${objUpdates.length} Custom conditions on ${sobject}`);

      // Resolve source ext IDs → target ext IDs using auto-number mappings
      const extIdField = 'CPQ_External_ID__c';
      const mapping = autoNumberMappings.get(extIdField);
      const targetExtIds: string[] = objUpdates.map((u) =>
        mapping?.get(u.sourceExtId) ?? u.sourceExtId,
      );

      // Query target org to get Salesforce IDs
      const idMap = new Map<string, string>();
      const CHUNK = 200;
      for (let i = 0; i < targetExtIds.length; i += CHUNK) {
        const chunk = targetExtIds.slice(i, i + CHUNK);
        const inClause = chunk.map((v) => `'${v}'`).join(',');
        const result = await conn.query<Record<string, string>>(
          `SELECT Id, ${extIdField} FROM ${sobject} WHERE ${extIdField} IN (${inClause})`,
        );
        for (const rec of result.records) {
          idMap.set(rec[extIdField], rec.Id);
        }
      }

      // Build update CSV
      const csvRows = ['Id,SBQQ__ConditionsMet__c,SBQQ__AdvancedCondition__c'];
      let matched = 0;
      for (let i = 0; i < objUpdates.length; i++) {
        const targetExtId = targetExtIds[i];
        const sfId = idMap.get(targetExtId);
        if (!sfId) continue;
        const adv = objUpdates[i].advancedCondition.replace(/"/g, '""');
        csvRows.push(`${sfId},Custom,"${adv}"`);
        matched++;
      }

      if (matched === 0) {
        this.spinner.stop('no matching records found in target');
        continue;
      }

      const csv = csvRows.join('\n');
      const jobId = await loader.createUpsertJob(sobject, 'Id');
      await loader.uploadCsvData(jobId, csv);
      await loader.closeJob(jobId);
      const info = await loader.pollUntilDone(jobId);

      const failed = info.numberRecordsFailed;
      if (failed > 0) {
        const failedCsv = await loader.getFailedResults(jobId);
        this.spinner.stop(`${matched - failed} updated, ${failed} failed`);
        this.warn(`Deferred update failures:\n${failedCsv.substring(0, 500)}`);
      } else {
        this.spinner.stop(`${matched} rules restored to Custom`);
      }
    }
  }

  private printPlan(files: ManifestFile[]): void {
    const tableData = files.map((f) => ({
      Order: f.order,
      Object: f.sobject,
      Records: f.recordCount,
      Operation: f.operation,
      'External ID': f.externalIdField ?? '(none)',
      Notes: f.notes ?? '',
    }));
    this.table({ data: tableData });
    this.log(`\nTotal: ${files.reduce((s, f) => s + f.recordCount, 0)} records`);
  }

  private async deactivateRecords(
    loader: BulkLoader,
    sobject: string,
    conn: Connection,
    activeField: string,
  ): Promise<void> {
    this.spinner.start(`Deactivating ${sobject} (${activeField})`);

    let result: { records: { Id: string }[] };
    try {
      result = await conn.query<{ Id: string }>(
        `SELECT Id FROM ${sobject} WHERE ${activeField} = true`,
      );
    } catch {
      this.spinner.stop(`skipped (${activeField} not found on target)`);
      return;
    }

    if (result.records.length === 0) {
      this.spinner.stop('no active records');
      return;
    }

    const csv = `Id,${activeField}\n` + result.records.map((r) => `${r.Id},false`).join('\n');

    const jobId = await loader.createUpsertJob(sobject, 'Id');
    await loader.uploadCsvData(jobId, csv);
    await loader.closeJob(jobId);
    await loader.pollUntilDone(jobId);

    this.spinner.stop(`${result.records.length} records deactivated`);
  }

  private async reactivateRecords(
    loader: BulkLoader,
    sobject: string,
    conn: Connection,
    activeField: string,
  ): Promise<void> {
    this.spinner.start(`Reactivating ${sobject} (${activeField})`);

    let result: { records: { Id: string }[] };
    try {
      result = await conn.query<{ Id: string }>(
        `SELECT Id FROM ${sobject} WHERE ${activeField} = false`,
      );
    } catch {
      this.spinner.stop(`skipped (${activeField} not found on target)`);
      return;
    }

    if (result.records.length === 0) {
      this.spinner.stop('no records to reactivate');
      return;
    }

    const csv = `Id,${activeField}\n` + result.records.map((r) => `${r.Id},true`).join('\n');

    const jobId = await loader.createUpsertJob(sobject, 'Id');
    await loader.uploadCsvData(jobId, csv);
    await loader.closeJob(jobId);
    await loader.pollUntilDone(jobId);

    this.spinner.stop(`${result.records.length} records reactivated`);
  }
}

function countCsvRows(csv: string): number {
  const clean = csv.charCodeAt(0) === 0xfeff ? csv.slice(1) : csv;
  const lines = clean.trim().split('\n');
  return Math.max(0, lines.length - 1);
}
