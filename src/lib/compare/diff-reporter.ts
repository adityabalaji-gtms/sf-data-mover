import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { DiffResult } from '../types.js';

/**
 * Formats diff results as a console summary table and writes a detailed JSON report.
 */
export class DiffReporter {
  /**
   * Returns a formatted console table string.
   */
  formatSummaryTable(result: DiffResult): string {
    const lines: string[] = [];
    const header = [
      'Object'.padEnd(40),
      'Match'.padEnd(14),
      'Source'.padStart(8),
      'Target'.padStart(8),
      'New'.padStart(8),
      'Modified'.padStart(10),
      'Deleted'.padStart(9),
      'Identical'.padStart(10),
    ].join('');

    lines.push(header);
    lines.push('─'.repeat(header.length));

    for (const [sobject, diff] of Object.entries(result.objects)) {
      const strategy = diff.matchStrategy === 'fingerprint' ? 'fingerprint' : 'external-id';
      lines.push([
        sobject.padEnd(40),
        strategy.padEnd(14),
        String(diff.counts.source).padStart(8),
        String(diff.counts.target).padStart(8),
        String(diff.counts.new).padStart(8),
        String(diff.counts.modified).padStart(10),
        String(diff.counts.deleted).padStart(9),
        String(diff.counts.identical).padStart(10),
      ].join(''));
    }

    lines.push('─'.repeat(header.length));

    const totals = result.summary;
    const totalSource = Object.values(result.objects).reduce((s, d) => s + d.counts.source, 0);
    const totalTarget = Object.values(result.objects).reduce((s, d) => s + d.counts.target, 0);

    lines.push([
      'TOTAL'.padEnd(40),
      ''.padEnd(14),
      String(totalSource).padStart(8),
      String(totalTarget).padStart(8),
      String(totals.totalNew).padStart(8),
      String(totals.totalModified).padStart(10),
      String(totals.totalDeleted).padStart(9),
      String(totals.totalIdentical).padStart(10),
    ].join(''));

    return lines.join('\n');
  }

  /**
   * Write the full diff report as JSON.
   */
  writeReport(outputDir: string, result: DiffResult): string {
    const filePath = join(outputDir, '_diff-report.json');
    writeFileSync(filePath, JSON.stringify(result, null, 2) + '\n', 'utf-8');
    return filePath;
  }
}
