import { writeFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { stringify } from 'csv-stringify/sync';

/**
 * Writes Inspector-compatible CSV files.
 * - UTF-8 with BOM (so Excel opens correctly)
 * - Proper quoting for fields containing commas or newlines
 * - Relationship notation headers (e.g. SBQQ__Rule__r.CPQ_External_ID__c)
 */
export class CsvWriter {
  write(
    outputDir: string,
    tierDir: string,
    filename: string,
    headers: string[],
    rows: Record<string, unknown>[],
  ): string {
    const dir = join(outputDir, tierDir);
    mkdirSync(dir, { recursive: true });

    const filePath = join(dir, filename);

    const data = rows.map((row) =>
      headers.map((h) => {
        const val = row[h];
        if (val === null || val === undefined) return '';
        if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
        return String(val);
      })
    );

    const csv = stringify([headers, ...data], {
      quoted: true,
      quoted_empty: false,
    });

    const bom = '\uFEFF';
    writeFileSync(filePath, bom + csv, 'utf-8');

    return filePath;
  }
}
