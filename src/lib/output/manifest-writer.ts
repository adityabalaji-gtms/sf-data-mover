import { writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { ExportManifest, ManifestFile, ManifestTier } from '../types.js';

export class ManifestWriter {
  private tiers: ManifestTier[] = [];
  private totalRecords = 0;

  addFile(
    tier: number,
    file: ManifestFile,
  ): void {
    let tierEntry = this.tiers.find((t) => t.tier === tier);
    if (!tierEntry) {
      tierEntry = { tier, files: [] };
      this.tiers.push(tierEntry);
    }
    tierEntry.files.push(file);
    this.totalRecords += file.recordCount;
  }

  write(
    outputDir: string,
    sourceOrg: string,
    recipeName: string,
    mode: 'full' | 'delta' = 'full',
  ): void {
    this.tiers.sort((a, b) => a.tier - b.tier);

    const manifest: ExportManifest = {
      generated: new Date().toISOString(),
      sourceOrg,
      recipe: recipeName,
      mode,
      tiers: this.tiers,
      totalRecords: this.totalRecords,
      instructions: this.buildInstructions(),
    };

    writeFileSync(
      join(outputDir, '_manifest.json'),
      JSON.stringify(manifest, null, 2) + '\n',
      'utf-8'
    );
  }

  private buildInstructions(): string[] {
    const instructions: string[] = [
      'Load CSVs in order (tier-0 first, then tier-1, etc.).',
      'For each CSV, use Salesforce Inspector\'s "Import" tab.',
      'Select the object, choose "Upsert" action, and set the external ID field shown in this manifest.',
      'Self-reference pass-2 files (marked isSelfRefPass2) must be loaded AFTER the main file for that object.',
    ];

    const deactivateObjects = this.tiers
      .flatMap((t) => t.files)
      .filter((f) => f.notes?.includes('deactivate'));

    if (deactivateObjects.length > 0) {
      instructions.push(
        `Before importing, deactivate rules on: ${deactivateObjects.map((f) => f.sobject).join(', ')}.`,
        'After all imports, reactivate those rules.'
      );
    }

    return instructions;
  }
}
