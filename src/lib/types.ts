// ---------------------------------------------------------------------------
// Schema types — mirrors what we get from sf sobject describe
// ---------------------------------------------------------------------------

export interface FieldDescribe {
  name: string;
  label: string;
  type: string;
  referenceTo: string[];
  relationshipName: string | null;
  externalId: boolean;
  nillable: boolean;
  createable: boolean;
  updateable: boolean;
  calculated: boolean;
  autoNumber: boolean;
  custom: boolean;
  defaultedOnCreate: boolean;
  length: number;
}

export interface ObjectDescribe {
  name: string;
  label: string;
  fields: FieldDescribe[];
  childRelationships: { childSObject: string; field: string; relationshipName: string }[];
}

// ---------------------------------------------------------------------------
// Dependency graph
// ---------------------------------------------------------------------------

export interface GraphNode {
  sobject: string;
  label: string;
  tier: number;
  externalIdFields: string[];
  selfReferences: SelfReference[];
  recordCount: number;
  referenceFields: ReferenceField[];
}

export interface SelfReference {
  field: string;
  relationshipName: string | null;
}

export interface ReferenceField {
  field: string;
  relationshipName: string | null;
  referenceTo: string;
  externalIdOnTarget: string | null;
}

export interface GraphEdge {
  from: string;
  to: string;
  field: string;
  relationshipName: string | null;
}

export interface DependencyGraph {
  nodes: Map<string, GraphNode>;
  edges: GraphEdge[];
  tiers: string[][];
}

// ---------------------------------------------------------------------------
// Discovery output
// ---------------------------------------------------------------------------

export interface DiscoveryResult {
  orgAlias: string;
  timestamp: string;
  objects: GraphNode[];
  edges: GraphEdge[];
  tiers: string[][];
  gaps: ExternalIdGap[];
}

export interface ExternalIdGap {
  sobject: string;
  reason: string;
}

// ---------------------------------------------------------------------------
// Recipe types
// ---------------------------------------------------------------------------

export interface Recipe {
  name: string;
  version: string;
  description: string;
  objects: RecipeObject[];
  settings: RecipeSettings;
}

export interface RecipeObject {
  sobject: string;
  externalIdField: string | null;
  filter?: string;
  excludeFields?: string[];
  includeFields?: string[];
  compareIgnoreFields?: string[];
  compositeKey?: CompositeKeyConfig;
  preImport?: { deactivate: string };
  postImport?: { reactivate: string };
}

export interface CompositeKeyConfig {
  strategy: 'lookup-match';
  matchFields: { field: string; matchBy: string }[];
  additionalMatchFields?: string[];
}

export interface RecipeSettings {
  defaultExcludeFields: string[];
  batchSize: number;
  selfReferenceStrategy: 'two-pass' | 'skip';
}

// ---------------------------------------------------------------------------
// Export manifest
// ---------------------------------------------------------------------------

export interface ExportManifest {
  generated: string;
  sourceOrg: string;
  recipe: string;
  mode: 'full' | 'delta';
  tiers: ManifestTier[];
  totalRecords: number;
  instructions: string[];
}

export interface ManifestTier {
  tier: number;
  files: ManifestFile[];
}

export interface ManifestFile {
  order: number;
  filename: string;
  sobject: string;
  externalIdField: string | null;
  recordCount: number;
  operation: 'upsert' | 'insert';
  isSelfRefPass2?: boolean;
  notes?: string;
}

// ---------------------------------------------------------------------------
// Diff / compare types
// ---------------------------------------------------------------------------

export interface DiffResult {
  generated: string;
  sourceOrg: string;
  targetOrg: string;
  recipe: string;
  summary: DiffSummary;
  objects: Record<string, ObjectDiff>;
}

export interface DiffSummary {
  totalNew: number;
  totalModified: number;
  totalDeleted: number;
  totalIdentical: number;
}

export interface ObjectDiff {
  counts: {
    source: number;
    target: number;
    new: number;
    modified: number;
    deleted: number;
    identical: number;
  };
  matchStrategy: 'externalId' | 'fingerprint';
  newRecords: DiffRecord[];
  modifiedRecords: ModifiedRecord[];
  deletedRecords: DiffRecord[];
}

export interface DiffRecord {
  externalId: string;
  name?: string;
}

export interface ModifiedRecord {
  externalId: string;
  name?: string;
  changes: Record<string, { source: unknown; target: unknown }>;
}

// ---------------------------------------------------------------------------
// Import / rollback types
// ---------------------------------------------------------------------------

export interface ImportLog {
  generated: string;
  targetOrg: string;
  recipe: string;
  sourceExportDir: string;
  objects: ImportLogEntry[];
}

export interface ImportLogEntry {
  order: number;
  sobject: string;
  externalIdField: string | null;
  csvFile: string;
  recordsSubmitted: number;
  recordsSucceeded: number;
  recordsFailed: number;
  externalIds: string[];
  /** Salesforce record IDs from success results — used for rollback when external IDs are auto-number */
  sfIds: string[];
  jobId: string;
  status: 'success' | 'partial' | 'failed';
}

export interface BulkJobInfo {
  id: string;
  operation: string;
  object: string;
  state: 'Open' | 'UploadComplete' | 'InProgress' | 'JobComplete' | 'Failed' | 'Aborted';
  numberRecordsProcessed: number;
  numberRecordsFailed: number;
  totalProcessingTime: number;
  errorMessage?: string;
}

/**
 * Tracks a rule row whose ConditionsMet field was temporarily changed from
 * "Custom" to "All" during initial import. After conditions are loaded,
 * the rule is updated back to Custom + its AdvancedCondition.
 *
 * Works for both CPQ rules (SBQQ__ConditionsMet__c) and Advanced Approvals
 * (sbaa__ConditionsMet__c) by storing the actual field names.
 */
export interface DeferredConditionsUpdate {
  sobject: string;
  externalIdField: string;
  sourceExtId: string;
  conditionsMetField: string;
  advancedConditionField: string;
  conditionsMet: string;
  advancedCondition: string;
}
