import { Connection } from '@salesforce/core';
import { ObjectDescribe, FieldDescribe } from '../types.js';

/**
 * Wraps the Salesforce describe API with caching.
 * Fetches object metadata (fields, relationships) from a live org.
 */
export class SchemaDescriber {
  private cache = new Map<string, ObjectDescribe>();

  constructor(private conn: Connection) {}

  async describe(sobject: string): Promise<ObjectDescribe> {
    const cached = this.cache.get(sobject);
    if (cached) return cached;

    const raw = await this.conn.sobject(sobject).describe();

    const fields: FieldDescribe[] = raw.fields.map((f) => ({
      name: f.name,
      label: f.label,
      type: f.type,
      referenceTo: (f.referenceTo ?? []) as string[],
      relationshipName: f.relationshipName ?? null,
      externalId: f.externalId ?? false,
      nillable: f.nillable ?? true,
      createable: f.createable ?? false,
      updateable: f.updateable ?? false,
      calculated: f.calculated ?? false,
      autoNumber: f.autoNumber ?? false,
      custom: f.custom ?? false,
      defaultedOnCreate: f.defaultedOnCreate ?? false,
      length: f.length ?? 0,
    }));

    const childRelationships = (raw.childRelationships ?? [])
      .filter((cr) => cr.childSObject && cr.field && cr.relationshipName)
      .map((cr) => ({
        childSObject: cr.childSObject!,
        field: cr.field!,
        relationshipName: cr.relationshipName!,
      }));

    const result: ObjectDescribe = {
      name: raw.name,
      label: raw.label,
      fields,
      childRelationships,
    };

    this.cache.set(sobject, result);
    return result;
  }

  /**
   * Lists all SObjects in the org, optionally filtered by name prefix.
   */
  async listObjects(filters?: string[]): Promise<string[]> {
    const global = await this.conn.describeGlobal();
    let names = global.sobjects
      .filter((s) => s.queryable && !s.name.endsWith('__History') && !s.name.endsWith('__Tag')
        && !s.name.endsWith('__Share') && !s.name.endsWith('__Feed')
        && !s.name.endsWith('__ChangeEvent'))
      .map((s) => s.name);

    if (filters?.length) {
      names = names.filter((n) =>
        filters.some((f) => n.startsWith(f) || n === f)
      );
    }

    return names.sort();
  }

  /**
   * Counts records for an object, with an optional WHERE clause.
   */
  async countRecords(sobject: string, filter?: string): Promise<number> {
    const where = filter ? ` WHERE ${filter}` : '';
    try {
      const result = await this.conn.query<{ expr0: number }>(
        `SELECT COUNT() FROM ${sobject}${where}`
      );
      return result.totalSize;
    } catch {
      return -1;
    }
  }
}
