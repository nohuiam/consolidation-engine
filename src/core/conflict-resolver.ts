/**
 * Conflict Resolver
 *
 * Handles conflict detection and resolution for merge operations.
 */

import { v4 as uuidv4 } from 'uuid';
import { getDatabase, type MergeConflict } from '../database/schema.js';

export interface ResolveConflictInput {
  conflict_id: string;
  resolution: 'keep_first' | 'keep_second' | 'keep_both' | 'manual';
  manual_content?: string;
}

export interface ResolveConflictOutput {
  success: boolean;
  resolution_applied: string;
  resulting_content?: string;
}

export class ConflictResolver {
  private db = getDatabase();

  /**
   * Create a new conflict record
   */
  createConflict(
    operationId: string | null,
    type: 'content' | 'structure' | 'metadata',
    location: string,
    severity: 'low' | 'medium' | 'high',
    description: string
  ): string {
    const conflict: MergeConflict = {
      id: uuidv4(),
      operation_id: operationId,
      conflict_type: type,
      location,
      severity,
      description,
      resolution: null,
      resolved_at: null
    };

    this.db.insertConflict(conflict);
    return conflict.id;
  }

  /**
   * Resolve a conflict
   */
  async resolve(input: ResolveConflictInput): Promise<ResolveConflictOutput> {
    const conflict = this.db.getConflict(input.conflict_id);
    if (!conflict) {
      throw new Error(`Conflict not found: ${input.conflict_id}`);
    }

    if (conflict.resolved_at) {
      throw new Error(`Conflict already resolved: ${input.conflict_id}`);
    }

    let resolutionDescription: string;
    let resultingContent: string | undefined;

    switch (input.resolution) {
      case 'keep_first':
        resolutionDescription = 'Kept content from first source';
        break;

      case 'keep_second':
        resolutionDescription = 'Kept content from second source';
        break;

      case 'keep_both':
        resolutionDescription = 'Kept content from both sources (combined)';
        break;

      case 'manual':
        if (!input.manual_content) {
          throw new Error('Manual resolution requires manual_content');
        }
        resolutionDescription = 'Manual resolution applied';
        resultingContent = input.manual_content;
        break;

      default:
        throw new Error(`Unknown resolution type: ${input.resolution}`);
    }

    // Update conflict in database
    this.db.resolveConflict(input.conflict_id, resolutionDescription);

    return {
      success: true,
      resolution_applied: resolutionDescription,
      resulting_content: resultingContent
    };
  }

  /**
   * Get conflict by ID
   */
  getConflict(conflictId: string): MergeConflict | undefined {
    return this.db.getConflict(conflictId);
  }

  /**
   * List conflicts for an operation
   */
  listConflicts(operationId?: string): MergeConflict[] {
    return this.db.listConflicts(operationId);
  }

  /**
   * List all unresolved conflicts
   */
  listUnresolved(): MergeConflict[] {
    return this.db.listUnresolvedConflicts();
  }

  /**
   * Bulk create conflicts from detection results
   */
  bulkCreate(
    operationId: string | null,
    conflicts: Array<{
      type: 'content' | 'structure' | 'metadata';
      location: string;
      severity: 'low' | 'medium' | 'high';
      description: string;
    }>
  ): string[] {
    const ids: string[] = [];

    for (const conflict of conflicts) {
      const id = this.createConflict(
        operationId,
        conflict.type,
        conflict.location,
        conflict.severity,
        conflict.description
      );
      ids.push(id);
    }

    return ids;
  }

  /**
   * Get conflict statistics
   */
  getStats(): {
    total: number;
    unresolved: number;
    by_type: { content: number; structure: number; metadata: number };
    by_severity: { low: number; medium: number; high: number };
  } {
    const all = this.db.listConflicts();
    const unresolved = this.db.listUnresolvedConflicts();

    const byType = { content: 0, structure: 0, metadata: 0 };
    const bySeverity = { low: 0, medium: 0, high: 0 };

    for (const conflict of all) {
      if (conflict.conflict_type in byType) {
        byType[conflict.conflict_type as keyof typeof byType]++;
      }
      if (conflict.severity && conflict.severity in bySeverity) {
        bySeverity[conflict.severity as keyof typeof bySeverity]++;
      }
    }

    return {
      total: all.length,
      unresolved: unresolved.length,
      by_type: byType,
      by_severity: bySeverity
    };
  }
}

// Singleton instance
let conflictResolverInstance: ConflictResolver | null = null;

export function getConflictResolver(): ConflictResolver {
  if (!conflictResolverInstance) {
    conflictResolverInstance = new ConflictResolver();
  }
  return conflictResolverInstance;
}
