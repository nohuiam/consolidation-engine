/**
 * Database Schema for Consolidation Engine
 *
 * Tables:
 * - merge_plans: Store merge plans from BBB analysis
 * - merge_operations: Track actual merge operations
 * - merge_conflicts: Track and manage conflicts
 */

import Database from 'better-sqlite3';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { mkdirSync, existsSync } from 'fs';

const __dirname = dirname(fileURLToPath(import.meta.url));

// TypeScript interfaces
export interface MergePlan {
  id: string;
  bbb_report_path: string;
  strategy: 'aggressive' | 'conservative' | 'interactive';
  clusters: string; // JSON array
  estimated_savings: string | null;
  status: 'pending' | 'validated' | 'executing' | 'completed' | 'failed';
  created_at: number;
}

export interface MergeOperation {
  id: string;
  plan_id: string | null;
  source_files: string; // JSON array
  merged_file: string;
  merge_strategy: string;
  content_hash: string | null;
  performed_at: number;
  success: number;
}

export interface MergeConflict {
  id: string;
  operation_id: string | null;
  conflict_type: 'content' | 'structure' | 'metadata';
  location: string | null;
  severity: 'low' | 'medium' | 'high';
  description: string | null;
  resolution: string | null;
  resolved_at: number | null;
}

export class DatabaseManager {
  private db: Database.Database;

  constructor(dbPath?: string) {
    // Default to data directory in project root
    const finalPath = dbPath || join(__dirname, '..', '..', 'data', 'consolidation-engine.db');

    // Ensure data directory exists
    const dbDir = dirname(finalPath);
    if (!existsSync(dbDir)) {
      mkdirSync(dbDir, { recursive: true });
    }

    this.db = new Database(finalPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('cache_size = 2000');
    this.initializeSchema();
  }

  private initializeSchema(): void {
    const schema = `
      -- Merge Plans
      CREATE TABLE IF NOT EXISTS merge_plans (
        id TEXT PRIMARY KEY,
        bbb_report_path TEXT NOT NULL,
        strategy TEXT NOT NULL,
        clusters TEXT NOT NULL,
        estimated_savings TEXT,
        status TEXT DEFAULT 'pending',
        created_at INTEGER NOT NULL
      );

      -- Merge Operations
      CREATE TABLE IF NOT EXISTS merge_operations (
        id TEXT PRIMARY KEY,
        plan_id TEXT,
        source_files TEXT NOT NULL,
        merged_file TEXT NOT NULL,
        merge_strategy TEXT NOT NULL,
        content_hash TEXT,
        performed_at INTEGER NOT NULL,
        success INTEGER DEFAULT 1,
        FOREIGN KEY (plan_id) REFERENCES merge_plans(id)
      );

      -- Merge Conflicts
      CREATE TABLE IF NOT EXISTS merge_conflicts (
        id TEXT PRIMARY KEY,
        operation_id TEXT,
        conflict_type TEXT NOT NULL,
        location TEXT,
        severity TEXT,
        description TEXT,
        resolution TEXT,
        resolved_at INTEGER,
        FOREIGN KEY (operation_id) REFERENCES merge_operations(id)
      );

      -- Indexes
      CREATE INDEX IF NOT EXISTS idx_plans_status ON merge_plans(status);
      CREATE INDEX IF NOT EXISTS idx_operations_date ON merge_operations(performed_at);
      CREATE INDEX IF NOT EXISTS idx_operations_plan ON merge_operations(plan_id);
      CREATE INDEX IF NOT EXISTS idx_conflicts_operation ON merge_conflicts(operation_id);
      CREATE INDEX IF NOT EXISTS idx_conflicts_resolved ON merge_conflicts(resolved_at);
    `;

    this.db.exec(schema);
  }

  // Merge Plan Operations
  insertPlan(plan: MergePlan): void {
    const stmt = this.db.prepare(`
      INSERT INTO merge_plans (id, bbb_report_path, strategy, clusters, estimated_savings, status, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      plan.id,
      plan.bbb_report_path,
      plan.strategy,
      plan.clusters,
      plan.estimated_savings,
      plan.status,
      plan.created_at
    );
  }

  getPlan(id: string): MergePlan | undefined {
    const stmt = this.db.prepare('SELECT * FROM merge_plans WHERE id = ?');
    return stmt.get(id) as MergePlan | undefined;
  }

  listPlans(status?: string): MergePlan[] {
    if (status) {
      const stmt = this.db.prepare('SELECT * FROM merge_plans WHERE status = ? ORDER BY created_at DESC');
      return stmt.all(status) as MergePlan[];
    }
    const stmt = this.db.prepare('SELECT * FROM merge_plans ORDER BY created_at DESC');
    return stmt.all() as MergePlan[];
  }

  updatePlanStatus(id: string, status: MergePlan['status']): void {
    const stmt = this.db.prepare('UPDATE merge_plans SET status = ? WHERE id = ?');
    stmt.run(status, id);
  }

  // Merge Operation Operations
  insertOperation(operation: MergeOperation): void {
    const stmt = this.db.prepare(`
      INSERT INTO merge_operations (id, plan_id, source_files, merged_file, merge_strategy, content_hash, performed_at, success)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      operation.id,
      operation.plan_id,
      operation.source_files,
      operation.merged_file,
      operation.merge_strategy,
      operation.content_hash,
      operation.performed_at,
      operation.success
    );
  }

  getOperation(id: string): MergeOperation | undefined {
    const stmt = this.db.prepare('SELECT * FROM merge_operations WHERE id = ?');
    return stmt.get(id) as MergeOperation | undefined;
  }

  listOperations(filter?: 'all' | 'successful' | 'failed', limit: number = 20): MergeOperation[] {
    let query = 'SELECT * FROM merge_operations';

    if (filter === 'successful') {
      query += ' WHERE success = 1';
    } else if (filter === 'failed') {
      query += ' WHERE success = 0';
    }

    query += ' ORDER BY performed_at DESC LIMIT ?';

    const stmt = this.db.prepare(query);
    return stmt.all(limit) as MergeOperation[];
  }

  updateOperationSuccess(id: string, success: boolean): void {
    const stmt = this.db.prepare('UPDATE merge_operations SET success = ? WHERE id = ?');
    stmt.run(success ? 1 : 0, id);
  }

  // Conflict Operations
  insertConflict(conflict: MergeConflict): void {
    const stmt = this.db.prepare(`
      INSERT INTO merge_conflicts (id, operation_id, conflict_type, location, severity, description, resolution, resolved_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    stmt.run(
      conflict.id,
      conflict.operation_id,
      conflict.conflict_type,
      conflict.location,
      conflict.severity,
      conflict.description,
      conflict.resolution,
      conflict.resolved_at
    );
  }

  getConflict(id: string): MergeConflict | undefined {
    const stmt = this.db.prepare('SELECT * FROM merge_conflicts WHERE id = ?');
    return stmt.get(id) as MergeConflict | undefined;
  }

  listConflicts(operationId?: string): MergeConflict[] {
    if (operationId) {
      const stmt = this.db.prepare('SELECT * FROM merge_conflicts WHERE operation_id = ?');
      return stmt.all(operationId) as MergeConflict[];
    }
    const stmt = this.db.prepare('SELECT * FROM merge_conflicts ORDER BY resolved_at IS NULL DESC');
    return stmt.all() as MergeConflict[];
  }

  listUnresolvedConflicts(): MergeConflict[] {
    const stmt = this.db.prepare('SELECT * FROM merge_conflicts WHERE resolved_at IS NULL');
    return stmt.all() as MergeConflict[];
  }

  resolveConflict(id: string, resolution: string): void {
    const stmt = this.db.prepare('UPDATE merge_conflicts SET resolution = ?, resolved_at = ? WHERE id = ?');
    stmt.run(resolution, Date.now(), id);
  }

  // Stats
  getStats(): { plans: number; operations: number; conflicts: number; unresolved: number } {
    const plans = (this.db.prepare('SELECT COUNT(*) as count FROM merge_plans').get() as { count: number }).count;
    const operations = (this.db.prepare('SELECT COUNT(*) as count FROM merge_operations').get() as { count: number }).count;
    const conflicts = (this.db.prepare('SELECT COUNT(*) as count FROM merge_conflicts').get() as { count: number }).count;
    const unresolved = (this.db.prepare('SELECT COUNT(*) as count FROM merge_conflicts WHERE resolved_at IS NULL').get() as { count: number }).count;

    return { plans, operations, conflicts, unresolved };
  }

  close(): void {
    this.db.close();
  }
}

// Singleton instance
let dbInstance: DatabaseManager | null = null;

export function getDatabase(): DatabaseManager {
  if (!dbInstance) {
    dbInstance = new DatabaseManager();
  }
  return dbInstance;
}
