/**
 * Database Schema Tests
 *
 * Tests for SQLite database operations.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { DatabaseManager, type MergePlan, type MergeOperation, type MergeConflict } from '../../database/schema.js';
import { join } from 'path';
import { mkdirSync, existsSync, rmSync } from 'fs';
import { v4 as uuidv4 } from 'uuid';

describe('DatabaseManager', () => {
  let db: DatabaseManager;
  const testDir = join(__dirname, '../../test-data');
  const testDbPath = join(testDir, 'test-schema.db');

  beforeEach(() => {
    // Ensure test directory exists
    if (!existsSync(testDir)) {
      mkdirSync(testDir, { recursive: true });
    }
    db = new DatabaseManager(testDbPath);
  });

  afterEach(() => {
    db.close();
    // Clean up test database
    if (existsSync(testDbPath)) {
      rmSync(testDbPath, { force: true });
    }
    if (existsSync(testDbPath + '-wal')) {
      rmSync(testDbPath + '-wal', { force: true });
    }
    if (existsSync(testDbPath + '-shm')) {
      rmSync(testDbPath + '-shm', { force: true });
    }
  });

  describe('Schema Initialization', () => {
    it('should create database file', () => {
      expect(existsSync(testDbPath)).toBe(true);
    });

    it('should create merge_plans table', () => {
      const plans = db.listPlans();
      expect(Array.isArray(plans)).toBe(true);
    });

    it('should create merge_operations table', () => {
      const operations = db.listOperations();
      expect(Array.isArray(operations)).toBe(true);
    });

    it('should create merge_conflicts table', () => {
      const conflicts = db.listConflicts();
      expect(Array.isArray(conflicts)).toBe(true);
    });
  });

  describe('Merge Plans CRUD', () => {
    const createTestPlan = (): MergePlan => ({
      id: uuidv4(),
      bbb_report_path: '/test/report.json',
      strategy: 'aggressive',
      clusters: JSON.stringify([{ files: ['/a.md', '/b.md'], similarity_score: 0.85 }]),
      estimated_savings: '10 KB',
      status: 'pending',
      created_at: Date.now()
    });

    it('should insert and retrieve a plan', () => {
      const plan = createTestPlan();
      db.insertPlan(plan);

      const retrieved = db.getPlan(plan.id);
      expect(retrieved).toBeDefined();
      expect(retrieved!.id).toBe(plan.id);
      expect(retrieved!.bbb_report_path).toBe(plan.bbb_report_path);
      expect(retrieved!.strategy).toBe(plan.strategy);
    });

    it('should list all plans', () => {
      const plan1 = createTestPlan();
      const plan2 = createTestPlan();
      db.insertPlan(plan1);
      db.insertPlan(plan2);

      const plans = db.listPlans();
      expect(plans.length).toBe(2);
    });

    it('should list plans by status', () => {
      const plan1 = createTestPlan();
      const plan2 = { ...createTestPlan(), status: 'validated' as const };
      db.insertPlan(plan1);
      db.insertPlan(plan2);

      const pendingPlans = db.listPlans('pending');
      expect(pendingPlans.length).toBe(1);
      expect(pendingPlans[0].status).toBe('pending');
    });

    it('should update plan status', () => {
      const plan = createTestPlan();
      db.insertPlan(plan);

      db.updatePlanStatus(plan.id, 'validated');
      const updated = db.getPlan(plan.id);
      expect(updated!.status).toBe('validated');
    });

    it('should return undefined for non-existent plan', () => {
      const result = db.getPlan('nonexistent-id');
      expect(result).toBeUndefined();
    });
  });

  describe('Merge Operations CRUD', () => {
    const createTestOperation = (): MergeOperation => ({
      id: uuidv4(),
      plan_id: null,
      source_files: JSON.stringify(['/a.md', '/b.md']),
      merged_file: '/merged.md',
      merge_strategy: 'combine',
      content_hash: 'abc123',
      performed_at: Date.now(),
      success: 1
    });

    it('should insert and retrieve an operation', () => {
      const op = createTestOperation();
      db.insertOperation(op);

      const retrieved = db.getOperation(op.id);
      expect(retrieved).toBeDefined();
      expect(retrieved!.id).toBe(op.id);
      expect(retrieved!.merged_file).toBe(op.merged_file);
    });

    it('should list operations with default limit', () => {
      for (let i = 0; i < 5; i++) {
        db.insertOperation(createTestOperation());
      }

      const operations = db.listOperations();
      expect(operations.length).toBe(5);
    });

    it('should filter successful operations', () => {
      const success = createTestOperation();
      const failed = { ...createTestOperation(), success: 0 };
      db.insertOperation(success);
      db.insertOperation(failed);

      const successfulOps = db.listOperations('successful');
      expect(successfulOps.length).toBe(1);
      expect(successfulOps[0].success).toBe(1);
    });

    it('should filter failed operations', () => {
      const success = createTestOperation();
      const failed = { ...createTestOperation(), success: 0 };
      db.insertOperation(success);
      db.insertOperation(failed);

      const failedOps = db.listOperations('failed');
      expect(failedOps.length).toBe(1);
      expect(failedOps[0].success).toBe(0);
    });

    it('should update operation success status', () => {
      const op = createTestOperation();
      db.insertOperation(op);

      db.updateOperationSuccess(op.id, false);
      const updated = db.getOperation(op.id);
      expect(updated!.success).toBe(0);
    });
  });

  describe('Merge Conflicts CRUD', () => {
    const createTestConflict = (): MergeConflict => ({
      id: uuidv4(),
      operation_id: null,
      conflict_type: 'content',
      location: '# Introduction',
      severity: 'medium',
      description: 'Different content versions',
      resolution: null,
      resolved_at: null
    });

    it('should insert and retrieve a conflict', () => {
      const conflict = createTestConflict();
      db.insertConflict(conflict);

      const retrieved = db.getConflict(conflict.id);
      expect(retrieved).toBeDefined();
      expect(retrieved!.id).toBe(conflict.id);
      expect(retrieved!.conflict_type).toBe('content');
    });

    it('should list all conflicts', () => {
      db.insertConflict(createTestConflict());
      db.insertConflict(createTestConflict());

      const conflicts = db.listConflicts();
      expect(conflicts.length).toBe(2);
    });

    it('should list unresolved conflicts', () => {
      const unresolved = createTestConflict();
      const resolved = { ...createTestConflict(), resolution: 'keep_first', resolved_at: Date.now() };
      db.insertConflict(unresolved);
      db.insertConflict(resolved);

      const unresolvedConflicts = db.listUnresolvedConflicts();
      expect(unresolvedConflicts.length).toBe(1);
      expect(unresolvedConflicts[0].resolved_at).toBeNull();
    });

    it('should resolve a conflict', () => {
      const conflict = createTestConflict();
      db.insertConflict(conflict);

      db.resolveConflict(conflict.id, 'Kept first version');
      const resolved = db.getConflict(conflict.id);
      expect(resolved!.resolution).toBe('Kept first version');
      expect(resolved!.resolved_at).not.toBeNull();
    });
  });

  describe('Stats', () => {
    it('should return zero counts for empty database', () => {
      const stats = db.getStats();
      expect(stats.plans).toBe(0);
      expect(stats.operations).toBe(0);
      expect(stats.conflicts).toBe(0);
      expect(stats.unresolved).toBe(0);
    });

    it('should return accurate counts', () => {
      // Add some data
      db.insertPlan({
        id: uuidv4(),
        bbb_report_path: '/test.json',
        strategy: 'aggressive',
        clusters: '[]',
        estimated_savings: '0 B',
        status: 'pending',
        created_at: Date.now()
      });

      db.insertOperation({
        id: uuidv4(),
        plan_id: null,
        source_files: '[]',
        merged_file: '/out.md',
        merge_strategy: 'combine',
        content_hash: 'abc',
        performed_at: Date.now(),
        success: 1
      });

      db.insertConflict({
        id: uuidv4(),
        operation_id: null,
        conflict_type: 'content',
        location: 'test',
        severity: 'low',
        description: 'test',
        resolution: null,
        resolved_at: null
      });

      const stats = db.getStats();
      expect(stats.plans).toBe(1);
      expect(stats.operations).toBe(1);
      expect(stats.conflicts).toBe(1);
      expect(stats.unresolved).toBe(1);
    });
  });
});
