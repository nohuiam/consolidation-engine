/**
 * Full Workflow Integration Tests
 *
 * End-to-end tests for the complete merge workflow.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { join } from 'path';
import { mkdirSync, writeFileSync, existsSync, rmSync, readFileSync } from 'fs';
import { DatabaseManager } from '../../database/schema.js';
import { PlanManager } from '../../core/plan-manager.js';
import { MergeEngine } from '../../core/merge-engine.js';
import { ConflictResolver } from '../../core/conflict-resolver.js';

describe('Full Merge Workflow', () => {
  const testDir = join(__dirname, '../../test-data/integration-test');
  const dbPath = join(testDir, 'integration.db');
  const docA = join(testDir, 'doc-a.md');
  const docB = join(testDir, 'doc-b.md');
  const reportPath = join(testDir, 'bbb-report.json');

  let db: DatabaseManager;

  beforeAll(() => {
    // Create test directory
    if (!existsSync(testDir)) {
      mkdirSync(testDir, { recursive: true });
    }

    // Create test documents
    writeFileSync(docA, `# Introduction

This is document A.

# Setup

Setup from doc A.

# Features

- Feature 1
- Feature 2
`);

    writeFileSync(docB, `# Introduction

This is document B with different content.

# Setup

Setup from doc B.

# API

API documentation.
`);

    // Create BBB report
    writeFileSync(reportPath, JSON.stringify({
      clusters: [
        { files: [docA, docB], similarity: 0.75 }
      ],
      total_redundancy_bytes: 500
    }));

    // Initialize database
    db = new DatabaseManager(dbPath);
  });

  afterAll(() => {
    db.close();
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
  });

  describe('Plan Creation and Validation', () => {
    it('should create plan from BBB report', async () => {
      const planManager = new PlanManager();

      const plan = await planManager.createPlan({
        bbb_report_path: reportPath,
        strategy: 'aggressive'
      });

      expect(plan.plan_id).toBeDefined();
      expect(plan.clusters).toHaveLength(1);
      expect(plan.clusters[0].recommended_action).toBe('merge');
    });

    it('should validate plan with existing files', async () => {
      const planManager = new PlanManager();

      const plan = await planManager.createPlan({
        bbb_report_path: reportPath,
        strategy: 'aggressive'
      });

      const validation = await planManager.validatePlan(plan.plan_id);

      expect(validation.valid).toBe(true);
      expect(validation.files_exist).toBe(true);
    });
  });

  describe('Document Merging', () => {
    it('should merge documents with combine strategy', async () => {
      const mergeEngine = new MergeEngine();
      const outputPath = join(testDir, 'merged-combine.md');

      const result = await mergeEngine.merge({
        file_paths: [docA, docB],
        strategy: 'combine',
        output_path: outputPath
      });

      expect(existsSync(outputPath)).toBe(true);
      const content = readFileSync(outputPath, 'utf-8');

      // Should contain content from both files
      expect(content).toContain('Introduction');
      expect(content).toContain('Setup');
      expect(content).toContain('Features');
      expect(content).toContain('API');
    });

    it('should detect conflicts before merge', async () => {
      const mergeEngine = new MergeEngine();

      const conflicts = await mergeEngine.detectConflicts([docA, docB]);

      // Should detect content conflict in Introduction
      expect(conflicts.conflicts.length).toBeGreaterThan(0);
    });
  });

  describe('Conflict Resolution', () => {
    it('should create and resolve conflicts', async () => {
      const resolver = new ConflictResolver();

      // Create a conflict
      const conflictId = resolver.createConflict(
        null,
        'content',
        '# Introduction',
        'medium',
        'Different content versions'
      );

      // Verify conflict exists
      const conflict = resolver.getConflict(conflictId);
      expect(conflict).toBeDefined();
      expect(conflict!.resolved_at).toBeNull();

      // Resolve the conflict
      const resolution = await resolver.resolve({
        conflict_id: conflictId,
        resolution: 'keep_first'
      });

      expect(resolution.success).toBe(true);

      // Verify conflict is resolved
      const resolved = resolver.getConflict(conflictId);
      expect(resolved!.resolved_at).not.toBeNull();
    });
  });

  describe('Full Workflow End-to-End', () => {
    it('should complete full merge workflow', async () => {
      const planManager = new PlanManager();
      const mergeEngine = new MergeEngine();
      const resolver = new ConflictResolver();

      // Step 1: Create plan from BBB report
      const plan = await planManager.createPlan({
        bbb_report_path: reportPath,
        strategy: 'aggressive'
      });
      expect(plan.plan_id).toBeDefined();

      // Step 2: Validate plan
      const validation = await planManager.validatePlan(plan.plan_id);
      expect(validation.valid).toBe(true);

      // Step 3: Detect conflicts
      const conflictDetection = await mergeEngine.detectConflicts([docA, docB]);
      const initialConflictCount = conflictDetection.conflicts.length;

      // Step 4: Create conflict records
      if (initialConflictCount > 0) {
        const conflictIds = resolver.bulkCreate(
          null,
          conflictDetection.conflicts.map(c => ({
            type: c.type,
            location: c.location,
            severity: c.severity,
            description: c.description
          }))
        );
        expect(conflictIds.length).toBe(initialConflictCount);

        // Step 5: Resolve conflicts
        for (const id of conflictIds) {
          await resolver.resolve({
            conflict_id: id,
            resolution: 'keep_first'
          });
        }

        // Verify all resolved
        const unresolvedCount = resolver.listUnresolved().length;
        expect(unresolvedCount).toBe(0);
      }

      // Step 6: Execute merge
      const outputPath = join(testDir, 'merged-workflow.md');
      const mergeResult = await mergeEngine.merge({
        file_paths: [docA, docB],
        strategy: 'combine',
        output_path: outputPath
      });

      expect(existsSync(outputPath)).toBe(true);
      expect(mergeResult.content_hash).toBeDefined();

      // Step 7: Verify history
      const history = mergeEngine.getHistory(10);
      expect(history.length).toBeGreaterThan(0);
    });
  });
});
