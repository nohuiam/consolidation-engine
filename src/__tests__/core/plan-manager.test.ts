/**
 * Plan Manager Tests
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { PlanManager, type Cluster } from '../../core/plan-manager.js';
import { join } from 'path';
import { mkdirSync, writeFileSync, existsSync, rmSync, chmodSync } from 'fs';

// Shared mock state - must be outside vi.mock for clearing
const mockPlans = new Map();

// Mock the database module
vi.mock('../../database/schema.js', () => {
  return {
    getDatabase: () => ({
      insertPlan: (plan: any) => mockPlans.set(plan.id, plan),
      getPlan: (id: string) => mockPlans.get(id),
      listPlans: (status?: string) => {
        const all = Array.from(mockPlans.values());
        return status ? all.filter(p => p.status === status) : all;
      },
      updatePlanStatus: (id: string, status: string) => {
        const plan = mockPlans.get(id);
        if (plan) plan.status = status;
      }
    })
  };
});

describe('PlanManager', () => {
  let planManager: PlanManager;
  const testDir = join(__dirname, '../../test-data/plan-manager-test');
  const validReportPath = join(testDir, 'valid-report.json');
  const emptyReportPath = join(testDir, 'empty-report.json');
  const testDocA = join(testDir, 'doc-a.md');
  const testDocB = join(testDir, 'doc-b.md');

  beforeEach(() => {
    // Clear mock state
    mockPlans.clear();

    // Create test directory and files
    if (!existsSync(testDir)) {
      mkdirSync(testDir, { recursive: true });
    }

    // Create test documents
    writeFileSync(testDocA, '# Test A\nContent A');
    writeFileSync(testDocB, '# Test B\nContent B');

    // Create valid report with existing files
    writeFileSync(validReportPath, JSON.stringify({
      clusters: [
        { files: [testDocA, testDocB], similarity: 0.85 }
      ],
      total_redundancy_bytes: 10000
    }));

    // Create empty report
    writeFileSync(emptyReportPath, JSON.stringify({
      clusters: [],
      total_redundancy_bytes: 0
    }));

    planManager = new PlanManager();
  });

  afterEach(() => {
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
  });

  describe('createPlan', () => {
    it('should create plan from valid BBB report with aggressive strategy', async () => {
      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      expect(result.plan_id).toBeDefined();
      expect(result.clusters).toHaveLength(1);
      expect(result.estimated_savings).toBeDefined();
    });

    it('should create plan with conservative strategy', async () => {
      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'conservative'
      });

      expect(result.plan_id).toBeDefined();
      // Conservative: >=95% merge, >=80% review, <80% skip
      // 0.85 similarity means 'review'
      expect(result.clusters[0].recommended_action).toBe('review');
    });

    it('should create plan with interactive strategy', async () => {
      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'interactive'
      });

      expect(result.plan_id).toBeDefined();
      // Interactive strategy always reviews >= 60%
      expect(result.clusters[0].recommended_action).toBe('review');
    });

    it('should handle empty report', async () => {
      const result = await planManager.createPlan({
        bbb_report_path: emptyReportPath,
        strategy: 'aggressive'
      });

      expect(result.plan_id).toBeDefined();
      expect(result.clusters).toHaveLength(0);
    });

    it('should throw error for non-existent report', async () => {
      await expect(
        planManager.createPlan({
          bbb_report_path: '/nonexistent/report.json',
          strategy: 'aggressive'
        })
      ).rejects.toThrow('BBB report not found');
    });

    it('should recommend merge for high similarity with aggressive strategy', async () => {
      writeFileSync(validReportPath, JSON.stringify({
        clusters: [
          { files: [testDocA, testDocB], similarity: 0.75 }
        ]
      }));

      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      expect(result.clusters[0].recommended_action).toBe('merge');
    });

    it('should recommend review for medium similarity with aggressive strategy', async () => {
      writeFileSync(validReportPath, JSON.stringify({
        clusters: [
          { files: [testDocA, testDocB], similarity: 0.55 }
        ]
      }));

      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      expect(result.clusters[0].recommended_action).toBe('review');
    });

    it('should recommend skip for low similarity with aggressive strategy', async () => {
      writeFileSync(validReportPath, JSON.stringify({
        clusters: [
          { files: [testDocA, testDocB], similarity: 0.35 }
        ]
      }));

      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      expect(result.clusters[0].recommended_action).toBe('skip');
    });

    it('should recommend skip for low similarity with interactive strategy', async () => {
      writeFileSync(validReportPath, JSON.stringify({
        clusters: [
          { files: [testDocA, testDocB], similarity: 0.45 }
        ]
      }));

      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'interactive'
      });

      // Interactive: >=60% review, <60% skip
      expect(result.clusters[0].recommended_action).toBe('skip');
    });

    it('should recommend skip for low similarity with conservative strategy', async () => {
      writeFileSync(validReportPath, JSON.stringify({
        clusters: [
          { files: [testDocA, testDocB], similarity: 0.70 }
        ]
      }));

      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'conservative'
      });

      // Conservative: >=95% merge, >=80% review, <80% skip
      expect(result.clusters[0].recommended_action).toBe('skip');
    });

    it('should recommend merge for very high similarity with conservative strategy', async () => {
      writeFileSync(validReportPath, JSON.stringify({
        clusters: [
          { files: [testDocA, testDocB], similarity: 0.98 }
        ]
      }));

      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'conservative'
      });

      expect(result.clusters[0].recommended_action).toBe('merge');
    });

    it('should handle large redundancy bytes (MB format)', async () => {
      writeFileSync(validReportPath, JSON.stringify({
        clusters: [
          { files: [testDocA, testDocB], similarity: 0.85 }
        ],
        total_redundancy_bytes: 5000000 // 5MB
      }));

      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      expect(result.estimated_savings).toBeDefined();
      expect(result.estimated_savings).toContain('MB');
    });

    it('should handle KB-range redundancy bytes', async () => {
      writeFileSync(validReportPath, JSON.stringify({
        clusters: [
          { files: [testDocA, testDocB], similarity: 0.85 }
        ],
        total_redundancy_bytes: 50000 // ~50KB
      }));

      const result = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      expect(result.estimated_savings).toBeDefined();
    });
  });

  describe('validatePlan', () => {
    it('should validate plan with existing files', async () => {
      const plan = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      const result = await planManager.validatePlan(plan.plan_id);
      expect(result.valid).toBe(true);
      expect(result.files_exist).toBe(true);
      expect(result.issues).toHaveLength(0);
    });

    it('should throw error for non-existent plan', async () => {
      await expect(
        planManager.validatePlan('nonexistent-plan-id')
      ).rejects.toThrow('Plan not found');
    });

    it('should detect missing files', async () => {
      // Create plan with non-existent files
      writeFileSync(validReportPath, JSON.stringify({
        clusters: [
          { files: ['/nonexistent/a.md', '/nonexistent/b.md'], similarity: 0.85 }
        ]
      }));

      const plan = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      const result = await planManager.validatePlan(plan.plan_id);
      expect(result.valid).toBe(false);
      expect(result.files_exist).toBe(false);
      expect(result.issues.length).toBeGreaterThan(0);
    });
  });

  describe('getPlan', () => {
    it('should retrieve existing plan', async () => {
      const created = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      const plan = planManager.getPlan(created.plan_id);
      expect(plan).toBeDefined();
      expect(plan!.id).toBe(created.plan_id);
    });

    it('should return undefined for non-existent plan', () => {
      const plan = planManager.getPlan('nonexistent');
      expect(plan).toBeUndefined();
    });
  });

  describe('listPlans', () => {
    it('should list all plans', async () => {
      await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });
      await planManager.createPlan({
        bbb_report_path: emptyReportPath,
        strategy: 'conservative'
      });

      const plans = planManager.listPlans();
      expect(plans.length).toBe(2);
    });

    it('should filter plans by status', async () => {
      const plan1 = await planManager.createPlan({
        bbb_report_path: validReportPath,
        strategy: 'aggressive'
      });

      // Validate one plan to change its status
      await planManager.validatePlan(plan1.plan_id);

      await planManager.createPlan({
        bbb_report_path: emptyReportPath,
        strategy: 'conservative'
      });

      const validatedPlans = planManager.listPlans('validated');
      expect(validatedPlans.length).toBe(1);
    });
  });
});
