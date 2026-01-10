/**
 * create_merge_plan Tool Tests
 */

import { describe, it, expect, vi } from 'vitest';
import { handleCreateMergePlan, CREATE_MERGE_PLAN_SCHEMA } from '../../tools/create-merge-plan.js';

// Mock the plan manager
vi.mock('../../core/plan-manager.js', () => ({
  getPlanManager: () => ({
    createPlan: async (input: any) => {
      if (input.bbb_report_path.includes('nonexistent')) {
        throw new Error('BBB report not found');
      }
      if (input.bbb_report_path.includes('empty')) {
        return {
          plan_id: 'test-plan-empty',
          clusters: [],
          estimated_savings: '0 bytes'
        };
      }
      return {
        plan_id: 'test-plan-123',
        clusters: [{ files: ['/a.md', '/b.md'], similarity_score: 0.85, recommended_action: 'merge' }],
        estimated_savings: '10 KB'
      };
    }
  })
}));

describe('create_merge_plan Tool', () => {
  describe('Schema Validation', () => {
    it('should accept valid input with aggressive strategy', () => {
      const input = { bbb_report_path: '/test/report.json', strategy: 'aggressive' };
      expect(() => CREATE_MERGE_PLAN_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept valid input with conservative strategy', () => {
      const input = { bbb_report_path: '/test/report.json', strategy: 'conservative' };
      expect(() => CREATE_MERGE_PLAN_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept valid input with interactive strategy', () => {
      const input = { bbb_report_path: '/test/report.json', strategy: 'interactive' };
      expect(() => CREATE_MERGE_PLAN_SCHEMA.parse(input)).not.toThrow();
    });

    it('should reject missing strategy', () => {
      const input = { bbb_report_path: '/test/report.json' };
      expect(() => CREATE_MERGE_PLAN_SCHEMA.parse(input)).toThrow();
    });

    it('should reject invalid strategy', () => {
      const input = { bbb_report_path: '/test/report.json', strategy: 'invalid' };
      expect(() => CREATE_MERGE_PLAN_SCHEMA.parse(input)).toThrow();
    });

    it('should reject missing bbb_report_path', () => {
      const input = { strategy: 'aggressive' };
      expect(() => CREATE_MERGE_PLAN_SCHEMA.parse(input)).toThrow();
    });
  });

  describe('Handler', () => {
    it('should create plan with valid input', async () => {
      const result = await handleCreateMergePlan({
        bbb_report_path: '/test/report.json',
        strategy: 'aggressive'
      }) as any;

      expect(result.plan_id).toBeDefined();
      expect(result.clusters).toBeDefined();
      expect(result.estimated_savings).toBeDefined();
    });

    it('should handle empty report', async () => {
      const result = await handleCreateMergePlan({
        bbb_report_path: '/empty/report.json',
        strategy: 'aggressive'
      }) as any;

      expect(result.plan_id).toBe('test-plan-empty');
      expect(result.clusters).toHaveLength(0);
    });

    it('should throw error for non-existent report', async () => {
      await expect(
        handleCreateMergePlan({
          bbb_report_path: '/nonexistent/report.json',
          strategy: 'aggressive'
        })
      ).rejects.toThrow('BBB report not found');
    });
  });
});
