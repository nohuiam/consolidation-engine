/**
 * validate_plan Tool Tests
 */

import { describe, it, expect, vi } from 'vitest';
import { handleValidatePlan, VALIDATE_PLAN_SCHEMA } from '../../tools/validate-plan.js';

// Mock the plan manager
vi.mock('../../core/plan-manager.js', () => ({
  getPlanManager: () => ({
    validatePlan: async (planId: string) => {
      if (planId === 'nonexistent') {
        throw new Error('Plan not found: nonexistent');
      }
      if (planId === 'with-issues') {
        return {
          valid: false,
          issues: [{ file: '/missing.md', issue: 'File does not exist' }],
          files_exist: false,
          conflicts_detected: 1
        };
      }
      if (planId === 'already-validated') {
        return {
          valid: true,
          issues: [],
          files_exist: true,
          conflicts_detected: 0,
          already_validated: true
        };
      }
      return {
        valid: true,
        issues: [],
        files_exist: true,
        conflicts_detected: 0
      };
    }
  })
}));

describe('validate_plan Tool', () => {
  describe('Schema Validation', () => {
    it('should accept valid plan_id', () => {
      const input = { plan_id: 'test-plan-123' };
      expect(() => VALIDATE_PLAN_SCHEMA.parse(input)).not.toThrow();
    });

    it('should reject missing plan_id', () => {
      const input = {};
      expect(() => VALIDATE_PLAN_SCHEMA.parse(input)).toThrow();
    });
  });

  describe('Handler', () => {
    it('should validate existing plan', async () => {
      const result = await handleValidatePlan({ plan_id: 'existing-plan' }) as any;

      expect(result.valid).toBe(true);
      expect(result.issues).toHaveLength(0);
      expect(result.files_exist).toBe(true);
    });

    it('should return issues for plan with problems', async () => {
      const result = await handleValidatePlan({ plan_id: 'with-issues' }) as any;

      expect(result.valid).toBe(false);
      expect(result.issues.length).toBeGreaterThan(0);
      expect(result.conflicts_detected).toBe(1);
    });

    it('should handle already validated plan', async () => {
      const result = await handleValidatePlan({ plan_id: 'already-validated' }) as any;

      expect(result.valid).toBe(true);
      expect(result.already_validated).toBe(true);
    });

    it('should throw error for non-existent plan', async () => {
      await expect(
        handleValidatePlan({ plan_id: 'nonexistent' })
      ).rejects.toThrow('Plan not found');
    });
  });
});
