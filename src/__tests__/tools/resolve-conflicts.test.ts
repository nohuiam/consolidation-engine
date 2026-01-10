/**
 * resolve_conflicts Tool Tests
 */

import { describe, it, expect, vi } from 'vitest';
import { handleResolveConflicts, RESOLVE_CONFLICTS_SCHEMA } from '../../tools/resolve-conflicts.js';

// Mock the conflict resolver
vi.mock('../../core/conflict-resolver.js', () => ({
  getConflictResolver: () => ({
    resolve: async (input: any) => {
      if (input.conflict_id === 'nonexistent') {
        throw new Error('Conflict not found: nonexistent');
      }
      if (input.conflict_id === 'already-resolved') {
        throw new Error('Conflict already resolved');
      }
      if (input.resolution === 'manual' && !input.manual_content) {
        throw new Error('Manual resolution requires manual_content');
      }

      const resolutionMap: Record<string, string> = {
        keep_first: 'Kept content from first source',
        keep_second: 'Kept content from second source',
        keep_both: 'Kept content from both sources (combined)',
        manual: 'Manual resolution applied'
      };

      return {
        success: true,
        resolution_applied: resolutionMap[input.resolution],
        resulting_content: input.resolution === 'manual' ? input.manual_content : undefined
      };
    }
  })
}));

describe('resolve_conflicts Tool', () => {
  describe('Schema Validation', () => {
    it('should accept keep_first resolution', () => {
      const input = { conflict_id: 'c-123', resolution: 'keep_first' };
      expect(() => RESOLVE_CONFLICTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept keep_second resolution', () => {
      const input = { conflict_id: 'c-123', resolution: 'keep_second' };
      expect(() => RESOLVE_CONFLICTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept keep_both resolution', () => {
      const input = { conflict_id: 'c-123', resolution: 'keep_both' };
      expect(() => RESOLVE_CONFLICTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept manual resolution with content', () => {
      const input = { conflict_id: 'c-123', resolution: 'manual', manual_content: 'Custom' };
      expect(() => RESOLVE_CONFLICTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should reject invalid resolution', () => {
      const input = { conflict_id: 'c-123', resolution: 'invalid' };
      expect(() => RESOLVE_CONFLICTS_SCHEMA.parse(input)).toThrow();
    });

    it('should reject missing conflict_id', () => {
      const input = { resolution: 'keep_first' };
      expect(() => RESOLVE_CONFLICTS_SCHEMA.parse(input)).toThrow();
    });

    it('should reject missing resolution', () => {
      const input = { conflict_id: 'c-123' };
      expect(() => RESOLVE_CONFLICTS_SCHEMA.parse(input)).toThrow();
    });
  });

  describe('Handler', () => {
    it('should resolve with keep_first', async () => {
      const result = await handleResolveConflicts({
        conflict_id: 'c-123',
        resolution: 'keep_first'
      }) as any;

      expect(result.success).toBe(true);
      expect(result.resolution_applied).toContain('first');
    });

    it('should resolve with keep_second', async () => {
      const result = await handleResolveConflicts({
        conflict_id: 'c-123',
        resolution: 'keep_second'
      }) as any;

      expect(result.success).toBe(true);
      expect(result.resolution_applied).toContain('second');
    });

    it('should resolve with keep_both', async () => {
      const result = await handleResolveConflicts({
        conflict_id: 'c-123',
        resolution: 'keep_both'
      }) as any;

      expect(result.success).toBe(true);
      expect(result.resolution_applied).toContain('both');
    });

    it('should resolve with manual content', async () => {
      const result = await handleResolveConflicts({
        conflict_id: 'c-123',
        resolution: 'manual',
        manual_content: 'Custom merged content'
      }) as any;

      expect(result.success).toBe(true);
      expect(result.resulting_content).toBe('Custom merged content');
    });

    it('should throw error for non-existent conflict', async () => {
      await expect(
        handleResolveConflicts({
          conflict_id: 'nonexistent',
          resolution: 'keep_first'
        })
      ).rejects.toThrow('Conflict not found');
    });

    it('should throw error for already resolved conflict', async () => {
      await expect(
        handleResolveConflicts({
          conflict_id: 'already-resolved',
          resolution: 'keep_first'
        })
      ).rejects.toThrow('already resolved');
    });
  });
});
