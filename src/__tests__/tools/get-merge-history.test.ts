/**
 * get_merge_history Tool Tests
 */

import { describe, it, expect, vi } from 'vitest';
import { handleGetMergeHistory, GET_MERGE_HISTORY_SCHEMA } from '../../tools/get-merge-history.js';

// Mock the merge engine
const mockOperations = [
  {
    id: 'op-1',
    source_files: JSON.stringify(['/a.md', '/b.md']),
    merged_file: '/merged-1.md',
    merge_strategy: 'combine',
    performed_at: Date.now() - 3600000,
    success: 1,
    content_hash: 'hash1'
  },
  {
    id: 'op-2',
    source_files: JSON.stringify(['/c.md', '/d.md']),
    merged_file: '/merged-2.md',
    merge_strategy: 'prioritize_first',
    performed_at: Date.now() - 7200000,
    success: 1,
    content_hash: 'hash2'
  },
  {
    id: 'op-3',
    source_files: JSON.stringify(['/e.md', '/f.md']),
    merged_file: '/merged-3.md',
    merge_strategy: 'combine',
    performed_at: Date.now() - 10800000,
    success: 0,
    content_hash: null
  }
];

vi.mock('../../core/merge-engine.js', () => ({
  getMergeEngine: () => ({
    getHistory: (limit: number = 20, filter?: string) => {
      let ops = [...mockOperations];

      if (filter === 'successful') {
        ops = ops.filter(o => o.success === 1);
      } else if (filter === 'failed') {
        ops = ops.filter(o => o.success === 0);
      }

      return ops.slice(0, limit);
    }
  })
}));

describe('get_merge_history Tool', () => {
  describe('Schema Validation', () => {
    it('should accept empty input (defaults)', () => {
      const input = {};
      expect(() => GET_MERGE_HISTORY_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept custom limit', () => {
      const input = { limit: 5 };
      expect(() => GET_MERGE_HISTORY_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept filter: all', () => {
      const input = { filter: 'all' };
      expect(() => GET_MERGE_HISTORY_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept filter: successful', () => {
      const input = { filter: 'successful' };
      expect(() => GET_MERGE_HISTORY_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept filter: failed', () => {
      const input = { filter: 'failed' };
      expect(() => GET_MERGE_HISTORY_SCHEMA.parse(input)).not.toThrow();
    });

    it('should reject invalid filter', () => {
      const input = { filter: 'invalid' };
      expect(() => GET_MERGE_HISTORY_SCHEMA.parse(input)).toThrow();
    });

    it('should reject limit below 1', () => {
      const input = { limit: 0 };
      expect(() => GET_MERGE_HISTORY_SCHEMA.parse(input)).toThrow();
    });

    it('should reject limit above 100', () => {
      const input = { limit: 101 };
      expect(() => GET_MERGE_HISTORY_SCHEMA.parse(input)).toThrow();
    });
  });

  describe('Handler', () => {
    it('should return operations with default settings', async () => {
      const result = await handleGetMergeHistory({}) as any;

      expect(result.operations).toBeDefined();
      expect(Array.isArray(result.operations)).toBe(true);
      expect(result.operations.length).toBe(3);
    });

    it('should respect limit parameter', async () => {
      const result = await handleGetMergeHistory({ limit: 2 }) as any;

      expect(result.operations.length).toBe(2);
    });

    it('should filter successful operations', async () => {
      const result = await handleGetMergeHistory({ filter: 'successful' }) as any;

      expect(result.operations.every((op: any) => op.success === true)).toBe(true);
    });

    it('should filter failed operations', async () => {
      const result = await handleGetMergeHistory({ filter: 'failed' }) as any;

      expect(result.operations.every((op: any) => op.success === false)).toBe(true);
    });

    it('should transform operation format', async () => {
      const result = await handleGetMergeHistory({ limit: 1 }) as any;
      const op = result.operations[0];

      expect(op.id).toBeDefined();
      expect(Array.isArray(op.source_files)).toBe(true);
      expect(op.merged_file).toBeDefined();
      expect(op.strategy).toBeDefined();
      expect(op.performed_at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
      expect(typeof op.success).toBe('boolean');
    });

    it('should handle empty history', async () => {
      // Override mock for this test
      vi.doMock('../../core/merge-engine.js', () => ({
        getMergeEngine: () => ({
          getHistory: () => []
        })
      }));

      const result = await handleGetMergeHistory({}) as any;
      expect(result.operations).toBeDefined();
    });
  });
});
