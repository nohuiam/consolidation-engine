/**
 * merge_documents Tool Tests
 */

import { describe, it, expect, vi } from 'vitest';
import { handleMergeDocuments, MERGE_DOCUMENTS_SCHEMA } from '../../tools/merge-documents.js';

// Mock the merge engine
vi.mock('../../core/merge-engine.js', () => ({
  getMergeEngine: () => ({
    merge: async (input: any) => {
      if (input.file_paths.some((p: string) => p.includes('nonexistent'))) {
        throw new Error('File not found');
      }
      if (input.file_paths.some((p: string) => p.includes('protected'))) {
        throw new Error('Permission denied');
      }
      return {
        merged_file_path: input.output_path || '/merged/output.md',
        sources_preserved: input.file_paths.length,
        merge_strategy_used: input.strategy,
        content_hash: 'abc123def456'
      };
    }
  })
}));

describe('merge_documents Tool', () => {
  describe('Schema Validation', () => {
    it('should accept valid input with combine strategy', () => {
      const input = { file_paths: ['/a.md', '/b.md'], strategy: 'combine' };
      expect(() => MERGE_DOCUMENTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept valid input with prioritize_first strategy', () => {
      const input = { file_paths: ['/a.md', '/b.md'], strategy: 'prioritize_first' };
      expect(() => MERGE_DOCUMENTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept valid input with prioritize_latest strategy', () => {
      const input = { file_paths: ['/a.md', '/b.md'], strategy: 'prioritize_latest' };
      expect(() => MERGE_DOCUMENTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept optional output_path', () => {
      const input = { file_paths: ['/a.md', '/b.md'], strategy: 'combine', output_path: '/out.md' };
      expect(() => MERGE_DOCUMENTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should reject single file', () => {
      const input = { file_paths: ['/a.md'], strategy: 'combine' };
      expect(() => MERGE_DOCUMENTS_SCHEMA.parse(input)).toThrow();
    });

    it('should reject empty array', () => {
      const input = { file_paths: [], strategy: 'combine' };
      expect(() => MERGE_DOCUMENTS_SCHEMA.parse(input)).toThrow();
    });

    it('should reject missing strategy', () => {
      const input = { file_paths: ['/a.md', '/b.md'] };
      expect(() => MERGE_DOCUMENTS_SCHEMA.parse(input)).toThrow();
    });

    it('should reject invalid strategy', () => {
      const input = { file_paths: ['/a.md', '/b.md'], strategy: 'invalid' };
      expect(() => MERGE_DOCUMENTS_SCHEMA.parse(input)).toThrow();
    });
  });

  describe('Handler', () => {
    it('should merge two files with combine strategy', async () => {
      const result = await handleMergeDocuments({
        file_paths: ['/a.md', '/b.md'],
        strategy: 'combine'
      }) as any;

      expect(result.merged_file_path).toBeDefined();
      expect(result.sources_preserved).toBe(2);
      expect(result.merge_strategy_used).toBe('combine');
      expect(result.content_hash).toBeDefined();
    });

    it('should merge three files with prioritize_first', async () => {
      const result = await handleMergeDocuments({
        file_paths: ['/a.md', '/b.md', '/c.md'],
        strategy: 'prioritize_first'
      }) as any;

      expect(result.sources_preserved).toBe(3);
      expect(result.merge_strategy_used).toBe('prioritize_first');
    });

    it('should use provided output_path', async () => {
      const result = await handleMergeDocuments({
        file_paths: ['/a.md', '/b.md'],
        strategy: 'combine',
        output_path: '/custom/output.md'
      }) as any;

      expect(result.merged_file_path).toBe('/custom/output.md');
    });

    it('should throw error for non-existent file', async () => {
      await expect(
        handleMergeDocuments({
          file_paths: ['/a.md', '/nonexistent.md'],
          strategy: 'combine'
        })
      ).rejects.toThrow('File not found');
    });

    it('should throw error for permission denied', async () => {
      await expect(
        handleMergeDocuments({
          file_paths: ['/protected.md', '/b.md'],
          strategy: 'combine'
        })
      ).rejects.toThrow('Permission denied');
    });
  });
});
