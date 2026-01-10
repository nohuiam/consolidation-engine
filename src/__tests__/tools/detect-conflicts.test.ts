/**
 * detect_conflicts Tool Tests
 */

import { describe, it, expect, vi } from 'vitest';
import { handleDetectConflicts, DETECT_CONFLICTS_SCHEMA } from '../../tools/detect-conflicts.js';

// Mock the merge engine
vi.mock('../../core/merge-engine.js', () => ({
  getMergeEngine: () => ({
    detectConflicts: async (filePaths: string[]) => {
      const conflicts: any[] = [];

      // Simulate different conflict scenarios
      if (filePaths.some(p => p.includes('conflict'))) {
        conflicts.push({
          id: 'conflict-1',
          type: 'content',
          location: '# Introduction',
          severity: 'medium',
          description: 'Different content versions'
        });
      }

      if (filePaths.some(p => p.includes('struct'))) {
        conflicts.push({
          id: 'conflict-2',
          type: 'structure',
          location: '# API',
          severity: 'low',
          description: 'Section exists in 1 of 2 files'
        });
      }

      if (filePaths.length === 1) {
        return { conflicts: [] };
      }

      return { conflicts };
    }
  })
}));

describe('detect_conflicts Tool', () => {
  describe('Schema Validation', () => {
    it('should accept valid input with two files', () => {
      const input = { file_paths: ['/a.md', '/b.md'] };
      expect(() => DETECT_CONFLICTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should accept more than two files', () => {
      const input = { file_paths: ['/a.md', '/b.md', '/c.md'] };
      expect(() => DETECT_CONFLICTS_SCHEMA.parse(input)).not.toThrow();
    });

    it('should reject single file', () => {
      const input = { file_paths: ['/a.md'] };
      expect(() => DETECT_CONFLICTS_SCHEMA.parse(input)).toThrow();
    });

    it('should reject empty array', () => {
      const input = { file_paths: [] };
      expect(() => DETECT_CONFLICTS_SCHEMA.parse(input)).toThrow();
    });

    it('should reject missing file_paths', () => {
      const input = {};
      expect(() => DETECT_CONFLICTS_SCHEMA.parse(input)).toThrow();
    });
  });

  describe('Handler', () => {
    it('should detect no conflicts for clean files', async () => {
      const result = await handleDetectConflicts({
        file_paths: ['/clean-a.md', '/clean-b.md']
      }) as any;

      expect(result.conflicts).toHaveLength(0);
    });

    it('should detect content conflicts', async () => {
      const result = await handleDetectConflicts({
        file_paths: ['/conflict-a.md', '/conflict-b.md']
      }) as any;

      const contentConflicts = result.conflicts.filter((c: any) => c.type === 'content');
      expect(contentConflicts.length).toBeGreaterThan(0);
    });

    it('should detect structure conflicts', async () => {
      const result = await handleDetectConflicts({
        file_paths: ['/struct-a.md', '/struct-b.md']
      }) as any;

      const structConflicts = result.conflicts.filter((c: any) => c.type === 'structure');
      expect(structConflicts.length).toBeGreaterThan(0);
    });

    it('should detect multiple conflict types', async () => {
      const result = await handleDetectConflicts({
        file_paths: ['/conflict-struct-a.md', '/conflict-struct-b.md']
      }) as any;

      expect(result.conflicts.length).toBe(2);
    });

    it('should return conflict details', async () => {
      const result = await handleDetectConflicts({
        file_paths: ['/conflict-a.md', '/b.md']
      }) as any;

      const conflict = result.conflicts[0];
      expect(conflict.id).toBeDefined();
      expect(conflict.type).toBeDefined();
      expect(conflict.location).toBeDefined();
      expect(conflict.severity).toBeDefined();
      expect(conflict.description).toBeDefined();
    });
  });
});
