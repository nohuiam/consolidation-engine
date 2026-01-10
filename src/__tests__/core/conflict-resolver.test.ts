/**
 * Conflict Resolver Tests
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { ConflictResolver } from '../../core/conflict-resolver.js';
import { v4 as uuidv4 } from 'uuid';

// Mock the database module
const conflicts = new Map();

vi.mock('../../database/schema.js', () => {
  return {
    getDatabase: () => ({
      insertConflict: (conflict: any) => conflicts.set(conflict.id, conflict),
      getConflict: (id: string) => conflicts.get(id),
      listConflicts: (operationId?: string) => {
        const all = Array.from(conflicts.values());
        return operationId ? all.filter(c => c.operation_id === operationId) : all;
      },
      listUnresolvedConflicts: () => {
        return Array.from(conflicts.values()).filter(c => c.resolved_at === null);
      },
      resolveConflict: (id: string, resolution: string) => {
        const conflict = conflicts.get(id);
        if (conflict) {
          conflict.resolution = resolution;
          conflict.resolved_at = Date.now();
        }
      }
    })
  };
});

describe('ConflictResolver', () => {
  let resolver: ConflictResolver;

  beforeEach(() => {
    conflicts.clear();
    resolver = new ConflictResolver();
  });

  describe('createConflict', () => {
    it('should create a content conflict', () => {
      const id = resolver.createConflict(
        null,
        'content',
        '# Introduction',
        'medium',
        'Different versions found'
      );

      expect(id).toBeDefined();
      const conflict = resolver.getConflict(id);
      expect(conflict).toBeDefined();
      expect(conflict!.conflict_type).toBe('content');
      expect(conflict!.severity).toBe('medium');
    });

    it('should create a structure conflict', () => {
      const id = resolver.createConflict(
        'op-123',
        'structure',
        '# API',
        'low',
        'Section missing in some files'
      );

      const conflict = resolver.getConflict(id);
      expect(conflict!.conflict_type).toBe('structure');
      expect(conflict!.operation_id).toBe('op-123');
    });

    it('should create a metadata conflict', () => {
      const id = resolver.createConflict(
        null,
        'metadata',
        '/path/to/file.md',
        'high',
        'File does not exist'
      );

      const conflict = resolver.getConflict(id);
      expect(conflict!.conflict_type).toBe('metadata');
      expect(conflict!.severity).toBe('high');
    });
  });

  describe('resolve', () => {
    it('should resolve with keep_first', async () => {
      const id = resolver.createConflict(null, 'content', 'test', 'medium', 'test');

      const result = await resolver.resolve({
        conflict_id: id,
        resolution: 'keep_first'
      });

      expect(result.success).toBe(true);
      expect(result.resolution_applied).toContain('first');
    });

    it('should resolve with keep_second', async () => {
      const id = resolver.createConflict(null, 'content', 'test', 'medium', 'test');

      const result = await resolver.resolve({
        conflict_id: id,
        resolution: 'keep_second'
      });

      expect(result.success).toBe(true);
      expect(result.resolution_applied).toContain('second');
    });

    it('should resolve with keep_both', async () => {
      const id = resolver.createConflict(null, 'content', 'test', 'medium', 'test');

      const result = await resolver.resolve({
        conflict_id: id,
        resolution: 'keep_both'
      });

      expect(result.success).toBe(true);
      expect(result.resolution_applied).toContain('both');
    });

    it('should resolve with manual content', async () => {
      const id = resolver.createConflict(null, 'content', 'test', 'medium', 'test');

      const result = await resolver.resolve({
        conflict_id: id,
        resolution: 'manual',
        manual_content: 'Custom merged content'
      });

      expect(result.success).toBe(true);
      expect(result.resulting_content).toBe('Custom merged content');
    });

    it('should throw error for manual without content', async () => {
      const id = resolver.createConflict(null, 'content', 'test', 'medium', 'test');

      await expect(
        resolver.resolve({
          conflict_id: id,
          resolution: 'manual'
        })
      ).rejects.toThrow('manual_content');
    });

    it('should throw error for non-existent conflict', async () => {
      await expect(
        resolver.resolve({
          conflict_id: 'nonexistent',
          resolution: 'keep_first'
        })
      ).rejects.toThrow('Conflict not found');
    });

    it('should throw error for already resolved conflict', async () => {
      const id = resolver.createConflict(null, 'content', 'test', 'medium', 'test');

      await resolver.resolve({
        conflict_id: id,
        resolution: 'keep_first'
      });

      await expect(
        resolver.resolve({
          conflict_id: id,
          resolution: 'keep_second'
        })
      ).rejects.toThrow('already resolved');
    });
  });

  describe('listConflicts', () => {
    it('should list all conflicts', () => {
      resolver.createConflict(null, 'content', 'a', 'low', 'a');
      resolver.createConflict(null, 'structure', 'b', 'medium', 'b');

      const all = resolver.listConflicts();
      expect(all.length).toBe(2);
    });

    it('should filter by operation ID', () => {
      resolver.createConflict('op-1', 'content', 'a', 'low', 'a');
      resolver.createConflict('op-2', 'structure', 'b', 'medium', 'b');

      const filtered = resolver.listConflicts('op-1');
      expect(filtered.length).toBe(1);
      expect(filtered[0].operation_id).toBe('op-1');
    });
  });

  describe('listUnresolved', () => {
    it('should list only unresolved conflicts', async () => {
      const id1 = resolver.createConflict(null, 'content', 'a', 'low', 'a');
      resolver.createConflict(null, 'structure', 'b', 'medium', 'b');

      await resolver.resolve({ conflict_id: id1, resolution: 'keep_first' });

      const unresolved = resolver.listUnresolved();
      expect(unresolved.length).toBe(1);
    });
  });

  describe('bulkCreate', () => {
    it('should create multiple conflicts', () => {
      const ids = resolver.bulkCreate('op-123', [
        { type: 'content', location: 'a', severity: 'low', description: 'a' },
        { type: 'structure', location: 'b', severity: 'medium', description: 'b' },
        { type: 'metadata', location: 'c', severity: 'high', description: 'c' }
      ]);

      expect(ids.length).toBe(3);
      const all = resolver.listConflicts('op-123');
      expect(all.length).toBe(3);
    });
  });

  describe('getStats', () => {
    it('should return accurate statistics', async () => {
      const id1 = resolver.createConflict(null, 'content', 'a', 'low', 'a');
      resolver.createConflict(null, 'content', 'b', 'medium', 'b');
      resolver.createConflict(null, 'structure', 'c', 'high', 'c');

      await resolver.resolve({ conflict_id: id1, resolution: 'keep_first' });

      const stats = resolver.getStats();
      expect(stats.total).toBe(3);
      expect(stats.unresolved).toBe(2);
      expect(stats.by_type.content).toBe(2);
      expect(stats.by_type.structure).toBe(1);
      expect(stats.by_severity.low).toBe(1);
      expect(stats.by_severity.medium).toBe(1);
      expect(stats.by_severity.high).toBe(1);
    });

    it('should return zeros for empty database', () => {
      const stats = resolver.getStats();
      expect(stats.total).toBe(0);
      expect(stats.unresolved).toBe(0);
    });
  });
});
