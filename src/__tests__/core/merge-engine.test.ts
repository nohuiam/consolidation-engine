/**
 * Merge Engine Tests
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { MergeEngine } from '../../core/merge-engine.js';
import { join } from 'path';
import { mkdirSync, writeFileSync, existsSync, rmSync, readFileSync, utimesSync } from 'fs';

// Shared mock state - must be outside vi.mock for clearing
const mockOperations = new Map();

// Mock the database module
vi.mock('../../database/schema.js', () => {
  return {
    getDatabase: () => ({
      insertOperation: (op: any) => mockOperations.set(op.id, op),
      getOperation: (id: string) => mockOperations.get(id),
      listOperations: (filter?: string, limit?: number) => {
        let all = Array.from(mockOperations.values());
        if (filter === 'successful') all = all.filter(o => o.success === 1);
        if (filter === 'failed') all = all.filter(o => o.success === 0);
        return all.slice(0, limit || 20);
      },
      updateOperationSuccess: (id: string, success: boolean) => {
        const op = mockOperations.get(id);
        if (op) op.success = success ? 1 : 0;
      }
    })
  };
});

describe('MergeEngine', () => {
  let mergeEngine: MergeEngine;
  const testDir = join(__dirname, '../../test-data/merge-engine-test');
  const docA = join(testDir, 'doc-a.md');
  const docB = join(testDir, 'doc-b.md');
  const docC = join(testDir, 'doc-c.md');
  const outputPath = join(testDir, 'merged.md');

  beforeEach(() => {
    // Clear mock state
    mockOperations.clear();

    if (!existsSync(testDir)) {
      mkdirSync(testDir, { recursive: true });
    }

    // Create test documents with different content
    writeFileSync(docA, `# Introduction

Content from document A.

# Setup

Setup instructions from A.
`);

    writeFileSync(docB, `# Introduction

Content from document B.

# API Reference

API docs from B.
`);

    // Create docC with older timestamp
    writeFileSync(docC, `# Introduction

Content from document C.

# Changelog

Version history.
`);

    // Set different mtimes
    const now = new Date();
    const older = new Date(now.getTime() - 86400000); // 1 day ago
    utimesSync(docA, older, older);
    utimesSync(docB, now, now);

    mergeEngine = new MergeEngine();
  });

  afterEach(() => {
    if (existsSync(testDir)) {
      rmSync(testDir, { recursive: true, force: true });
    }
  });

  describe('merge', () => {
    it('should merge two files with combine strategy', async () => {
      const result = await mergeEngine.merge({
        file_paths: [docA, docB],
        strategy: 'combine',
        output_path: outputPath
      });

      expect(result.merged_file_path).toBe(outputPath);
      expect(result.sources_preserved).toBe(2);
      expect(result.merge_strategy_used).toBe('combine');
      expect(result.content_hash).toBeDefined();
      expect(existsSync(outputPath)).toBe(true);
    });

    it('should merge with prioritize_first strategy', async () => {
      const result = await mergeEngine.merge({
        file_paths: [docA, docB],
        strategy: 'prioritize_first',
        output_path: outputPath
      });

      const content = readFileSync(outputPath, 'utf-8');
      // First file's Introduction should be used, and unique sections from B added
      expect(content).toContain('Content from document A');
      expect(content).toContain('API Reference');
    });

    it('should merge with prioritize_latest strategy', async () => {
      const result = await mergeEngine.merge({
        file_paths: [docA, docB],
        strategy: 'prioritize_latest',
        output_path: outputPath
      });

      const content = readFileSync(outputPath, 'utf-8');
      // docB is newer, so its Introduction should be primary
      expect(content).toContain('Content from document B');
    });

    it('should auto-generate output path if not provided', async () => {
      const result = await mergeEngine.merge({
        file_paths: [docA, docB],
        strategy: 'combine'
      });

      expect(result.merged_file_path).toContain('-merged.md');
      expect(existsSync(result.merged_file_path)).toBe(true);
      // Clean up
      rmSync(result.merged_file_path, { force: true });
    });

    it('should throw error for less than 2 files', async () => {
      await expect(
        mergeEngine.merge({
          file_paths: [docA],
          strategy: 'combine'
        })
      ).rejects.toThrow('At least 2 files required');
    });

    it('should throw error for non-existent file', async () => {
      await expect(
        mergeEngine.merge({
          file_paths: [docA, '/nonexistent.md'],
          strategy: 'combine'
        })
      ).rejects.toThrow('File not found');
    });

    it('should generate content hash', async () => {
      const result = await mergeEngine.merge({
        file_paths: [docA, docB],
        strategy: 'combine',
        output_path: outputPath
      });

      expect(result.content_hash).toMatch(/^[a-f0-9]{16}$/);
    });

    it('should merge three files', async () => {
      const result = await mergeEngine.merge({
        file_paths: [docA, docB, docC],
        strategy: 'combine',
        output_path: outputPath
      });

      expect(result.sources_preserved).toBe(3);
      const content = readFileSync(outputPath, 'utf-8');
      expect(content).toContain('Changelog');
    });
  });

  describe('detectConflicts', () => {
    it('should detect content conflicts', async () => {
      const result = await mergeEngine.detectConflicts([docA, docB]);

      // Both have Introduction with different content
      const contentConflicts = result.conflicts.filter(c => c.type === 'content');
      expect(contentConflicts.length).toBeGreaterThan(0);
    });

    it('should detect structural conflicts', async () => {
      const result = await mergeEngine.detectConflicts([docA, docB]);

      // docA has Setup, docB has API Reference - different structures
      const structuralConflicts = result.conflicts.filter(c => c.type === 'structure');
      expect(structuralConflicts.length).toBeGreaterThan(0);
    });

    it('should detect missing file as metadata conflict', async () => {
      const result = await mergeEngine.detectConflicts([docA, '/nonexistent.md']);

      const metadataConflicts = result.conflicts.filter(c => c.type === 'metadata');
      expect(metadataConflicts.length).toBe(1);
      expect(metadataConflicts[0].severity).toBe('high');
    });

    it('should return no conflicts for identical files', async () => {
      writeFileSync(docB, readFileSync(docA, 'utf-8'));

      const result = await mergeEngine.detectConflicts([docA, docB]);

      const contentConflicts = result.conflicts.filter(c => c.type === 'content');
      expect(contentConflicts.length).toBe(0);
    });
  });

  describe('getHistory', () => {
    it('should return empty history initially', () => {
      const history = mergeEngine.getHistory();
      expect(history).toEqual([]);
    });

    it('should return merge history after operations', async () => {
      await mergeEngine.merge({
        file_paths: [docA, docB],
        strategy: 'combine',
        output_path: outputPath
      });

      const history = mergeEngine.getHistory();
      expect(history.length).toBe(1);
    });

    it('should respect limit parameter', async () => {
      for (let i = 0; i < 5; i++) {
        await mergeEngine.merge({
          file_paths: [docA, docB],
          strategy: 'combine',
          output_path: join(testDir, `merged-${i}.md`)
        });
      }

      const history = mergeEngine.getHistory(3);
      expect(history.length).toBe(3);
    });
  });
});
