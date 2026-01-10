/**
 * Merge Engine
 *
 * Handles document merging with conflict detection.
 */

import { v4 as uuidv4 } from 'uuid';
import { readFileSync, writeFileSync, existsSync, statSync } from 'fs';
import { dirname, basename, join } from 'path';
import { createHash } from 'crypto';
import { getDatabase, type MergeOperation } from '../database/schema.js';

export interface MergeInput {
  file_paths: string[];
  strategy: 'combine' | 'prioritize_first' | 'prioritize_latest';
  output_path?: string;
}

export interface MergeOutput {
  merged_file_path: string;
  sources_preserved: number;
  merge_strategy_used: string;
  content_hash: string;
}

export interface DetectConflictsOutput {
  conflicts: Array<{
    id: string;
    type: 'content' | 'structure' | 'metadata';
    location: string;
    severity: 'low' | 'medium' | 'high';
    description: string;
  }>;
}

export class MergeEngine {
  private db = getDatabase();

  /**
   * Merge multiple documents into one
   */
  async merge(input: MergeInput): Promise<MergeOutput> {
    // Validate inputs
    if (input.file_paths.length < 2) {
      throw new Error('At least 2 files required for merge');
    }

    // Verify all files exist
    for (const filePath of input.file_paths) {
      if (!existsSync(filePath)) {
        throw new Error(`File not found: ${filePath}`);
      }
    }

    // Read all files
    const fileContents = input.file_paths.map(path => ({
      path,
      content: readFileSync(path, 'utf-8'),
      mtime: statSync(path).mtime.getTime()
    }));

    // Execute merge based on strategy
    let mergedContent: string;
    switch (input.strategy) {
      case 'combine':
        mergedContent = this.combineContents(fileContents);
        break;
      case 'prioritize_first':
        mergedContent = this.prioritizeFirst(fileContents);
        break;
      case 'prioritize_latest':
        mergedContent = this.prioritizeLatest(fileContents);
        break;
      default:
        throw new Error(`Unknown merge strategy: ${input.strategy}`);
    }

    // Generate output path if not provided
    const outputPath = input.output_path || this.generateOutputPath(input.file_paths[0]);

    // Calculate content hash
    const contentHash = createHash('sha256').update(mergedContent).digest('hex').substring(0, 16);

    // Write merged file
    writeFileSync(outputPath, mergedContent, 'utf-8');

    // Record operation in database
    const operationId = uuidv4();
    const operation: MergeOperation = {
      id: operationId,
      plan_id: null,
      source_files: JSON.stringify(input.file_paths),
      merged_file: outputPath,
      merge_strategy: input.strategy,
      content_hash: contentHash,
      performed_at: Date.now(),
      success: 1
    };

    this.db.insertOperation(operation);

    return {
      merged_file_path: outputPath,
      sources_preserved: fileContents.length,
      merge_strategy_used: input.strategy,
      content_hash: contentHash
    };
  }

  /**
   * Detect conflicts between documents
   */
  async detectConflicts(filePaths: string[]): Promise<DetectConflictsOutput> {
    const conflicts: DetectConflictsOutput['conflicts'] = [];

    // Read all files
    const fileContents = filePaths.map(path => ({
      path,
      content: existsSync(path) ? readFileSync(path, 'utf-8') : '',
      exists: existsSync(path)
    }));

    // Check for missing files
    for (const file of fileContents) {
      if (!file.exists) {
        conflicts.push({
          id: uuidv4(),
          type: 'metadata',
          location: file.path,
          severity: 'high',
          description: `File does not exist: ${file.path}`
        });
      }
    }

    // Check for structural conflicts (different sections)
    const validFiles = fileContents.filter(f => f.exists);
    if (validFiles.length >= 2) {
      const structuralConflicts = this.detectStructuralConflicts(validFiles);
      conflicts.push(...structuralConflicts);

      // Check for content conflicts (different content in same sections)
      const contentConflicts = this.detectContentConflicts(validFiles);
      conflicts.push(...contentConflicts);
    }

    return { conflicts };
  }

  /**
   * Get merge history
   */
  getHistory(limit: number = 20, filter?: 'all' | 'successful' | 'failed'): MergeOperation[] {
    return this.db.listOperations(filter, limit);
  }

  /**
   * Combine all contents with separators
   */
  private combineContents(files: Array<{ path: string; content: string }>): string {
    const sections: string[] = [];

    for (const file of files) {
      // Parse sections from each file
      const fileSections = this.parseSections(file.content);

      for (const section of fileSections) {
        // Check if section already exists
        const existingIdx = sections.findIndex(s =>
          this.extractHeading(s) === this.extractHeading(section.content)
        );

        if (existingIdx >= 0) {
          // Combine section contents
          sections[existingIdx] = this.combineSections(sections[existingIdx], section.content);
        } else {
          sections.push(section.content);
        }
      }
    }

    return sections.join('\n\n');
  }

  /**
   * Prioritize content from first file, supplement with others
   */
  private prioritizeFirst(files: Array<{ path: string; content: string }>): string {
    if (files.length === 0) return '';

    const primary = files[0].content;
    const primarySections = this.parseSections(primary);
    const result: string[] = primarySections.map(s => s.content);

    // Add unique sections from other files
    for (let i = 1; i < files.length; i++) {
      const otherSections = this.parseSections(files[i].content);
      for (const section of otherSections) {
        const heading = this.extractHeading(section.content);
        const exists = result.some(r => this.extractHeading(r) === heading);
        if (!exists) {
          result.push(section.content);
        }
      }
    }

    return result.join('\n\n');
  }

  /**
   * Prioritize content from most recently modified file
   */
  private prioritizeLatest(files: Array<{ path: string; content: string; mtime: number }>): string {
    // Sort by modification time, newest first
    const sorted = [...files].sort((a, b) => b.mtime - a.mtime);
    return this.prioritizeFirst(sorted);
  }

  /**
   * Parse sections from markdown content
   */
  private parseSections(content: string): Array<{ heading: string; content: string }> {
    const sections: Array<{ heading: string; content: string }> = [];
    const lines = content.split('\n');
    let currentSection: string[] = [];
    let currentHeading = '';

    for (const line of lines) {
      if (line.startsWith('#')) {
        // Save previous section
        if (currentSection.length > 0) {
          sections.push({
            heading: currentHeading,
            content: currentSection.join('\n')
          });
        }
        currentHeading = line;
        currentSection = [line];
      } else {
        currentSection.push(line);
      }
    }

    // Don't forget last section
    if (currentSection.length > 0) {
      sections.push({
        heading: currentHeading,
        content: currentSection.join('\n')
      });
    }

    return sections;
  }

  /**
   * Extract heading text from section
   */
  private extractHeading(content: string): string {
    const firstLine = content.split('\n')[0] || '';
    return firstLine.replace(/^#+\s*/, '').trim().toLowerCase();
  }

  /**
   * Combine two sections with same heading
   */
  private combineSections(existing: string, newContent: string): string {
    const existingLines = existing.split('\n');
    const newLines = newContent.split('\n');

    // Skip heading line in new content
    const newBody = newLines.slice(1).join('\n').trim();
    if (!newBody) return existing;

    // Add new content with separator
    return existing + '\n\n<!-- Merged content -->\n\n' + newBody;
  }

  /**
   * Detect structural conflicts between files
   */
  private detectStructuralConflicts(
    files: Array<{ path: string; content: string }>
  ): DetectConflictsOutput['conflicts'] {
    const conflicts: DetectConflictsOutput['conflicts'] = [];

    // Get headings from each file
    const fileHeadings = files.map(f => ({
      path: f.path,
      headings: this.extractAllHeadings(f.content)
    }));

    // Find mismatched headings
    const allHeadings = new Set<string>();
    for (const fh of fileHeadings) {
      for (const h of fh.headings) {
        allHeadings.add(h.toLowerCase());
      }
    }

    for (const heading of allHeadings) {
      const filesWithHeading = fileHeadings.filter(fh =>
        fh.headings.some(h => h.toLowerCase() === heading)
      );

      if (filesWithHeading.length > 0 && filesWithHeading.length < files.length) {
        conflicts.push({
          id: uuidv4(),
          type: 'structure',
          location: heading,
          severity: 'low',
          description: `Section "${heading}" exists in ${filesWithHeading.length} of ${files.length} files`
        });
      }
    }

    return conflicts;
  }

  /**
   * Detect content conflicts between files
   */
  private detectContentConflicts(
    files: Array<{ path: string; content: string }>
  ): DetectConflictsOutput['conflicts'] {
    const conflicts: DetectConflictsOutput['conflicts'] = [];

    // Compare sections with same headings
    const sectionsByHeading = new Map<string, Array<{ path: string; content: string }>>();

    for (const file of files) {
      const sections = this.parseSections(file.content);
      for (const section of sections) {
        const heading = this.extractHeading(section.content);
        if (!sectionsByHeading.has(heading)) {
          sectionsByHeading.set(heading, []);
        }
        sectionsByHeading.get(heading)!.push({
          path: file.path,
          content: section.content
        });
      }
    }

    // Check for conflicting content
    for (const [heading, sections] of sectionsByHeading) {
      if (sections.length > 1) {
        // Compare content (excluding heading)
        const contents = sections.map(s => s.content.split('\n').slice(1).join('\n').trim());
        const unique = new Set(contents);

        if (unique.size > 1) {
          conflicts.push({
            id: uuidv4(),
            type: 'content',
            location: heading,
            severity: 'medium',
            description: `Section "${heading}" has ${unique.size} different versions across files`
          });
        }
      }
    }

    return conflicts;
  }

  /**
   * Extract all headings from content
   */
  private extractAllHeadings(content: string): string[] {
    const headings: string[] = [];
    const lines = content.split('\n');

    for (const line of lines) {
      if (line.startsWith('#')) {
        headings.push(line.replace(/^#+\s*/, '').trim());
      }
    }

    return headings;
  }

  /**
   * Generate output path for merged file
   */
  private generateOutputPath(firstFilePath: string): string {
    const dir = dirname(firstFilePath);
    const name = basename(firstFilePath, '.md');
    return join(dir, `${name}-merged.md`);
  }
}

// Singleton instance
let mergeEngineInstance: MergeEngine | null = null;

export function getMergeEngine(): MergeEngine {
  if (!mergeEngineInstance) {
    mergeEngineInstance = new MergeEngine();
  }
  return mergeEngineInstance;
}
