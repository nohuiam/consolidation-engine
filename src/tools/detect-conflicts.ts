/**
 * Tool: detect_conflicts
 *
 * Detect merge conflicts between documents.
 */

import { z } from 'zod';
import { Tool } from '@modelcontextprotocol/sdk/types.js';
import { getMergeEngine } from '../core/merge-engine.js';

export const DETECT_CONFLICTS_SCHEMA = z.object({
  file_paths: z.array(z.string()).min(2).describe('Array of file paths to check for conflicts')
});

export type DetectConflictsInput = z.infer<typeof DETECT_CONFLICTS_SCHEMA>;

export const DETECT_CONFLICTS_TOOL: Tool = {
  name: 'detect_conflicts',
  description: 'Detect merge conflicts between documents before merging. Returns list of conflicts with type, location, and severity.',
  inputSchema: {
    type: 'object',
    properties: {
      file_paths: {
        type: 'array',
        items: { type: 'string' },
        description: 'Array of file paths to check for conflicts (minimum 2)',
        minItems: 2
      }
    },
    required: ['file_paths']
  }
};

export async function handleDetectConflicts(args: unknown): Promise<unknown> {
  const input = DETECT_CONFLICTS_SCHEMA.parse(args);
  const mergeEngine = getMergeEngine();
  return mergeEngine.detectConflicts(input.file_paths);
}
