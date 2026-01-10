/**
 * Tool: get_merge_history
 *
 * Get history of merge operations.
 */

import { z } from 'zod';
import { Tool } from '@modelcontextprotocol/sdk/types.js';
import { getMergeEngine } from '../core/merge-engine.js';

export const GET_MERGE_HISTORY_SCHEMA = z.object({
  limit: z.number().min(1).max(100).optional().default(20).describe('Maximum number of records to return'),
  filter: z.enum(['all', 'successful', 'failed']).optional().default('all').describe('Filter by status')
});

export type GetMergeHistoryInput = z.infer<typeof GET_MERGE_HISTORY_SCHEMA>;

export const GET_MERGE_HISTORY_TOOL: Tool = {
  name: 'get_merge_history',
  description: 'Get history of merge operations. Returns list of past merges with source files, output, strategy, and success status.',
  inputSchema: {
    type: 'object',
    properties: {
      limit: {
        type: 'number',
        description: 'Maximum number of records to return (default: 20, max: 100)',
        minimum: 1,
        maximum: 100,
        default: 20
      },
      filter: {
        type: 'string',
        enum: ['all', 'successful', 'failed'],
        description: 'Filter by status (default: all)',
        default: 'all'
      }
    }
  }
};

export async function handleGetMergeHistory(args: unknown): Promise<unknown> {
  const input = GET_MERGE_HISTORY_SCHEMA.parse(args || {});
  const mergeEngine = getMergeEngine();

  const operations = mergeEngine.getHistory(input.limit, input.filter);

  // Transform to more readable format
  return {
    operations: operations.map(op => ({
      id: op.id,
      source_files: JSON.parse(op.source_files),
      merged_file: op.merged_file,
      strategy: op.merge_strategy,
      performed_at: new Date(op.performed_at).toISOString(),
      success: op.success === 1,
      content_hash: op.content_hash
    }))
  };
}
