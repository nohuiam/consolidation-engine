/**
 * Tool: merge_documents
 *
 * Merge multiple documents into one.
 */

import { z } from 'zod';
import { Tool } from '@modelcontextprotocol/sdk/types.js';
import { getMergeEngine } from '../core/merge-engine.js';

export const MERGE_DOCUMENTS_SCHEMA = z.object({
  file_paths: z.array(z.string()).min(2).describe('Array of file paths to merge'),
  strategy: z.enum(['combine', 'prioritize_first', 'prioritize_latest']).describe(
    'Merge strategy: combine (merge all), prioritize_first (primary from first file), prioritize_latest (primary from newest)'
  ),
  output_path: z.string().optional().describe('Output file path (optional, auto-generated if not provided)')
});

export type MergeDocumentsInput = z.infer<typeof MERGE_DOCUMENTS_SCHEMA>;

export const MERGE_DOCUMENTS_TOOL: Tool = {
  name: 'merge_documents',
  description: 'Merge multiple documents into one. Supports different strategies for combining content.',
  inputSchema: {
    type: 'object',
    properties: {
      file_paths: {
        type: 'array',
        items: { type: 'string' },
        description: 'Array of file paths to merge (minimum 2)',
        minItems: 2
      },
      strategy: {
        type: 'string',
        enum: ['combine', 'prioritize_first', 'prioritize_latest'],
        description: 'combine: merge all content; prioritize_first: use first file as primary; prioritize_latest: use most recent as primary'
      },
      output_path: {
        type: 'string',
        description: 'Output file path (optional, auto-generated if not provided)'
      }
    },
    required: ['file_paths', 'strategy']
  }
};

export async function handleMergeDocuments(args: unknown): Promise<unknown> {
  const input = MERGE_DOCUMENTS_SCHEMA.parse(args);
  const mergeEngine = getMergeEngine();
  return mergeEngine.merge(input);
}
