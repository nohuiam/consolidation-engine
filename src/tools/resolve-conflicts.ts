/**
 * Tool: resolve_conflicts
 *
 * Resolve detected merge conflicts.
 */

import { z } from 'zod';
import { Tool } from '@modelcontextprotocol/sdk/types.js';
import { getConflictResolver } from '../core/conflict-resolver.js';

export const RESOLVE_CONFLICTS_SCHEMA = z.object({
  conflict_id: z.string().describe('ID of the conflict to resolve'),
  resolution: z.enum(['keep_first', 'keep_second', 'keep_both', 'manual']).describe(
    'Resolution strategy'
  ),
  manual_content: z.string().optional().describe('Manual content (required if resolution is "manual")')
});

export type ResolveConflictsInput = z.infer<typeof RESOLVE_CONFLICTS_SCHEMA>;

export const RESOLVE_CONFLICTS_TOOL: Tool = {
  name: 'resolve_conflicts',
  description: 'Resolve a detected merge conflict. Supports automatic resolution strategies or manual content override.',
  inputSchema: {
    type: 'object',
    properties: {
      conflict_id: {
        type: 'string',
        description: 'ID of the conflict to resolve'
      },
      resolution: {
        type: 'string',
        enum: ['keep_first', 'keep_second', 'keep_both', 'manual'],
        description: 'keep_first: use first source; keep_second: use second source; keep_both: combine both; manual: provide custom content'
      },
      manual_content: {
        type: 'string',
        description: 'Custom content for manual resolution (required if resolution is "manual")'
      }
    },
    required: ['conflict_id', 'resolution']
  }
};

export async function handleResolveConflicts(args: unknown): Promise<unknown> {
  const input = RESOLVE_CONFLICTS_SCHEMA.parse(args);
  const conflictResolver = getConflictResolver();
  return conflictResolver.resolve(input);
}
