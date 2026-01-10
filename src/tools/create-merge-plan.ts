/**
 * Tool: create_merge_plan
 *
 * Create a merge plan from BBB redundancy analysis.
 */

import { z } from 'zod';
import { Tool } from '@modelcontextprotocol/sdk/types.js';
import { getPlanManager } from '../core/plan-manager.js';

export const CREATE_MERGE_PLAN_SCHEMA = z.object({
  bbb_report_path: z.string().describe('Path to BBB analysis output file'),
  strategy: z.enum(['aggressive', 'conservative', 'interactive']).describe(
    'Merge strategy: aggressive (>=70% similarity), conservative (>=95%), interactive (always review)'
  )
});

export type CreateMergePlanInput = z.infer<typeof CREATE_MERGE_PLAN_SCHEMA>;

export const CREATE_MERGE_PLAN_TOOL: Tool = {
  name: 'create_merge_plan',
  description: 'Create a merge plan from Bonzai Bloat Buster (BBB) redundancy analysis. Returns clusters of similar files with recommended actions.',
  inputSchema: {
    type: 'object',
    properties: {
      bbb_report_path: {
        type: 'string',
        description: 'Path to BBB analysis output file (JSON or text format)'
      },
      strategy: {
        type: 'string',
        enum: ['aggressive', 'conservative', 'interactive'],
        description: 'Merge strategy: aggressive (merge >=70% similar), conservative (merge >=95% similar), interactive (always review)'
      }
    },
    required: ['bbb_report_path', 'strategy']
  }
};

export async function handleCreateMergePlan(args: unknown): Promise<unknown> {
  const input = CREATE_MERGE_PLAN_SCHEMA.parse(args);
  const planManager = getPlanManager();
  return planManager.createPlan(input);
}
