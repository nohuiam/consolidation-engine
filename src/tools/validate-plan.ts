/**
 * Tool: validate_plan
 *
 * Validate a merge plan before execution.
 */

import { z } from 'zod';
import { Tool } from '@modelcontextprotocol/sdk/types.js';
import { getPlanManager } from '../core/plan-manager.js';

export const VALIDATE_PLAN_SCHEMA = z.object({
  plan_id: z.string().describe('ID of the merge plan to validate')
});

export type ValidatePlanInput = z.infer<typeof VALIDATE_PLAN_SCHEMA>;

export const VALIDATE_PLAN_TOOL: Tool = {
  name: 'validate_plan',
  description: 'Validate a merge plan before execution. Checks file existence, detects potential conflicts, and verifies plan integrity.',
  inputSchema: {
    type: 'object',
    properties: {
      plan_id: {
        type: 'string',
        description: 'ID of the merge plan to validate'
      }
    },
    required: ['plan_id']
  }
};

export async function handleValidatePlan(args: unknown): Promise<unknown> {
  const input = VALIDATE_PLAN_SCHEMA.parse(args);
  const planManager = getPlanManager();
  return planManager.validatePlan(input.plan_id);
}
