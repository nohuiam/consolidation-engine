#!/usr/bin/env node
/**
 * Consolidation Engine MCP Server
 *
 * 4-Layer Architecture:
 * - Layer 1: MCP stdio (Claude Desktop interface)
 * - Layer 2: InterLock UDP mesh (port 3032)
 * - Layer 3: HTTP REST API (port 8032)
 * - Layer 4: WebSocket real-time (port 9032)
 *
 * Combines Smart Merger + Consolidation Planner functionality.
 *
 * Tools:
 * - create_merge_plan: Create plan from BBB analysis
 * - validate_plan: Validate plan before execution
 * - merge_documents: Execute document merge
 * - detect_conflicts: Find conflicts between files
 * - resolve_conflicts: Resolve detected conflicts
 * - get_merge_history: Get merge operation history
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool
} from '@modelcontextprotocol/sdk/types.js';
import { readFileSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

// Tool imports
import { CREATE_MERGE_PLAN_TOOL, handleCreateMergePlan } from './tools/create-merge-plan.js';
import { VALIDATE_PLAN_TOOL, handleValidatePlan } from './tools/validate-plan.js';
import { MERGE_DOCUMENTS_TOOL, handleMergeDocuments } from './tools/merge-documents.js';
import { DETECT_CONFLICTS_TOOL, handleDetectConflicts } from './tools/detect-conflicts.js';
import { RESOLVE_CONFLICTS_TOOL, handleResolveConflicts } from './tools/resolve-conflicts.js';
import { GET_MERGE_HISTORY_TOOL, handleGetMergeHistory } from './tools/get-merge-history.js';

// Layer imports
import { HttpServer } from './http/server.js';
import { WebSocketService } from './websocket/server.js';
import { InterlockSocket } from './interlock/index.js';

// Server metadata
const SERVER_NAME = 'consolidation-engine';
const SERVER_VERSION = '0.1.0';

// Load config
const __dirname = dirname(fileURLToPath(import.meta.url));
const configPath = join(__dirname, '..', 'config', 'interlock.json');
let config: {
  port: number;
  http_port: number;
  websocket_port: number;
  server_id: string;
  heartbeat?: { interval: number; timeout: number };
  accepted_signals?: string[];
  connections?: Record<string, { host: string; port: number }>;
};

try {
  config = JSON.parse(readFileSync(configPath, 'utf-8'));
} catch {
  // Default config if file not found
  config = {
    server_id: 'consolidation-engine',
    port: 3032,
    http_port: 8032,
    websocket_port: 9032
  };
}

// Tool definitions
const TOOLS: Tool[] = [
  CREATE_MERGE_PLAN_TOOL as Tool,
  VALIDATE_PLAN_TOOL as Tool,
  MERGE_DOCUMENTS_TOOL as Tool,
  DETECT_CONFLICTS_TOOL as Tool,
  RESOLVE_CONFLICTS_TOOL as Tool,
  GET_MERGE_HISTORY_TOOL as Tool
];

// Layer instances
let httpServer: HttpServer | null = null;
let wsServer: WebSocketService | null = null;
let interlock: InterlockSocket | null = null;

/**
 * Initialize Layer 2-4 servers (InterLock, HTTP, WebSocket)
 */
async function initServers(): Promise<void> {
  // Layer 4: WebSocket (real-time events)
  try {
    wsServer = new WebSocketService(config.websocket_port);
    await wsServer.start();
  } catch (error) {
    console.error(`[${SERVER_NAME}] WebSocket failed to start:`, (error as Error).message);
  }

  // Layer 3: HTTP REST API
  try {
    httpServer = new HttpServer(config.http_port);
    await httpServer.start();
  } catch (error) {
    console.error(`[${SERVER_NAME}] HTTP failed to start:`, (error as Error).message);
  }

  // Layer 2: InterLock UDP mesh
  try {
    interlock = new InterlockSocket({
      port: config.port,
      serverId: config.server_id,
      heartbeat: config.heartbeat,
      acceptedSignals: config.accepted_signals,
      connections: config.connections
    });
    await interlock.start();
  } catch (error) {
    console.error(`[${SERVER_NAME}] InterLock failed to start (graceful degradation):`, (error as Error).message);
    interlock = null;
  }
}

/**
 * Handle tool calls
 */
async function handleToolCall(
  name: string,
  args: Record<string, unknown>
): Promise<unknown> {
  switch (name) {
    case 'create_merge_plan':
      return handleCreateMergePlan(args);

    case 'validate_plan':
      return handleValidatePlan(args);

    case 'merge_documents':
      return handleMergeDocuments(args);

    case 'detect_conflicts':
      return handleDetectConflicts(args);

    case 'resolve_conflicts':
      return handleResolveConflicts(args);

    case 'get_merge_history':
      return handleGetMergeHistory(args);

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
}

/**
 * Graceful shutdown
 */
async function shutdown(): Promise<void> {
  console.error(`\n[${SERVER_NAME}] Shutting down...`);

  if (interlock) {
    await interlock.stop().catch(() => {});
  }

  if (wsServer) {
    await wsServer.stop().catch(() => {});
  }

  if (httpServer) {
    await httpServer.stop().catch(() => {});
  }

  console.error(`[${SERVER_NAME}] Shutdown complete`);
  process.exit(0);
}

// Register shutdown handlers
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

/**
 * Main server entry point
 */
async function main(): Promise<void> {
  // Initialize Layer 2-4 servers
  await initServers();

  // Layer 1: Create MCP server
  const server = new Server(
    {
      name: SERVER_NAME,
      version: SERVER_VERSION
    },
    {
      capabilities: {
        tools: {}
      }
    }
  );

  // Register tool list handler
  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: TOOLS
  }));

  // Register tool call handler
  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args } = request.params;

    try {
      const result = await handleToolCall(name, args || {});

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2)
          }
        ]
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';

      // Log to stderr (stdout reserved for MCP JSON-RPC)
      console.error(`[${SERVER_NAME}] Tool error (${name}):`, errorMessage);

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: false,
              error: errorMessage
            })
          }
        ],
        isError: true
      };
    }
  });

  // Connect to stdio transport
  const transport = new StdioServerTransport();
  await server.connect(transport);

  // Log startup info
  console.error(`[${SERVER_NAME}] Server started (v${SERVER_VERSION})`);
  console.error(`[${SERVER_NAME}] MCP: stdio`);
  if (interlock) {
    console.error(`[${SERVER_NAME}] InterLock: UDP ${config.port}`);
  }
  if (httpServer) {
    console.error(`[${SERVER_NAME}] HTTP: http://localhost:${config.http_port}`);
  }
  if (wsServer) {
    console.error(`[${SERVER_NAME}] WebSocket: ws://localhost:${config.websocket_port}`);
  }
  console.error(`[${SERVER_NAME}] Tools: ${TOOLS.map(t => t.name).join(', ')}`);
}

// Run server
main().catch((error) => {
  console.error(`[${SERVER_NAME}] Fatal error:`, error);
  process.exit(1);
});
