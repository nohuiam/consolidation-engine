# Consolidation Engine

[![CI](https://github.com/nohuiam/consolidation-engine/actions/workflows/consolidation-engine-ci.yml/badge.svg)](https://github.com/nohuiam/consolidation-engine/actions/workflows/consolidation-engine-ci.yml)

Intelligent document merging MCP Server - Smart Merger + Consolidation Planner.

## Features

- **Merge Planning** - Create merge plans from BBB (Bonzai Bloat Buster) redundancy reports
- **Document Merging** - Merge multiple markdown documents with configurable strategies
- **Conflict Detection** - Detect and resolve content/structure conflicts
- **4-Layer Architecture** - MCP stdio, InterLock UDP mesh, HTTP REST API, WebSocket events

## Installation

```bash
npm install
npm run build
```

## Usage

### As MCP Server (Web App)

Add to your MCP configuration:

```json
{
  "mcpServers": {
    "consolidation-engine": {
      "command": "node",
      "args": ["/path/to/consolidation-engine/dist/index.js"]
    }
  }
}
```

Then start the server:

```bash
npm start
```

### Development

```bash
npm run dev
```

## MCP Tools

| Tool | Description |
|------|-------------|
| `create_merge_plan` | Create merge plan from BBB report |
| `validate_plan` | Validate merge plan before execution |
| `merge_documents` | Execute document merge operation |
| `detect_conflicts` | Detect conflicts between files |
| `resolve_conflicts` | Resolve detected conflicts |
| `get_merge_history` | Get history of merge operations |

## Merge Strategies

| Strategy | Merge Threshold | Review Threshold |
|----------|-----------------|------------------|
| `aggressive` | ≥70% similarity | ≥50% similarity |
| `conservative` | ≥95% similarity | ≥80% similarity |
| `interactive` | manual | ≥60% similarity |

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/api/plans` | GET/POST | List/create merge plans |
| `/api/plans/:id` | GET | Get plan details |
| `/api/plans/:id/validate` | POST | Validate plan |
| `/api/merge` | POST | Execute merge |
| `/api/conflicts` | GET | List conflicts |
| `/api/conflicts/:id/resolve` | POST | Resolve conflict |
| `/api/history` | GET | Get merge history |
| `/api/stats` | GET | Get statistics |

## Testing

```bash
npm test                 # Run tests
npm run test:watch       # Watch mode
npm run test:coverage    # With coverage report
```

### Coverage

- **217 tests** across 17 test files
- Statement coverage: 89%
- Branch coverage: 76%
- Function coverage: 91%

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  MCP stdio                       │
│                (Web App)                         │
├─────────────────────────────────────────────────┤
│              InterLock UDP Mesh                  │
│                (Port 3032)                       │
├─────────────────────────────────────────────────┤
│               HTTP REST API                      │
│                (Port 8032)                       │
├─────────────────────────────────────────────────┤
│              WebSocket Events                    │
│                (Port 9032)                       │
└─────────────────────────────────────────────────┘
```

## License

MIT
