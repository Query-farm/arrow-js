# CLAUDE.md

This file provides guidance for Claude Code when working on the Apache Arrow JavaScript codebase.

## Project Overview

Apache Arrow JavaScript is the official JS/TypeScript implementation of Apache Arrow - a universal columnar format for fast data interchange and in-memory analytics. The package is published as `apache-arrow` on npm.

## Common Commands

```bash
# Install dependencies
yarn

# Build all targets (es5, es2015, esnext) and module formats (cjs, esm, umd)
yarn build

# Build specific target/module (faster for development)
yarn build -t es2015 -m esm

# Run tests against built targets
yarn test

# Run tests directly on sources (no build required)
yarn test -t src

# Test with coverage
yarn test:coverage

# Lint and auto-fix
yarn lint

# Lint without auto-fix (CI mode)
yarn lint:ci

# Generate documentation
yarn doc

# Run performance benchmarks
yarn perf

# Test bundling with various bundlers
yarn test:bundle
```

## Project Structure

- `src/` - TypeScript source code
  - `Arrow.ts` - Main export aggregator
  - `Arrow.dom.ts` - DOM-specific entry point
  - `Arrow.node.ts` - Node.js-specific entry point
  - `builder/` - Builder classes for each Arrow data type
  - `visitor/` - Visitor pattern implementations
  - `io/` - Input/output operations
  - `ipc/` - Inter-Process Communication (serialization/deserialization)
  - `fb/` - FlatBuffers generated schema code (auto-generated, rarely edit)
  - `util/` - Utility functions
- `test/` - Test files
- `perf/` - Performance benchmarks
- `bin/` - CLI tools (arrow2csv)

## Code Style

- TypeScript with ES modules (`"type": "module"` in package.json)
- ESLint for linting with TypeScript and Jest plugins
- Extensive use of generics and the Visitor pattern
- Builders create Vectors, Tables contain RecordBatches

## Commit Convention

Uses [Conventional Commits](https://www.conventionalcommits.org/):

- `fix:` - Bug fixes (patch version bump)
- `feat:` - New features (minor version bump)
- `chore:` - Maintenance tasks (patch version bump)
- `feat!:` or `fix!:` - Breaking changes (major version bump)

Examples:
```
fix: Handle empty structs in C data interface
feat: Support new encoding type
feat!: Reconstruct API
chore: Update CI environment
```

## Key Patterns

### Creating Tables
```typescript
import { tableFromArrays, tableFromIPC } from 'apache-arrow';

// From arrays
const table = tableFromArrays({ col1: [1, 2, 3], col2: ['a', 'b', 'c'] });

// From IPC format
const table = tableFromIPC(buffer);
```

### Creating Vectors
```typescript
import { makeVector, vectorFromArray } from 'apache-arrow';

// Fast: from typed arrays (no copy)
const vector = makeVector(new Float32Array([1.0, 2.0, 3.0]));

// Flexible: from JS arrays
const vector = vectorFromArray(['foo', 'bar', 'baz']);
```

## Testing

- Jest is the test framework
- Tests are in `test/unit/`
- Build first with `yarn build` before running `yarn test`
- Or use `yarn test -t src` to test sources directly

## Node.js Requirement

Node.js >= 20.0 is required.
