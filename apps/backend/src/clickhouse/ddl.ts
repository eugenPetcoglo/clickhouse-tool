import { AppError } from '../errors.js';
import type { ClickHouseClient, ConnectedConnection, DatabaseSchema, OperationContext } from '../types.js';

/** Check a saved CREATE statement's destination; this does not rewrite SQL. */
export function checkCreateStatements(schema: DatabaseSchema): void {
  checkHeader(schema.ddl, ['CREATE', 'DATABASE'], [schema.name]);
  checkDefinition(schema.ddl, schema.engine);
  for (const object of schema.objects) {
    const words = object.kind === 'table'
      ? ['CREATE', 'TABLE']
      : object.kind === 'view' ? ['CREATE', 'VIEW'] : ['CREATE', 'MATERIALIZED', 'VIEW'];
    checkHeader(object.ddl, words, [schema.name, object.name]);
    if (object.kind === 'table') checkDefinition(object.ddl, object.engine);
    else checkViewDefinition(object.ddl, schema.name, object.kind === 'materialized-view' ? object.targetTable : undefined);
  }
}

/** Ask ClickHouse's parser about dependencies without executing saved CREATE SQL. */
export async function checkParsedCreateStatements(client: ClickHouseClient, target: ConnectedConnection, schema: DatabaseSchema, context?: OperationContext): Promise<void> {
  const definitions = [{ ddl: schema.ddl, dependencies: [] as string[], targetTable: undefined as string | undefined }, ...schema.objects];
  for (const definition of definitions) {
    const referenced = await readSqlDependencies(client, target, schema, definition.ddl, context);
    const declared = new Set(definition.dependencies.filter(name => name !== definition.targetTable));
    if (JSON.stringify(referenced) !== JSON.stringify([...declared].sort())) {
      const label = 'name' in definition ? String(definition.name) : schema.name;
      throw new AppError('INVALID_BACKUP_SQL', `Saved SQL dependencies for ${label} differ from the backup metadata (SQL: ${referenced.join(', ')}; metadata: ${[...declared].sort().join(', ')}).`, 422);
    }
  }
}

/** Metadata reads can retain missing local view names for journal-based recovery. */
export async function readSqlDependencies(client: ClickHouseClient, target: ConnectedConnection, schema: DatabaseSchema, ddl: string, context?: OperationContext, allowUnresolvedLocal = false): Promise<string[]> {
    checkSingleStatement(ddl);
    const rows = await client.query<{ explain: string }>(target, `EXPLAIN AST ${ddl.trim().replace(/;$/, '')}`, { context, signal: context?.signal });
    const lines = rows.flatMap(row => {
      if (typeof row.explain !== 'string') throw new AppError('UNSUPPORTED_VERSION', 'ClickHouse did not return a readable SQL syntax tree.', 422);
      return row.explain.split('\n');
    }).filter(line => line.trim());
    if (!lines[0]?.startsWith('CreateQuery ')) invalidSql();
    const referenced = new Set<string>();
    function reference(identifier: string): void {
      // EXPLAIN AST omits quoting. Dots and newlines inside an identifier would
      // make a remote DB reference indistinguishable from a local table name.
      const simpleDatabase = !/[.\r\n]/.test(schema.name);
      const object = simpleDatabase ? schema.objects.find(candidate => !/[.\r\n]/.test(candidate.name)
        && (identifier === candidate.name || identifier === `${schema.name}.${candidate.name}`)) : undefined;
      if (object) {
        referenced.add(object.name);
        return;
      }
      if (simpleDatabase && allowUnresolvedLocal && !/[\r\n]/.test(identifier)) {
        const parts = identifier.split('.');
        const local = parts.length === 1 ? parts[0] : parts.length === 2 && parts[0] === schema.name ? parts[1] : undefined;
        if (local) { referenced.add(local); return; }
      }
      {
        throw new AppError('UNSUPPORTED_SCHEMA', 'Saved SQL references an external, undeclared or ambiguous table. Views must use local objects without dots or newlines in their names.', 422);
      }
    }
    function visit(node: AstNode): void {
      const line = node.text;
      if (line.startsWith('TableExpression ')) {
        const child = node.children[0]?.text ?? '';
        if (!child.startsWith('TableIdentifier ') && !child.startsWith('Subquery ')) {
          throw new AppError('UNSUPPORTED_SCHEMA', 'Views using table functions are not supported.', 422);
        }
      }
      if (line.startsWith('WithElement ')) {
        throw new AppError('UNSUPPORTED_SCHEMA', 'Views using named CTE subqueries are not supported in this backup format.', 422);
      }
      if (/^Function (?:dictGet\w*|dictHas|joinGet\w*)\b/i.test(line)) {
        throw new AppError('UNSUPPORTED_SCHEMA', 'Dictionary and Join-engine dependencies are not supported.', 422);
      }
      if (/^Function (?:in|notIn|globalIn|globalNotIn)\b/i.test(line)) {
        const rightArgument = node.children[0]?.children[1]?.text;
        // ClickHouse dumps `x IN db.table` as Identifier, not TableIdentifier.
        if (rightArgument?.startsWith('Identifier ')) reference(rightArgument.slice('Identifier '.length));
      }
      if (line.startsWith('TableIdentifier ')) reference(line.slice('TableIdentifier '.length));
      node.children.forEach(visit);
    }
    astTree(lines).forEach(visit);
    return [...referenced].sort();
}

interface AstNode { text: string; children: AstNode[] }

function astTree(lines: string[]): AstNode[] {
  const roots: AstNode[] = [];
  const parents: Array<{ depth: number; node: AstNode }> = [];
  for (const line of lines) {
    const depth = line.length - line.trimStart().length;
    const node: AstNode = { text: line.trim(), children: [] };
    while (parents.length && parents[parents.length - 1]!.depth >= depth) parents.pop();
    const parent = parents[parents.length - 1];
    if (parent) parent.node.children.push(node);
    else roots.push(node);
    parents.push({ depth, node });
  }
  return roots;
}

/** Metadata fields do not grant permission to execute a different SQL engine. */
function checkDefinition(sql: string, expectedEngine: string): void {
  const tokens = topLevelTokens(sql);
  const engine = tokens.findIndex(token => token === 'ENGINE');
  if (engine < 0 || tokens[engine + 1] !== '=' || tokens[engine + 2] !== expectedEngine.toUpperCase()) invalidSql();
  if (tokens.includes('UUID') || tokens.includes('CLUSTER') || tokens.includes('AS')) invalidSql();
}

function checkViewDefinition(sql: string, database: string, targetTable?: string): void {
  const tokens = topLevelTokens(sql);
  const select = tokens.indexOf('AS');
  if (select < 0) invalidSql();
  const header = tokens.slice(0, select);
  if (header.includes('UUID') || header.includes('CLUSTER') || header.includes('REFRESH') || header.includes('POPULATE')) invalidSql();
  if (targetTable) {
    const target = header.indexOf('TO');
    if (target < 0) invalidSql();
    const first = header[target + 1];
    const second = header[target + 2];
    if (second === '.') {
      if (!identifierMatches(first, database) || !identifierMatches(header[target + 3], targetTable)) invalidSql();
    } else if (!identifierMatches(first, targetTable)) invalidSql();
  }
}

function topLevelTokens(sql: string): string[] {
  const tokens = sql.match(/--[^\n]*|\/\*[\s\S]*?\*\/|`(?:\\.|``|[^`])*`|"(?:\\.|""|[^"])*"|'(?:\\.|''|[^'])*'|[A-Za-z_][A-Za-z0-9_$]*|[^\s]/g) ?? [];
  const result: string[] = [];
  let depth = 0;
  for (const token of tokens) {
    if (token.startsWith('--') || token.startsWith('/*') || token.startsWith("'")) continue;
    if (token === '(') { depth++; continue; }
    if (token === ')') { depth--; if (depth < 0) invalidSql(); continue; }
    if (depth !== 0) continue;
    result.push(token.startsWith('`') || token.startsWith('"') ? token : token.toUpperCase());
  }
  if (depth !== 0) invalidSql();
  return result;
}

function identifierMatches(token: string | undefined, expected: string): boolean {
  if (token === undefined) return false;
  if (token.startsWith('`') || token.startsWith('"')) {
    return token.slice(1, -1).replace(/\\(.)/gs, '$1').replaceAll(token[0]! + token[0]!, token[0]!) === expected;
  }
  // Unquoted SQL identifiers preserve case. Match the parser's uppercase token.
  return token === expected.toUpperCase();
}

function checkHeader(sql: string, words: string[], expectedName: string[]): void {
  checkSingleStatement(sql);
  const tokens = headerTokens(sql);
  for (const word of words) {
    if (tokens.shift()?.toUpperCase() !== word) invalidSql();
  }
  if (tokens[0]?.toUpperCase() === 'IF') {
    for (const word of ['IF', 'NOT', 'EXISTS']) {
      if (tokens.shift()?.toUpperCase() !== word) invalidSql();
    }
  }
  for (let index = 0; index < expectedName.length; index++) {
    if (index > 0 && tokens.shift() !== '.') invalidSql();
    if (tokens.shift() !== expectedName[index]) invalidSql();
  }
  if (tokens[0] === '.') invalidSql();
}

function headerTokens(sql: string): string[] {
  const tokens: string[] = [];
  let offset = 0;
  while (offset < sql.length && tokens.length < 12) {
    const character = sql[offset];
    if (character === undefined) break;
    if (/\s/.test(character)) { offset++; continue; }
    if (character === '`' || character === '"') {
      const quote = character;
      let value = '';
      let closed = false;
      offset++;
      while (offset < sql.length) {
        const next = sql[offset++];
        if (next === '\\') {
          const escaped = sql[offset++];
          if (escaped === undefined) invalidSql();
          value += escaped;
        } else if (next === quote) {
          if (sql[offset] === quote) { value += quote; offset++; }
          else { closed = true; break; }
        } else {
          value += next;
        }
      }
      if (!closed) invalidSql();
      tokens.push(value);
      continue;
    }
    const word = /^[A-Za-z_][A-Za-z0-9_$]*/.exec(sql.slice(offset))?.[0];
    if (word) { tokens.push(word); offset += word.length; continue; }
    tokens.push(character);
    offset++;
  }
  return tokens;
}

function checkSingleStatement(sql: string): void {
  let quote: string | undefined;
  let blockComment = false;
  let lineComment = false;
  let ended = false;
  for (let index = 0; index < sql.length; index++) {
    const character = sql[index];
    const next = sql[index + 1];
    if (lineComment) { if (character === '\n') lineComment = false; continue; }
    if (blockComment) {
      if (character === '*' && next === '/') { blockComment = false; index++; }
      continue;
    }
    if (quote) {
      if (character === '\\') index++;
      else if (character === quote) {
        if (next === quote) index++;
        else quote = undefined;
      }
      continue;
    }
    if (character === '-' && next === '-') { lineComment = true; index++; continue; }
    if (character === '#') { lineComment = true; continue; }
    if (character === '/' && next === '*') { blockComment = true; index++; continue; }
    if (character !== undefined && /\s/.test(character)) continue;
    if (ended) invalidSql();
    if (character === ';') ended = true;
    else if (character === "'" || character === '`' || character === '"') quote = character;
  }
  if (quote || blockComment) invalidSql();
}

function invalidSql(): never {
  throw new AppError('INVALID_BACKUP_SQL', 'Backup contains an invalid CREATE statement or a different destination.', 422);
}
