import { AppError } from '../errors.js';
import type { ClickHouseClient, ConnectedConnection, DatabaseSchema, OperationContext, SchemaObject } from '../types.js';

export function readViewHeader(ddl: string, database: string): { targetDatabase?: string; targetTable?: string; refreshable: boolean } {
  const header = viewHeaderTokens(ddl);
  const words = header.map(token => token.text.toUpperCase());
  const refreshable = words[0] === 'REFRESH';
  if (refreshable && words.some((word, index) => word === 'APPEND' && words[index + 1] === 'INCREMENTAL')) {
    throw new AppError('UNSUPPORTED_SCHEMA', 'Refreshable materialized views using APPEND INCREMENTAL require cursor backups and are not supported.');
  }
  const target = targetClauseIndex(header);
  const first = header[target + 1]?.text;
  if (target < 0 || !first) return { refreshable };
  if (header[target + 2]?.text === '.' && header[target + 3]) {
    return { refreshable, targetDatabase: unquote(first), targetTable: unquote(header[target + 3]!.text) };
  }
  return { refreshable, targetDatabase: database, targetTable: unquote(first) };
}

export function isRefreshableView(object: SchemaObject): boolean {
  return object.kind === 'materialized-view' && readViewHeader(object.ddl, '').refreshable;
}

/** Skip only the creation-time refresh; the saved definition and SELECT body stay unchanged. */
export function createViewStatement(ddl: string): string {
  if (!readViewHeader(ddl, '').refreshable) return ddl;
  const tokens = viewHeaderTokens(ddl);
  const select = tokens.findIndex(token => token.text.toUpperCase() === 'AS');
  if (select < 0) invalidSql();
  const target = targetClauseIndex(tokens);
  if (target < 0) invalidSql();
  const targetEnd = tokens[target + 2]?.text === '.' ? target + 4 : target + 2;
  const clauses = tokens.slice(targetEnd, select);
  const empty = clauses.some((token, index) => token.text.toUpperCase() === 'EMPTY'
    && !['.', '='].includes(clauses[index - 1]?.text ?? '') && clauses[index + 1]?.text !== '=');
  if (empty) return ddl;
  const offset = tokens[select]!.offset;
  return `${ddl.slice(0, offset)}EMPTY ${ddl.slice(offset)}`;
}

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
    const viewTarget = 'kind' in definition && definition.kind === 'materialized-view' ? definition.targetTable : undefined;
    const declared = new Set(definition.dependencies.filter(name => name !== viewTarget));
    if (JSON.stringify(referenced) !== JSON.stringify([...declared].sort())) {
      const label = 'name' in definition ? String(definition.name) : schema.name;
      throw new AppError('INVALID_BACKUP_SQL', `Saved SQL dependencies for ${label} differ from the backup metadata (SQL: ${referenced.join(', ')}; metadata: ${[...declared].sort().join(', ')}).`, 422);
    }
  }
}

/** Metadata reads can retain missing local view names for journal-based recovery. */
export async function readSqlDependencies(client: ClickHouseClient, target: ConnectedConnection, schema: DatabaseSchema, ddl: string, context?: OperationContext, allowUnresolvedLocal = false): Promise<string[]> {
    if ([schema.name, ...schema.objects.map(object => object.name)].some(name => name.includes(' (alias '))) {
      throw new AppError('UNSUPPORTED_SCHEMA', 'Database and object names containing " (alias " cannot be distinguished safely from ClickHouse syntax-tree alias annotations.');
    }
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
      if (line.startsWith('Storage definition ')) {
        const buffer = node.children.find(child => /^Function Buffer(?: |$)/i.test(child.text));
        if (buffer) {
          const argumentsList = buffer.children[0]?.children ?? [];
          const database = readBufferArgument(argumentsList[0]);
          const table = readBufferArgument(argumentsList[1]);
          if (!database || database !== schema.name || !table) {
            throw new AppError('UNSUPPORTED_SCHEMA', 'Buffer tables must write to a table in the same database.');
          }
          const destination = schema.objects.find(object => object.name === table);
          if (!destination || destination.kind !== 'table' || destination.engine === 'Buffer') {
            throw new AppError('UNSUPPORTED_SCHEMA', `Buffer destination ${table} must be a supported persistent table, without Buffer chains.`);
          }
          reference(`${database}.${table}`);
        }
      }
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
      if (line.startsWith('TableIdentifier ')) {
        const identifier = line.slice('TableIdentifier '.length);
        const alias = identifier.indexOf(' (alias ');
        reference(alias < 0 ? identifier : identifier.slice(0, alias));
      }
      node.children.forEach(visit);
    }
    astTree(lines).forEach(visit);
    return [...referenced].sort();
}

interface AstNode { text: string; children: AstNode[] }

function readBufferArgument(node: AstNode | undefined): string {
  if (node?.text.startsWith('Identifier ')) return node.text.slice('Identifier '.length);
  const literal = node?.text.match(/^Literal '((?:[^'\\]|\\.|'')*)'$/s);
  if (!literal) throw new AppError('UNSUPPORTED_SCHEMA', 'Buffer destination arguments must be saved as database and table names, without expressions.');
  return literal[1]!.replace(/''|\\(?:x[\da-fA-F]{2}|.)/gs, escape => {
    if (escape === "''") return "'";
    if (escape.startsWith('\\x')) return String.fromCharCode(Number.parseInt(escape.slice(2), 16));
    const character = escape[1]!;
    const decoded = ({ n: '\n', r: '\r', t: '\t', b: '\b', f: '\f', '0': '\0', '\\': '\\', "'": "'", '"': '"' })[character];
    if (decoded === undefined) throw new AppError('UNSUPPORTED_SCHEMA', 'Buffer destination names contain an unsupported escape sequence.');
    return decoded;
  });
}

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
  const tokens = topLevelTokens(sql).map(token => token.text.startsWith('`') || token.text.startsWith('"') ? token.text : token.text.toUpperCase());
  const engine = tokens.findIndex(token => token === 'ENGINE');
  if (engine < 0 || tokens[engine + 1] !== '=' || tokens[engine + 2] !== expectedEngine.toUpperCase()) invalidSql();
  if (tokens.includes('UUID') || tokens.includes('CLUSTER') || tokens.includes('AS')) invalidSql();
}

function checkViewDefinition(sql: string, database: string, targetTable?: string): void {
  const view = readViewHeader(sql, database);
  const tokens = topLevelTokens(sql).map(token => token.text.startsWith('`') || token.text.startsWith('"') ? token.text : token.text.toUpperCase());
  const select = tokens.indexOf('AS');
  if (select < 0) invalidSql();
  const header = tokens.slice(0, select);
  if (header.includes('UUID') || header.includes('CLUSTER') || header.includes('POPULATE')) invalidSql();
  if (view.refreshable && !targetTable) invalidSql();
  if (targetTable && (view.targetDatabase !== database || view.targetTable !== targetTable)) invalidSql();
}

function topLevelTokens(sql: string): Array<{ text: string; offset: number }> {
  const tokens = sql.matchAll(/--[^\n]*|#[^\n]*|\/\*[\s\S]*?\*\/|`(?:\\.|``|[^`])*`|"(?:\\.|""|[^"])*"|'(?:\\.|''|[^'])*'|[A-Za-z_][A-Za-z0-9_$]*|[^\s]/g);
  const result: Array<{ text: string; offset: number }> = [];
  let depth = 0;
  for (const match of tokens) {
    const token = match[0];
    if (token.startsWith('--') || token.startsWith('#') || token.startsWith('/*') || token.startsWith("'")) continue;
    if (token === '(') { depth++; continue; }
    if (token === ')') { depth--; if (depth < 0) invalidSql(); continue; }
    if (depth !== 0) continue;
    result.push({ text: token, offset: match.index });
  }
  if (depth !== 0) invalidSql();
  return result;
}

/** Skip the CREATE object name so identifiers such as db.refresh are not clauses. */
function viewHeaderTokens(sql: string): Array<{ text: string; offset: number }> {
  const tokens = topLevelTokens(sql);
  const view = tokens[1]?.text.toUpperCase() === 'MATERIALIZED' ? 2 : 1;
  if (tokens[0]?.text.toUpperCase() !== 'CREATE' || tokens[view]?.text.toUpperCase() !== 'VIEW') return [];
  let start = view + 1;
  if (tokens[start]?.text.toUpperCase() === 'IF') start += 3;
  start++;
  if (tokens[start]?.text === '.') start += 2;
  const select = tokens.findIndex((token, index) => index >= start && token.text.toUpperCase() === 'AS');
  return tokens.slice(start, select < 0 ? undefined : select + 1);
}

function targetClauseIndex(tokens: Array<{ text: string }>): number {
  return tokens.findIndex((token, index) => token.text.toUpperCase() === 'TO'
    && !['.', 'ON', ','].includes(tokens[index - 1]?.text.toUpperCase() ?? ''));
}

function unquote(token: string): string {
  if (!token.startsWith('`') && !token.startsWith('"')) return token;
  const quote = token[0]!;
  return token.slice(1, -1).replaceAll(quote + quote, quote).replace(/\\(.)/gs, (_match, character: string) => ({ n: '\n', r: '\r', t: '\t' })[character] ?? character);
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
