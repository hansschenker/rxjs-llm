import { defer, from, map, of, type Observable } from 'rxjs';
import type { MetadataFilter, QueryMatch, VectorEntry, VectorStore } from './types';

/**
 * PGlite (WASM Postgres) + pgvector store via Drizzle (decision D4.4,
 * ADR-0018).
 *
 * NOT exported from the package root: @electric-sql/pglite and drizzle-orm
 * are opt-in dependencies — importing this module is what opts you in, and
 * every import here is dynamic so type-checking or bundling the core never
 * loads them.
 *
 * Passes the identical contract suite as the in-memory store; that suite,
 * not this file, defines what "is a VectorStore" means.
 */
export interface PgliteStoreConfig {
  /** pgvector columns are fixed-dimension; this store is too. */
  dimensions: number;
  /** PGlite data directory; default in-memory (fresh per instance). */
  dataDir?: string;
  tableName?: string;
}

export interface PgliteStore extends VectorStore {
  /** Shut the WASM database down. */
  close(): Promise<void>;
}

export async function pgliteStore(config: PgliteStoreConfig): Promise<PgliteStore> {
  const { PGlite } = await import('@electric-sql/pglite');
  const { vector: vectorExtension } = await import('@electric-sql/pglite-pgvector');
  const { drizzle } = await import('drizzle-orm/pglite');
  const { pgTable, text, jsonb, vector } = await import('drizzle-orm/pg-core');
  const { asc, cosineDistance, inArray, sql } = await import('drizzle-orm');

  const tableName = config.tableName ?? 'rxjs_llm_chunks';
  if (!/^[a-z_][a-z0-9_]*$/i.test(tableName)) {
    throw new RangeError(`invalid table name: ${tableName}`);
  }

  const client = config.dataDir
    ? new PGlite(config.dataDir, { extensions: { vector: vectorExtension } })
    : new PGlite({ extensions: { vector: vectorExtension } });

  // Drizzle-kit is a migration tool, not runtime DDL — the schema statement
  // stays SQL, the schema OBJECT below is what queries type-check against.
  await client.exec(`
    CREATE EXTENSION IF NOT EXISTS vector;
    CREATE TABLE IF NOT EXISTS "${tableName}" (
      id text PRIMARY KEY,
      text text NOT NULL,
      metadata jsonb NOT NULL DEFAULT '{}',
      embedding vector(${config.dimensions}) NOT NULL
    );
  `);

  const chunks = pgTable(tableName, {
    id: text('id').primaryKey(),
    text: text('text').notNull(),
    metadata: jsonb('metadata').$type<Record<string, unknown>>().notNull(),
    embedding: vector('embedding', { dimensions: config.dimensions }).notNull(),
  });
  const db = drizzle(client);

  const upsert = (entries: readonly VectorEntry[]): Observable<number> =>
    defer(() => {
      if (entries.length === 0) return of(0);
      return from(
        db
          .insert(chunks)
          .values(
            entries.map((entry) => ({
              id: entry.id,
              text: entry.text,
              metadata: entry.metadata,
              embedding: Array.from(entry.vector),
            })),
          )
          .onConflictDoUpdate({
            target: chunks.id,
            set: {
              text: sql`excluded.text`,
              metadata: sql`excluded.metadata`,
              embedding: sql`excluded.embedding`,
            },
          }),
      ).pipe(map(() => entries.length));
    });

  const query = (
    queryVector: Float32Array,
    k: number,
    filter?: MetadataFilter,
  ): Observable<QueryMatch[]> =>
    defer(() => {
      const distance = cosineDistance(chunks.embedding, Array.from(queryVector));
      const selection = db
        .select({
          id: chunks.id,
          text: chunks.text,
          metadata: chunks.metadata,
          embedding: chunks.embedding,
          distance,
        })
        .from(chunks)
        .orderBy(asc(distance));
      // A predicate filter cannot push down to SQL: scan ordered rows and
      // keep the first k that pass — acceptable at the stated ~50k bound
      // (ADR-0015). Without a filter the database LIMITs.
      return from(filter === undefined ? selection.limit(k) : selection);
    }).pipe(
      map((rows) => {
        const matches: QueryMatch[] = [];
        for (const row of rows) {
          if (filter !== undefined && !filter(row.metadata, row.id)) continue;
          matches.push({
            id: row.id,
            text: row.text,
            metadata: row.metadata,
            vector: Float32Array.from(row.embedding),
            score: 1 - Number(row.distance),
          });
          if (matches.length === k) break;
        }
        return matches;
      }),
    );

  const remove = (ids: readonly string[]): Observable<number> =>
    defer(() => {
      if (ids.length === 0) return of(0);
      return from(
        db.delete(chunks).where(inArray(chunks.id, Array.from(ids))).returning({ id: chunks.id }),
      ).pipe(map((rows) => rows.length));
    });

  return {
    upsert,
    query,
    delete: remove,
    close: () => client.close(),
  };
}
