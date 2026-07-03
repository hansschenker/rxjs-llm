import { of } from 'rxjs';
import { describe, expectTypeOf, it } from 'vitest';
import { chain } from '../../src/chain/chain';
import { stage } from '../../src/chain/stage';

// Type-level tests for D3.1: the context type accumulates through the pipe,
// so downstream stages can only reference keys upstream stages produced.

describe('chain context typing (D3.1)', () => {
  it('accumulates keys across stages and run() returns the full context', () => {
    const built = chain<{ url: string }>().pipe(
      stage('fetch', (ctx) => of({ page: ctx.url.toUpperCase() })),
      stage('measure', (ctx) => of({ length: ctx.page.length })),
    );
    const { result$ } = built.run({ url: 'x' });
    result$.subscribe((ctx) => {
      expectTypeOf(ctx.url).toBeString();
      expectTypeOf(ctx.page).toBeString();
      expectTypeOf(ctx.length).toBeNumber();
    });
  });

  it('rejects keys no upstream stage produced', () => {
    chain<{ url: string }>().pipe(
      stage('fetch', (ctx) => of({ page: ctx.url })),
      // @ts-expect-error — 'missing' is not in the accumulated context
      stage('bad', (ctx) => of({ x: ctx.missing })),
    );
  });

  it('rejects run() input that does not satisfy the chain input type', () => {
    const built = chain<{ url: string }>().pipe(
      stage('fetch', (ctx) => of({ page: ctx.url })),
    );
    // @ts-expect-error — 'url' is required
    built.run({});
  });
});
