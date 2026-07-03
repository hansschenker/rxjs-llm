import { pgliteStore } from '../../src/index/store/pglite';
import { storeContractTests } from './store-contract';

// The same law suite the in-memory store passes — that interchangeability
// is the point of the contract file (ADR-0015). In-memory PGlite instance
// per test; cleanup shuts the WASM database down.
storeContractTests('pglite', async (dimensions) => {
  const store = await pgliteStore({ dimensions });
  return { store, cleanup: () => store.close() };
});
