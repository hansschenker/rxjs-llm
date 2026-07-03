/** Shared types for the RAG index layer (Module 4). */

/** What loaders emit (decision D4.1). */
export interface Doc {
  id: string;
  text: string;
  metadata: Record<string, unknown>;
}

/** Pure splitter output: an owned span of the source plus overlap context. */
export interface TextChunk {
  index: number;
  /** The chunk's payload: overlap prefix + owned span. */
  text: string;
  /** Owned span offsets into the source. Concatenating every owned span
   * reconstructs the source exactly — the splitter is lossless. */
  start: number;
  end: number;
}

/** A TextChunk attributed to its document (what splitDocs emits). */
export interface Chunk extends TextChunk {
  /** `${docId}#${index}` */
  id: string;
  docId: string;
  metadata: Record<string, unknown>;
}
