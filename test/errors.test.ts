import { describe, expect, it } from 'vitest';
import {
  HttpError,
  isRetryable,
  LlmError,
  ParseError,
  ProviderError,
  RateLimitError,
  TimeoutError,
  TransportError,
} from '../src/errors';

describe('error taxonomy', () => {
  it('RateLimitError is an HttpError is an LlmError', () => {
    const err = new RateLimitError('slow down', { retryAfterMs: 2000 });
    expect(err).toBeInstanceOf(RateLimitError);
    expect(err).toBeInstanceOf(HttpError);
    expect(err).toBeInstanceOf(LlmError);
    expect(err).toBeInstanceOf(Error);
    expect(err.status).toBe(429);
    expect(err.retryAfterMs).toBe(2000);
    expect(err.kind).toBe('rate_limit');
  });

  it('carries provider and requestId', () => {
    const err = new HttpError('boom', 500, {
      provider: 'anthropic',
      requestId: 'req_123',
    });
    expect(err.provider).toBe('anthropic');
    expect(err.requestId).toBe('req_123');
  });

  it('preserves cause', () => {
    const cause = new Error('ECONNRESET');
    const err = new TransportError('socket reset', { cause });
    expect(err.cause).toBe(cause);
  });

  it('sets name from the concrete class', () => {
    expect(new ParseError('bad frame').name).toBe('ParseError');
    expect(new RateLimitError('429').name).toBe('RateLimitError');
  });
});

describe('isRetryable', () => {
  it('transport errors are retryable', () => {
    expect(isRetryable(new TransportError('dns'))).toBe(true);
  });

  it('429 and 5xx are retryable, other 4xx are not', () => {
    expect(isRetryable(new RateLimitError('429'))).toBe(true);
    expect(isRetryable(new HttpError('500', 500))).toBe(true);
    expect(isRetryable(new HttpError('503', 503))).toBe(true);
    expect(isRetryable(new HttpError('400', 400))).toBe(false);
    expect(isRetryable(new HttpError('401', 401))).toBe(false);
    expect(isRetryable(new HttpError('404', 404))).toBe(false);
  });

  it('parse errors are never retryable', () => {
    expect(isRetryable(new ParseError('bad json'))).toBe(false);
  });

  it('provider errors follow their per-code flag', () => {
    expect(
      isRetryable(new ProviderError('overloaded', { code: 'overloaded_error', retryable: true })),
    ).toBe(true);
    expect(
      isRetryable(new ProviderError('invalid', { code: 'invalid_request_error' })),
    ).toBe(false);
  });

  it('first-byte timeouts are retryable by default, idle timeouts are not', () => {
    expect(isRetryable(new TimeoutError('no data', 'first-byte'))).toBe(true);
    expect(isRetryable(new TimeoutError('stalled', 'idle'))).toBe(false);
    expect(isRetryable(new TimeoutError('stalled', 'idle', { retryable: true }))).toBe(true);
  });

  it('non-LlmError values are not retryable', () => {
    expect(isRetryable(new Error('random'))).toBe(false);
    expect(isRetryable('string')).toBe(false);
    expect(isRetryable(undefined)).toBe(false);
  });
});
