import { SseParser } from '../src/api/sse';

describe('SseParser', () => {
  test('reassembles fields split across network chunks', () => {
    const parser = new SseParser();
    expect(parser.feed('id: boot:4\r\nevent: ti')).toEqual([]);
    expect(parser.feed('ck\r\ndata: {"jobs":[]}\r\n\r\n')).toEqual([
      { id: 'boot:4', event: 'tick', data: '{"jobs":[]}' },
    ]);
  });

  test('joins multiline data and ignores keepalive comments', () => {
    const parser = new SseParser();
    expect(parser.feed(': keepalive\n\nevent: log\ndata: first\ndata: second\n\n')).toEqual([
      { event: 'log', data: 'first\nsecond' },
    ]);
  });

  test('uses the SSE default event name', () => {
    const parser = new SseParser();
    expect(parser.feed('data: ready\n\n')).toEqual([{ event: 'message', data: 'ready' }]);
  });
});
