export interface SseFrame {
  event: string;
  data: string;
  id?: string;
}

/** Incremental SSE parser. Chunks may end in the middle of a CRLF or field. */
export class SseParser {
  private buffer = '';

  feed(chunk: string): SseFrame[] {
    this.buffer += chunk;
    const frames: SseFrame[] = [];
    while (true) {
      const boundary = this.buffer.match(/\r\n\r\n|\n\n|\r\r/);
      if (!boundary || boundary.index === undefined) break;
      const raw = this.buffer.slice(0, boundary.index);
      this.buffer = this.buffer.slice(boundary.index + boundary[0].length);
      const frame = parseFrame(raw);
      if (frame) frames.push(frame);
    }
    return frames;
  }
}

function parseFrame(raw: string): SseFrame | null {
  let event = 'message';
  let id: string | undefined;
  const data: string[] = [];

  for (const line of raw.split(/\r\n|\n|\r/)) {
    if (!line || line.startsWith(':')) continue;
    const colon = line.indexOf(':');
    const field = colon < 0 ? line : line.slice(0, colon);
    let value = colon < 0 ? '' : line.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    if (field === 'event') event = value || 'message';
    if (field === 'data') data.push(value);
    if (field === 'id' && !value.includes('\0')) id = value;
  }

  if (data.length === 0) return null;
  return { event, data: data.join('\n'), id };
}
