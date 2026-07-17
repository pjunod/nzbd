//! Incremental yEnc decoder.
//!
//! Decodes a raw NNTP `BODY` byte stream (as read off the socket, in chunks of
//! arbitrary size) into file data. Handles, across any chunk boundary:
//!
//! - `=ybegin` / `=ypart` / `=yend` header, part and trailer lines
//! - escape sequences (`=` + char−64), including `=` split from its operand
//! - CRLF line structure (line breaks carry no data)
//! - NNTP dot-unstuffing (`\r\n..` → `\r\n.`) and end-of-article (`\r\n.\r\n`)
//! - running CRC32 of the decoded output
//!
//! This is the scalar reference implementation (phase 0). The `rapidyenc`
//! feature will bind the vendored SIMD decoder with this exact API, and this
//! implementation becomes the differential-testing oracle. UU decoding
//! (rare legacy posts) is a phase-2 addition.

use crc32fast::Hasher;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum YencError {
    #[error("not a yEnc article: {0}")]
    NotYenc(&'static str),
    #[error("malformed yEnc stream: {0}")]
    Malformed(&'static str),
    #[error("article ended before =yend trailer")]
    PrematureEnd,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct YencHeader {
    pub name: String,
    /// Total file size from `=ybegin size=`.
    pub size: u64,
    pub part: Option<u32>,
    pub total: Option<u32>,
    /// From `=ypart`: 1-based inclusive byte range of this part in the file.
    pub begin: Option<u64>,
    pub end: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct YencTrailer {
    /// Decoded size of this part per `=yend size=`.
    pub size: u64,
    pub part: Option<u32>,
    /// CRC of this part (`pcrc32`).
    pub pcrc32: Option<u32>,
    /// CRC of the whole file (`crc32`, usually only on single-part posts).
    pub crc32: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeResult {
    pub header: YencHeader,
    pub trailer: YencTrailer,
    /// Byte offset of this part within the output file (`begin − 1`, or 0).
    pub offset: u64,
    pub decoded_len: u64,
    /// CRC32 of the decoded bytes of this part.
    pub crc32: u32,
    /// `None` if the trailer carried no checksum to compare against.
    pub crc_ok: Option<bool>,
    pub len_ok: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Feed more bytes.
    NeedMore,
    /// `=yend` and the article terminator (`.` line) have been consumed.
    /// Call [`YencDecoder::take_result`]. Bytes past the consumed count
    /// returned by [`YencDecoder::push`] belong to the next response
    /// (pipelining) and were NOT consumed.
    Finished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Before the body: accumulating lines, waiting for `=ybegin`.
    Pre,
    /// `=ybegin` had `part=`: the next line must be `=ypart`.
    ExpectPart,
    Body,
    /// Line-start `=y` seen in body: accumulating a control line (`=yend`).
    Control,
    /// Trailer parsed; swallowing trailing lines until the `.` terminator
    /// line, which ends consumption (pipelining-safe).
    Drain,
}

const MAX_LINE: usize = 8192;

pub struct YencDecoder {
    state: State,
    line_buf: Vec<u8>,
    header: YencHeader,
    trailer: YencTrailer,
    hasher: Hasher,
    decoded_len: u64,
    // body sub-state
    esc: bool,
    at_line_start: bool,
    pending_eq: bool,
    dot: u8, // 0 = none, 1 = line-start '.' seen, 2 = ".\r" seen
    finished: bool,
}

impl Default for YencDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl YencDecoder {
    pub fn new() -> Self {
        YencDecoder {
            state: State::Pre,
            line_buf: Vec::with_capacity(256),
            header: YencHeader::default(),
            trailer: YencTrailer::default(),
            hasher: Hasher::new(),
            decoded_len: 0,
            esc: false,
            at_line_start: true,
            pending_eq: false,
            dot: 0,
            finished: false,
        }
    }

    pub fn header(&self) -> Option<&YencHeader> {
        if self.header.size > 0 || !self.header.name.is_empty() {
            Some(&self.header)
        } else {
            None
        }
    }

    /// Feed a chunk; decoded bytes are appended to `out`.
    ///
    /// Returns the status and the number of bytes **consumed** from `buf`.
    /// On [`Status::Finished`] the article terminator has been consumed and
    /// any remaining bytes belong to the next response on the connection —
    /// the caller must not feed them to this decoder again.
    pub fn push(&mut self, buf: &[u8], out: &mut Vec<u8>) -> Result<(Status, usize), YencError> {
        let hash_from = out.len();
        let mut i = 0usize;
        while i < buf.len() && !self.finished {
            let b = buf[i];
            i += 1;
            match self.state {
                State::Pre | State::ExpectPart | State::Control | State::Drain => {
                    if b == b'\n' {
                        self.take_line(out)?;
                    } else {
                        if self.line_buf.len() >= MAX_LINE {
                            return Err(YencError::Malformed("line too long"));
                        }
                        self.line_buf.push(b);
                    }
                }
                State::Body => self.body_byte(b, out)?,
            }
        }
        let newly = &out[hash_from..];
        if !newly.is_empty() {
            self.hasher.update(newly);
            self.decoded_len += newly.len() as u64;
        }
        Ok(if self.finished {
            (Status::Finished, i)
        } else {
            (Status::NeedMore, i)
        })
    }

    /// Available after [`Status::Finished`].
    pub fn take_result(&self) -> Option<DecodeResult> {
        if !self.finished {
            return None;
        }
        let crc = self.hasher.clone().finalize();
        let expected_crc = self.trailer.pcrc32.or(self.trailer.crc32);
        let expected_len = match (self.header.begin, self.header.end) {
            (Some(b), Some(e)) if e >= b => e - b + 1,
            _ => self.header.size,
        };
        Some(DecodeResult {
            offset: self.header.begin.map(|b| b.saturating_sub(1)).unwrap_or(0),
            decoded_len: self.decoded_len,
            crc32: crc,
            crc_ok: expected_crc.map(|c| c == crc),
            len_ok: self.decoded_len == self.trailer.size && self.decoded_len == expected_len,
            header: self.header.clone(),
            trailer: self.trailer.clone(),
        })
    }

    fn take_line(&mut self, _out: &mut [u8]) -> Result<(), YencError> {
        if self.line_buf.last() == Some(&b'\r') {
            self.line_buf.pop();
        }
        let line = std::mem::take(&mut self.line_buf);
        match self.state {
            State::Pre => {
                if line.starts_with(b"=ybegin ") {
                    self.header = parse_begin(&line)?;
                    if self.header.part.is_some() {
                        self.state = State::ExpectPart;
                    } else {
                        self.enter_body();
                    }
                } else if line == b"." {
                    return Err(YencError::NotYenc("article ended before =ybegin"));
                }
                // else: tolerate stray leading lines (headers, blank lines)
            }
            State::ExpectPart => {
                if line.starts_with(b"=ypart ") {
                    let (begin, end) = parse_part(&line)?;
                    self.header.begin = Some(begin);
                    self.header.end = Some(end);
                    self.enter_body();
                } else if line.is_empty() {
                    // tolerate blank line between =ybegin and =ypart
                    self.state = State::ExpectPart;
                } else {
                    return Err(YencError::Malformed("expected =ypart after =ybegin part=…"));
                }
            }
            State::Control => {
                if line.starts_with(b"=yend") {
                    self.trailer = parse_end(&line)?;
                    self.state = State::Drain;
                } else {
                    return Err(YencError::Malformed("unexpected =y control line in body"));
                }
            }
            State::Drain => {
                // Swallow trailing junk lines until the article terminator.
                if line == b"." {
                    self.finished = true;
                }
            }
            State::Body => unreachable!(),
        }
        Ok(())
    }

    fn enter_body(&mut self) {
        self.state = State::Body;
        self.esc = false;
        self.pending_eq = false;
        self.dot = 0;
        self.at_line_start = true;
    }

    #[inline]
    fn emit(&mut self, raw: u8, out: &mut Vec<u8>) {
        out.push(raw.wrapping_sub(42));
    }

    fn body_byte(&mut self, b: u8, out: &mut Vec<u8>) -> Result<(), YencError> {
        // 1. A pending escape consumes the next byte (leniently across CRLF).
        if self.esc {
            match b {
                b'\r' => {}
                b'\n' => self.at_line_start = true,
                _ => {
                    out.push(b.wrapping_sub(106)); // 42 + 64
                    self.esc = false;
                    self.at_line_start = false;
                }
            }
            return Ok(());
        }
        // 2. Line-start '=' waiting to see if this is a "=y" control line.
        if self.pending_eq {
            self.pending_eq = false;
            if b == b'y' {
                self.line_buf.clear();
                self.line_buf.extend_from_slice(b"=y");
                self.state = State::Control;
                return Ok(());
            }
            // It was an ordinary escape at line start.
            match b {
                b'\r' => self.esc = true,
                b'\n' => {
                    self.esc = true;
                    self.at_line_start = true;
                }
                _ => {
                    out.push(b.wrapping_sub(106));
                    self.at_line_start = false;
                }
            }
            return Ok(());
        }
        // 3. Dot-unstuffing / end-of-article detection at line start.
        match self.dot {
            1 => {
                self.dot = 0;
                match b {
                    b'.' => {
                        // "\r\n.." → one literal '.' of yEnc data
                        self.emit(b'.', out);
                        self.at_line_start = false;
                    }
                    b'\r' => self.dot = 2,
                    b'\n' => return Err(YencError::PrematureEnd), // bare ".\n"
                    _ => {
                        // Lenient: lone leading dot, then ordinary byte.
                        self.emit(b'.', out);
                        self.at_line_start = false;
                        return self.body_byte(b, out);
                    }
                }
                return Ok(());
            }
            2 => {
                self.dot = 0;
                if b == b'\n' {
                    // "\r\n.\r\n" terminator before any trailer
                    return Err(YencError::PrematureEnd);
                }
                // Lenient: ".\r" followed by data — emit the dot, drop the CR.
                self.emit(b'.', out);
                self.at_line_start = false;
                return self.body_byte(b, out);
            }
            _ => {}
        }
        if self.at_line_start {
            match b {
                b'.' => {
                    self.dot = 1;
                    return Ok(());
                }
                b'=' => {
                    self.pending_eq = true;
                    self.at_line_start = false;
                    return Ok(());
                }
                _ => {}
            }
        }
        // 4. Ordinary body byte.
        match b {
            b'=' => self.esc = true,
            b'\r' => {}
            b'\n' => self.at_line_start = true,
            _ => {
                self.emit(b, out);
                self.at_line_start = false;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Header/trailer line parsing
// ---------------------------------------------------------------------------

fn kv_fields(line: &[u8]) -> impl Iterator<Item = (&[u8], &[u8])> {
    line.split(|&b| b == b' ').filter_map(|tok| {
        let eq = tok.iter().position(|&b| b == b'=')?;
        Some((&tok[..eq], &tok[eq + 1..]))
    })
}

fn parse_u64(v: &[u8]) -> Option<u64> {
    std::str::from_utf8(v).ok()?.trim().parse().ok()
}

fn parse_hex32(v: &[u8]) -> Option<u32> {
    u32::from_str_radix(std::str::from_utf8(v).ok()?.trim(), 16).ok()
}

fn parse_begin(line: &[u8]) -> Result<YencHeader, YencError> {
    let mut h = YencHeader::default();
    // `name=` is always last and may contain spaces and '=': split it off first.
    let (attrs, name) = match find_subslice(line, b" name=") {
        Some(pos) => (&line[..pos], &line[pos + 6..]),
        None => (line, &b""[..]),
    };
    h.name = String::from_utf8_lossy(name).trim().to_string();
    for (k, v) in kv_fields(attrs) {
        match k {
            b"size" => h.size = parse_u64(v).ok_or(YencError::Malformed("bad size"))?,
            b"part" => h.part = Some(parse_u64(v).ok_or(YencError::Malformed("bad part"))? as u32),
            b"total" => h.total = parse_u64(v).map(|t| t as u32),
            _ => {}
        }
    }
    if h.size == 0 {
        return Err(YencError::Malformed("=ybegin without size"));
    }
    Ok(h)
}

fn parse_part(line: &[u8]) -> Result<(u64, u64), YencError> {
    let mut begin = None;
    let mut end = None;
    for (k, v) in kv_fields(line) {
        match k {
            b"begin" => begin = parse_u64(v),
            b"end" => end = parse_u64(v),
            _ => {}
        }
    }
    match (begin, end) {
        (Some(b), Some(e)) if b >= 1 && e >= b => Ok((b, e)),
        _ => Err(YencError::Malformed("bad =ypart range")),
    }
}

fn parse_end(line: &[u8]) -> Result<YencTrailer, YencError> {
    let mut t = YencTrailer::default();
    for (k, v) in kv_fields(line) {
        match k {
            b"size" => t.size = parse_u64(v).ok_or(YencError::Malformed("bad =yend size"))?,
            b"part" => t.part = parse_u64(v).map(|p| p as u32),
            b"pcrc32" => t.pcrc32 = parse_hex32(v),
            b"crc32" => t.crc32 = parse_hex32(v),
            _ => {}
        }
    }
    Ok(t)
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

// ---------------------------------------------------------------------------
// CRC32 combine (zlib's GF(2) matrix method).
// Lets per-segment CRCs computed during download be merged into whole-file
// CRCs without re-reading data — this powers par2 quick verification.
// ---------------------------------------------------------------------------

fn gf2_matrix_times(mat: &[u32; 32], mut vec: u32) -> u32 {
    let mut sum = 0u32;
    let mut i = 0usize;
    while vec != 0 {
        if vec & 1 != 0 {
            sum ^= mat[i];
        }
        vec >>= 1;
        i += 1;
    }
    sum
}

fn gf2_matrix_square(square: &mut [u32; 32], mat: &[u32; 32]) {
    for n in 0..32 {
        square[n] = gf2_matrix_times(mat, mat[n]);
    }
}

/// CRC32 of the concatenation of two byte ranges, given `crc1 = crc(A)`,
/// `crc2 = crc(B)` and `len2 = B.len()`.
pub fn crc32_combine(crc1: u32, crc2: u32, len2: u64) -> u32 {
    if len2 == 0 {
        return crc1;
    }
    let mut even = [0u32; 32];
    let mut odd = [0u32; 32];

    // operator for one zero bit
    odd[0] = 0xEDB8_8320;
    let mut row = 1u32;
    for item in odd.iter_mut().skip(1) {
        *item = row;
        row <<= 1;
    }
    gf2_matrix_square(&mut even, &odd); // two zero bits
    gf2_matrix_square(&mut odd, &even); // four zero bits

    let mut crc1 = crc1;
    let mut len2 = len2;
    loop {
        gf2_matrix_square(&mut even, &odd);
        if len2 & 1 != 0 {
            crc1 = gf2_matrix_times(&even, crc1);
        }
        len2 >>= 1;
        if len2 == 0 {
            break;
        }
        gf2_matrix_square(&mut odd, &even);
        if len2 & 1 != 0 {
            crc1 = gf2_matrix_times(&odd, crc1);
        }
        len2 >>= 1;
        if len2 == 0 {
            break;
        }
    }
    crc1 ^ crc2
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random bytes (LCG; no external deps).
    fn prng_bytes(seed: u64, len: usize) -> Vec<u8> {
        let mut x = seed | 1;
        (0..len)
            .map(|_| {
                x = x
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (x >> 33) as u8
            })
            .collect()
    }

    fn crc(data: &[u8]) -> u32 {
        let mut h = Hasher::new();
        h.update(data);
        h.finalize()
    }

    /// Test-side yEnc encoder + NNTP dot-stuffing + terminator.
    fn encode_article(
        data: &[u8],
        file_size: u64,
        part: Option<(u32, u32, u64, u64)>, // (part, total, begin, end)
        name: &str,
        line_len: usize,
    ) -> Vec<u8> {
        let mut body = Vec::new();
        let mut col = 0usize;
        for &b in data {
            let enc = b.wrapping_add(42);
            if matches!(enc, 0x00 | 0x0A | 0x0D | 0x3D) {
                body.push(b'=');
                body.push(enc.wrapping_add(64));
                col += 2;
            } else {
                body.push(enc);
                col += 1;
            }
            if col >= line_len {
                body.extend_from_slice(b"\r\n");
                col = 0;
            }
        }
        if col > 0 {
            body.extend_from_slice(b"\r\n");
        }

        let mut art = Vec::new();
        match part {
            Some((p, t, _, _)) => art.extend_from_slice(
                format!(
                    "=ybegin part={p} total={t} line={line_len} size={file_size} name={name}\r\n"
                )
                .as_bytes(),
            ),
            None => art.extend_from_slice(
                format!("=ybegin line={line_len} size={file_size} name={name}\r\n").as_bytes(),
            ),
        }
        if let Some((_, _, b, e)) = part {
            art.extend_from_slice(format!("=ypart begin={b} end={e}\r\n").as_bytes());
        }
        art.extend_from_slice(&body);
        let pc = crc(data);
        match part {
            Some((p, _, _, _)) => art.extend_from_slice(
                format!("=yend size={} part={p} pcrc32={pc:08x}\r\n", data.len()).as_bytes(),
            ),
            None => art.extend_from_slice(
                format!("=yend size={} crc32={pc:08x}\r\n", data.len()).as_bytes(),
            ),
        }

        // NNTP dot-stuffing over the whole article, then the terminator.
        let mut stuffed = Vec::with_capacity(art.len() + 8);
        let mut line_start = true;
        for &b in &art {
            if line_start && b == b'.' {
                stuffed.push(b'.');
            }
            stuffed.push(b);
            line_start = b == b'\n';
        }
        stuffed.extend_from_slice(b".\r\n");
        stuffed
    }

    fn decode_all(chunks: &[&[u8]]) -> (Vec<u8>, DecodeResult) {
        let mut dec = YencDecoder::new();
        let mut out = Vec::new();
        let mut finished = false;
        for c in chunks {
            let (status, consumed) = dec.push(c, &mut out).unwrap();
            match status {
                Status::Finished => {
                    assert_eq!(consumed, c.len(), "must consume the full final chunk");
                    finished = true;
                    break;
                }
                Status::NeedMore => assert_eq!(consumed, c.len()),
            }
        }
        assert!(finished, "decoder did not finish");
        let res = dec.take_result().unwrap();
        (out, res)
    }

    #[test]
    fn round_trip_single_part() {
        let data = prng_bytes(7, 1000);
        let art = encode_article(&data, 1000, None, "test file.bin", 128);
        let (out, res) = decode_all(&[&art]);
        assert_eq!(out, data);
        assert_eq!(res.header.name, "test file.bin");
        assert_eq!(res.offset, 0);
        assert_eq!(res.decoded_len, 1000);
        assert_eq!(res.crc_ok, Some(true));
        assert!(res.len_ok);
    }

    #[test]
    fn round_trip_every_split_point() {
        // The chunk-boundary test: any single split of the byte stream must
        // decode identically (escapes, dots, control lines all straddle).
        let data = prng_bytes(42, 600);
        let art = encode_article(&data, 600, None, "x", 32);
        for i in 1..art.len() {
            let (out, res) = decode_all(&[&art[..i], &art[i..]]);
            assert_eq!(out, data, "split at {i}");
            assert_eq!(res.crc_ok, Some(true), "split at {i}");
        }
    }

    #[test]
    fn round_trip_many_random_chunkings() {
        let data = prng_bytes(1234, 4096);
        let art = encode_article(&data, 4096, None, "big.bin", 128);
        let mut x = 99u64;
        for round in 0..50 {
            let mut chunks: Vec<&[u8]> = Vec::new();
            let mut pos = 0;
            while pos < art.len() {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(round + 1);
                let n = 1 + ((x >> 33) as usize % 97);
                let end = (pos + n).min(art.len());
                chunks.push(&art[pos..end]);
                pos = end;
            }
            let (out, res) = decode_all(&chunks);
            assert_eq!(out, data, "round {round}");
            assert_eq!(res.crc_ok, Some(true));
        }
    }

    #[test]
    fn specials_escapes_and_leading_dots() {
        // Bytes whose encodings are the four escaped characters, plus 0x04
        // (encodes to '.') positioned so encoded lines *start* with '.', which
        // then exercises NNTP dot-unstuffing.
        let mut data = vec![0xD6u8, 0xE0, 0xE3, 0x13]; // -> 0x00, 0x0A, 0x0D, '='
        data.extend(std::iter::repeat_n(0x04, 64)); // -> '.' everywhere
        data.extend_from_slice(&[0xD6, 0x13, 0x13, 0xE3]);
        let art = encode_article(&data, data.len() as u64, None, "dots", 8);
        // sanity: the stuffed article must actually contain a doubled dot
        assert!(
            find_subslice(&art, b"\n..").is_some(),
            "test article should exercise dot-stuffing"
        );
        for i in 1..art.len() {
            let (out, res) = decode_all(&[&art[..i], &art[i..]]);
            assert_eq!(out, data, "split at {i}");
            assert_eq!(res.crc_ok, Some(true));
        }
    }

    #[test]
    fn multipart_offsets() {
        let file = prng_bytes(5, 500);
        let part_data = &file[100..200]; // begin=101, end=200 (1-based inclusive)
        let art = encode_article(part_data, 500, Some((2, 5, 101, 200)), "multi.bin", 128);
        let (out, res) = decode_all(&[&art]);
        assert_eq!(out, part_data);
        assert_eq!(res.offset, 100);
        assert_eq!(res.decoded_len, 100);
        assert_eq!(res.header.size, 500);
        assert_eq!(res.trailer.part, Some(2));
        assert_eq!(res.crc_ok, Some(true));
        assert!(res.len_ok);
    }

    #[test]
    fn premature_termination_is_an_error() {
        let data = prng_bytes(3, 300);
        let art = encode_article(&data, 300, None, "x", 128);
        // Cut the article before the trailer and terminate it.
        let yend = find_subslice(&art, b"=yend").unwrap();
        let mut cut = art[..yend].to_vec();
        cut.extend_from_slice(b".\r\n");
        let mut dec = YencDecoder::new();
        let mut out = Vec::new();
        let err = dec.push(&cut, &mut out).unwrap_err();
        assert_eq!(err, YencError::PrematureEnd);
    }

    #[test]
    fn stops_consuming_at_terminator() {
        // Pipelining: bytes of the next response must not be swallowed.
        let data = prng_bytes(9, 200);
        let art = encode_article(&data, 200, None, "x", 64);
        let mut wire = art.clone();
        wire.extend_from_slice(b"222 0 <next@x> body follows\r\n");
        let mut dec = YencDecoder::new();
        let mut out = Vec::new();
        let (st, consumed) = dec.push(&wire, &mut out).unwrap();
        assert_eq!(st, Status::Finished);
        assert_eq!(
            consumed,
            art.len(),
            "must stop exactly after the terminator"
        );
        assert_eq!(&wire[consumed..], b"222 0 <next@x> body follows\r\n");
        assert_eq!(out, data);
        assert_eq!(dec.take_result().unwrap().crc_ok, Some(true));
    }

    #[test]
    fn junk_lines_after_yend_are_swallowed() {
        let data = prng_bytes(11, 100);
        let mut art = encode_article(&data, 100, None, "x", 64);
        // Insert a junk line between =yend and the terminator.
        let term = art.len() - 3;
        art.splice(term..term, b"some trailing garbage\r\n".iter().copied());
        let (out, res) = decode_all(&[&art]);
        assert_eq!(out, data);
        assert_eq!(res.crc_ok, Some(true));
    }

    #[test]
    fn crc_combine_matches_concatenation() {
        let a = prng_bytes(1, 1500);
        let b = prng_bytes(2, 777);
        let c = prng_bytes(3, 1);
        let whole = [a.clone(), b.clone(), c.clone()].concat();
        let combined = crc32_combine(
            crc32_combine(crc(&a), crc(&b), b.len() as u64),
            crc(&c),
            c.len() as u64,
        );
        assert_eq!(combined, crc(&whole));
        // zero-length second range is the identity
        assert_eq!(crc32_combine(crc(&a), 0, 0), crc(&a));
    }
}
