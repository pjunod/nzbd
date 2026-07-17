//! NNTP protocol: codec (commands, responses, multiline reading) and the
//! async transport ([`transport::NntpConnection`]: tokio + rustls).
//!
//! Body streaming does NOT go through [`MultilineReader`]: article bodies are
//! fed raw into `nzbd-yenc`, which performs its own dot-unstuffing inline.
//! COMPRESS DEFLATE (RFC 8054) is a later addition.

pub mod transport;

use std::fmt;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum NntpError {
    #[error("malformed response line")]
    MalformedResponse,
    #[error("argument contains CR/LF or NUL (injection rejected)")]
    IllegalArgument,
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// Well-known response codes (RFC 3977 / 4643).
pub mod codes {
    pub const GREETING_POSTING_OK: u16 = 200;
    pub const GREETING_NO_POSTING: u16 = 201;
    pub const CAPABILITIES_FOLLOW: u16 = 101;
    pub const GROUP_SELECTED: u16 = 211;
    pub const ARTICLE_FOLLOWS: u16 = 220;
    pub const HEAD_FOLLOWS: u16 = 221;
    pub const BODY_FOLLOWS: u16 = 222;
    pub const AUTH_ACCEPTED: u16 = 281;
    pub const PASSWORD_REQUIRED: u16 = 381;
    pub const NO_SUCH_GROUP: u16 = 411;
    pub const NO_ARTICLE_WITH_NUMBER: u16 = 420;
    pub const NO_NEXT_ARTICLE: u16 = 421;
    pub const NO_PREV_ARTICLE: u16 = 422;
    pub const NO_ARTICLE_IN_RANGE: u16 = 423;
    pub const NO_SUCH_ARTICLE: u16 = 430;
    pub const AUTH_REQUIRED: u16 = 480;
    pub const COMPRESS_ACTIVE: u16 = 206;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub code: u16,
    pub text: String,
}

impl Response {
    /// Parse a single response line (without trailing CRLF).
    pub fn parse(line: &[u8]) -> Result<Response, NntpError> {
        let line = strip_crlf(line);
        if line.len() < 3 || !line[..3].iter().all(u8::is_ascii_digit) {
            return Err(NntpError::MalformedResponse);
        }
        let code = std::str::from_utf8(&line[..3])
            .unwrap()
            .parse::<u16>()
            .map_err(|_| NntpError::MalformedResponse)?;
        let text = String::from_utf8_lossy(line.get(4..).unwrap_or(&[])).into_owned();
        Ok(Response { code, text })
    }

    pub fn is_positive(&self) -> bool {
        (200..300).contains(&self.code)
    }

    /// "This article does not exist on this server" — walks the failover
    /// ladder as a per-server article miss (never retried on the same server).
    pub fn is_article_missing(&self) -> bool {
        matches!(
            self.code,
            codes::NO_SUCH_ARTICLE
                | codes::NO_ARTICLE_WITH_NUMBER
                | codes::NO_ARTICLE_IN_RANGE
                | codes::NO_NEXT_ARTICLE
                | codes::NO_PREV_ARTICLE
        )
    }

    /// Positive responses that are followed by a dot-terminated data block.
    pub fn expects_multiline(&self) -> bool {
        matches!(
            self.code,
            codes::CAPABILITIES_FOLLOW
                | codes::ARTICLE_FOLLOWS
                | codes::HEAD_FOLLOWS
                | codes::BODY_FOLLOWS
                | 215 // LIST
                | 224 // OVER
                | 225 // HDR
                | 230 // NEWNEWS
                | 231 // NEWGROUPS
        )
    }
}

fn strip_crlf(mut line: &[u8]) -> &[u8] {
    while let [rest @ .., b'\r' | b'\n'] = line {
        line = rest;
    }
    line
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command<'a> {
    Capabilities,
    ModeReader,
    AuthInfoUser(&'a str),
    AuthInfoPass(&'a str),
    Group(&'a str),
    /// Message-id, with or without angle brackets; they are added on the wire.
    Body(&'a str),
    Head(&'a str),
    Article(&'a str),
    Stat(&'a str),
    CompressDeflate,
    Date,
    Quit,
}

impl Command<'_> {
    /// Serialize to the wire form including CRLF.
    /// Rejects CR/LF/NUL in arguments (command injection).
    pub fn encode(&self) -> Result<String, NntpError> {
        fn check(arg: &str) -> Result<&str, NntpError> {
            if arg.bytes().any(|b| matches!(b, b'\r' | b'\n' | 0)) {
                Err(NntpError::IllegalArgument)
            } else {
                Ok(arg)
            }
        }
        fn msgid(arg: &str) -> Result<String, NntpError> {
            let arg = check(arg)?.trim();
            let bare = arg.trim_start_matches('<').trim_end_matches('>');
            if bare.is_empty() {
                return Err(NntpError::IllegalArgument);
            }
            Ok(format!("<{bare}>"))
        }
        Ok(match self {
            Command::Capabilities => "CAPABILITIES\r\n".into(),
            Command::ModeReader => "MODE READER\r\n".into(),
            Command::AuthInfoUser(u) => format!("AUTHINFO USER {}\r\n", check(u)?),
            Command::AuthInfoPass(p) => format!("AUTHINFO PASS {}\r\n", check(p)?),
            Command::Group(g) => format!("GROUP {}\r\n", check(g)?),
            Command::Body(id) => format!("BODY {}\r\n", msgid(id)?),
            Command::Head(id) => format!("HEAD {}\r\n", msgid(id)?),
            Command::Article(id) => format!("ARTICLE {}\r\n", msgid(id)?),
            Command::Stat(id) => format!("STAT {}\r\n", msgid(id)?),
            Command::CompressDeflate => "COMPRESS DEFLATE\r\n".into(),
            Command::Date => "DATE\r\n".into(),
            Command::Quit => "QUIT\r\n".into(),
        })
    }
}

impl fmt::Display for Command<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Redacts nothing except AUTHINFO PASS.
        match self {
            Command::AuthInfoPass(_) => write!(f, "AUTHINFO PASS ***"),
            other => write!(f, "{}", other.encode().unwrap_or_default().trim_end()),
        }
    }
}

// ---------------------------------------------------------------------------
// Multiline (dot-terminated) text blocks: CAPABILITIES, HEAD, LIST, …
// ---------------------------------------------------------------------------

/// Incremental reader for dot-terminated multiline *text* responses.
/// Performs dot-unstuffing; chunk boundaries may fall anywhere.
#[derive(Debug, Default)]
pub struct MultilineReader {
    content: Vec<u8>,
    at_line_start: bool,
    dot: u8, // 0 none, 1 line-start '.', 2 ".\r"
    done: bool,
}

impl MultilineReader {
    pub fn new() -> Self {
        MultilineReader {
            at_line_start: true,
            ..Default::default()
        }
    }

    /// Feed bytes; returns how many were consumed (the rest belong to the
    /// next response once the terminator is seen) and whether the block is
    /// complete.
    pub fn push(&mut self, buf: &[u8]) -> (usize, bool) {
        let mut i = 0;
        while i < buf.len() && !self.done {
            let b = buf[i];
            i += 1;
            match self.dot {
                1 => {
                    self.dot = 0;
                    match b {
                        b'.' => {
                            self.content.push(b'.');
                            self.at_line_start = false;
                        }
                        b'\r' => self.dot = 2,
                        b'\n' => self.done = true, // lenient bare-LF terminator
                        _ => {
                            self.content.push(b'.');
                            self.content.push(b);
                            self.at_line_start = false;
                        }
                    }
                }
                2 => {
                    self.dot = 0;
                    if b == b'\n' {
                        self.done = true;
                    } else {
                        self.content.extend_from_slice(b".\r");
                        self.content.push(b);
                        self.at_line_start = b == b'\n';
                    }
                }
                _ => {
                    if self.at_line_start && b == b'.' {
                        self.dot = 1;
                    } else {
                        self.content.push(b);
                        self.at_line_start = b == b'\n';
                    }
                }
            }
        }
        (i, self.done)
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    /// The unstuffed content; call once `is_done()`.
    pub fn into_lines(self) -> Vec<String> {
        self.content
            .split(|&b| b == b'\n')
            .map(|l| String::from_utf8_lossy(strip_crlf(l)).into_owned())
            .filter(|l| !l.is_empty())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_responses() {
        let r = Response::parse(b"430 No such article\r\n").unwrap();
        assert_eq!(r.code, 430);
        assert_eq!(r.text, "No such article");
        assert!(r.is_article_missing());
        assert!(!r.is_positive());

        let r = Response::parse(b"222 0 <x@y> body follows").unwrap();
        assert!(r.is_positive());
        assert!(r.expects_multiline());

        let r = Response::parse(b"281 Authentication accepted").unwrap();
        assert!(r.is_positive());
        assert!(!r.expects_multiline());

        assert_eq!(
            Response::parse(b"xx oops"),
            Err(NntpError::MalformedResponse)
        );
        assert_eq!(Response::parse(b"20"), Err(NntpError::MalformedResponse));
    }

    #[test]
    fn encodes_commands_and_wraps_msgids() {
        assert_eq!(
            Command::Body("abc@def").encode().unwrap(),
            "BODY <abc@def>\r\n"
        );
        assert_eq!(
            Command::Body("<abc@def>").encode().unwrap(),
            "BODY <abc@def>\r\n"
        );
        assert_eq!(
            Command::AuthInfoUser("user").encode().unwrap(),
            "AUTHINFO USER user\r\n"
        );
        assert_eq!(Command::Quit.encode().unwrap(), "QUIT\r\n");
    }

    #[test]
    fn rejects_injection() {
        assert_eq!(
            Command::Body("a@b>\r\nQUIT").encode(),
            Err(NntpError::IllegalArgument)
        );
        assert_eq!(
            Command::Group("alt.bin\r\n").encode(),
            Err(NntpError::IllegalArgument)
        );
        assert_eq!(
            Command::Body("<>").encode(),
            Err(NntpError::IllegalArgument)
        );
    }

    #[test]
    fn display_redacts_password() {
        assert_eq!(
            Command::AuthInfoPass("hunter2").to_string(),
            "AUTHINFO PASS ***"
        );
        assert_eq!(Command::Body("a@b").to_string(), "BODY <a@b>");
    }

    #[test]
    fn multiline_reader_unstuffs_and_terminates() {
        let wire = b"line one\r\n..starts with dot\r\nmiddle\r\n.\r\n299 next response";
        let mut r = MultilineReader::new();
        let (consumed, done) = r.push(wire);
        assert!(done);
        assert_eq!(&wire[consumed..], b"299 next response");
        assert_eq!(
            r.into_lines(),
            vec!["line one", ".starts with dot", "middle"]
        );
    }

    #[test]
    fn multiline_reader_across_chunk_boundaries() {
        let wire = b"a\r\n..b\r\n.\r\n";
        for i in 1..wire.len() {
            let mut r = MultilineReader::new();
            let (c1, done1) = r.push(&wire[..i]);
            assert_eq!(c1, i);
            let mut done = done1;
            if !done {
                let (_, d2) = r.push(&wire[i..]);
                done = d2;
            }
            assert!(done, "split at {i}");
            assert_eq!(r.into_lines(), vec!["a", ".b"], "split at {i}");
        }
    }
}
