//! Port of `aux_/http_parser` — incremental HTTP request/response
//! parser: status/request line, header multimap (lowercase names),
//! content-length / content-range / connection / transfer-encoding,
//! and chunked-body range tracking (`parse_chunk_header`,
//! `chunked_ranges`, `collapse_chunk_headers` left to the caller).

/// `flags_t::dont_parse_chunks`.
pub const DONT_PARSE_CHUNKS: i32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    ReadStatus,
    ReadHeader,
    ReadBody,
    Error,
}

/// `http_parser` — feed growing receive buffers via [`incoming`];
/// the parser keeps absolute positions (`m_recv_pos`,
/// `m_body_start_pos`) like upstream's `span`-based design.
#[derive(Debug)]
pub struct HttpParser {
    m_recv_pos: i64,
    /// header name (lowercase) → value, insertion order.
    m_header: Vec<(String, String)>,
    m_method: String,
    m_path: String,
    m_protocol: String,
    m_server_message: String,
    m_content_length: i64,
    m_range_start: i64,
    m_range_end: i64,
    /// `(start, end)` absolute byte ranges of chunk payloads.
    m_chunked_ranges: Vec<(i64, i64)>,
    m_cur_chunk_end: i64,
    m_status_code: i32,
    m_chunk_header_size: i32,
    m_partial_chunk_header: i32,
    m_flags: i32,
    m_body_start_pos: i64,
    m_recv_len: usize,
    state: State,
    m_connection_close: bool,
    m_chunked_encoding: bool,
    m_finished: bool,
}

impl HttpParser {
    /// `http_parser(flags)`.
    #[must_use]
    pub fn new(flags: i32) -> Self {
        Self {
            m_recv_pos: 0,
            m_header: Vec::new(),
            m_method: String::new(),
            m_path: String::new(),
            m_protocol: String::new(),
            m_server_message: String::new(),
            m_content_length: -1,
            m_range_start: -1,
            m_range_end: -1,
            m_chunked_ranges: Vec::new(),
            m_cur_chunk_end: -1,
            m_status_code: -1,
            m_chunk_header_size: 0,
            m_partial_chunk_header: 0,
            m_flags: flags,
            m_body_start_pos: 0,
            m_recv_len: 0,
            state: State::ReadStatus,
            m_connection_close: false,
            m_chunked_encoding: false,
            m_finished: false,
        }
    }

    /// `header(key)` — first value for `key` (case-insensitive).
    #[must_use]
    pub fn header(&self, key: &str) -> Option<&str> {
        let k = key.to_ascii_lowercase();
        self.m_header
            .iter()
            .find(|(n, _)| *n == k)
            .map(|(_, v)| v.as_str())
    }
    /// All headers (multimap — duplicates preserved).
    #[must_use]
    pub fn headers(&self) -> &[(String, String)] {
        &self.m_header
    }
    /// `protocol()`.
    #[must_use]
    pub fn protocol(&self) -> &str {
        &self.m_protocol
    }
    /// `status_code()`.
    #[must_use]
    pub fn status_code(&self) -> i32 {
        self.m_status_code
    }
    /// `method()` (lowercased, requests only).
    #[must_use]
    pub fn method(&self) -> &str {
        &self.m_method
    }
    /// `path()` (requests only).
    #[must_use]
    pub fn path(&self) -> &str {
        &self.m_path
    }
    /// `message()` — the reason phrase (responses).
    #[must_use]
    pub fn message(&self) -> &str {
        &self.m_server_message
    }
    /// `header_finished()`.
    #[must_use]
    pub fn header_finished(&self) -> bool {
        self.state == State::ReadBody
    }
    /// `finished()`.
    #[must_use]
    pub fn finished(&self) -> bool {
        self.m_finished
    }
    /// `body_start()`.
    #[must_use]
    pub fn body_start(&self) -> i64 {
        self.m_body_start_pos
    }
    /// `content_length()`.
    #[must_use]
    pub fn content_length(&self) -> i64 {
        self.m_content_length
    }
    /// `content_range()`.
    #[must_use]
    pub fn content_range(&self) -> (i64, i64) {
        (self.m_range_start, self.m_range_end)
    }
    /// `chunked_encoding()`.
    #[must_use]
    pub fn chunked_encoding(&self) -> bool {
        self.m_chunked_encoding
    }
    /// `chunks()`.
    #[must_use]
    pub fn chunks(&self) -> &[(i64, i64)] {
        &self.m_chunked_ranges
    }
    /// `connection_close()`.
    #[must_use]
    pub fn connection_close(&self) -> bool {
        self.m_connection_close
    }

    /// `get_body` — the body slice of `buf` (the same buffer passed
    /// to `incoming`).
    #[must_use]
    pub fn get_body<'a>(&self, buf: &'a [u8]) -> &'a [u8] {
        if self.state != State::ReadBody {
            return &[];
        }
        let received = self.m_recv_pos - self.m_body_start_pos;
        let body_length = if self.m_chunked_encoding && !self.m_chunked_ranges.is_empty() {
            (self.m_chunked_ranges.last().map_or(0, |r| r.1) - self.m_body_start_pos).min(received)
        } else if self.m_content_length < 0 {
            received
        } else {
            self.m_content_length.min(received)
        };
        let start = self.m_body_start_pos.max(0) as usize;
        let end = (self.m_body_start_pos + body_length).max(0) as usize;
        &buf[start.min(buf.len())..end.min(buf.len())]
    }

    /// `reset()`.
    pub fn reset(&mut self) {
        *self = Self::new(self.m_flags);
    }

    /// `incoming(recv_buffer, error)` — returns `(payload_bytes,
    /// overhead_bytes)` consumed in this call. `recv_buffer` must
    /// contain the full stream from the start (upstream keeps
    /// absolute offsets into it).
    pub fn incoming(&mut self, recv_buffer: &[u8], error: &mut bool) -> (usize, usize) {
        let mut ret = (0usize, 0usize);
        let start_pos_total = self.m_recv_len;
        if start_pos_total == recv_buffer.len() {
            return ret;
        }
        self.m_recv_len = recv_buffer.len();
        if self.state == State::Error {
            *error = true;
            return ret;
        }
        let mut pos = self.m_recv_pos as usize;
        let mut start_pos = start_pos_total;

        'restart: loop {
            if self.state == State::ReadStatus {
                let newline = match recv_buffer[pos..].iter().position(|&b| b == b'\n') {
                    Some(i) => pos + i,
                    None => {
                        ret.1 += self.m_recv_len - start_pos;
                        return ret;
                    }
                };
                if newline == pos {
                    self.state = State::Error;
                    *error = true;
                    return ret;
                }
                let mut line_end = newline;
                if pos != line_end && recv_buffer[line_end - 1] == b'\r' {
                    line_end -= 1;
                }
                let line = &recv_buffer[pos..line_end];
                let incoming = newline + 1 - pos;
                self.m_recv_pos += incoming as i64;
                ret.1 += (newline + 1) - start_pos.min(newline + 1);
                pos = newline + 1;

                // split on spaces
                let mut it = line.split(|&b| b == b' ');
                let first = String::from_utf8_lossy(it.next().unwrap_or(b"")).to_string();
                if first.starts_with("HTTP/") {
                    self.m_protocol = first;
                    let code_str = it.next().unwrap_or(b"");
                    let code_s = String::from_utf8_lossy(code_str).to_string();
                    match code_s.parse::<i32>() {
                        Ok(c) if c >= 0 && !code_s.is_empty() => self.m_status_code = c,
                        _ => {
                            self.state = State::Error;
                            *error = true;
                            return ret;
                        }
                    }
                    self.m_server_message =
                        String::from_utf8_lossy(it.next().unwrap_or(b"")).to_string();
                    if self.m_protocol == "HTTP/1.0" {
                        self.m_connection_close = true;
                    }
                } else {
                    self.m_method = first.to_ascii_lowercase();
                    self.m_content_length = 0;
                    self.m_protocol.clear();
                    self.m_path = String::from_utf8_lossy(it.next().unwrap_or(b"")).to_string();
                    self.m_protocol = String::from_utf8_lossy(it.next().unwrap_or(b"")).to_string();
                    self.m_status_code = 0;
                }
                self.state = State::ReadHeader;
                start_pos = pos;
            }

            if self.state == State::ReadHeader {
                while let Some(nl) = recv_buffer[pos..].iter().position(|&b| b == b'\n') {
                    let newline = pos + nl;
                    let mut line_end = newline;
                    if pos != line_end && recv_buffer[line_end - 1] == b'\r' {
                        line_end -= 1;
                    }
                    let line = &recv_buffer[pos..line_end];
                    self.m_recv_pos += (newline + 1 - pos) as i64;
                    pos = newline + 1;

                    let sep = line.iter().position(|&b| b == b':');
                    let sep = match sep {
                        Some(s) => s,
                        None => {
                            if self.m_status_code == 100 {
                                self.state = State::ReadStatus;
                                ret.1 += newline + 1 - start_pos.min(newline + 1);
                                continue 'restart;
                            }
                            self.state = State::ReadBody;
                            self.m_body_start_pos = self.m_recv_pos;
                            break;
                        }
                    };
                    let name = String::from_utf8_lossy(&line[..sep]).to_ascii_lowercase();
                    let mut vs = sep + 1;
                    while vs < line.len() && (line[vs] == b' ' || line[vs] == b'\t') {
                        vs += 1;
                    }
                    let value = String::from_utf8_lossy(&line[vs..]).to_string();
                    self.m_header.push((name.clone(), value.clone()));

                    if name == "content-length" {
                        match value.trim().parse::<i64>() {
                            Ok(v) if (0..i64::MAX).contains(&v) => self.m_content_length = v,
                            _ => {
                                self.state = State::Error;
                                *error = true;
                                return ret;
                            }
                        }
                    } else if name == "connection" {
                        self.m_connection_close =
                            value.len() >= 5 && value[..5].eq_ignore_ascii_case("close");
                    } else if name == "content-range" {
                        let mut v = value.as_str();
                        if v.len() >= 6 && v[..6].eq_ignore_ascii_case("bytes ") {
                            v = &v[6..];
                        }
                        let (range_ok, s, e) = parse_range(v);
                        if !range_ok {
                            self.state = State::Error;
                            *error = true;
                            return ret;
                        }
                        self.m_range_start = s;
                        self.m_range_end = e;
                        self.m_content_length = e - s + 1;
                    } else if name == "transfer-encoding" {
                        self.m_chunked_encoding =
                            value.len() >= 7 && value[..7].eq_ignore_ascii_case("chunked");
                    }
                }
                ret.1 += pos.saturating_sub(start_pos);
            }

            if self.state == State::ReadBody {
                let mut incoming = recv_buffer.len() - pos;
                if self.m_chunked_encoding && (self.m_flags & DONT_PARSE_CHUNKS) == 0 {
                    if self.m_cur_chunk_end == -1 {
                        self.m_cur_chunk_end = self.m_body_start_pos;
                    }
                    while self.m_cur_chunk_end <= self.m_recv_pos + incoming as i64
                        && !self.m_finished
                        && incoming > 0
                    {
                        let payload = self.m_cur_chunk_end - self.m_recv_pos;
                        if payload > 0 {
                            self.m_recv_pos += payload;
                            ret.0 += payload as usize;
                            incoming -= payload as usize;
                        }
                        let buf = &recv_buffer[self.m_cur_chunk_end as usize..];
                        match self.parse_chunk_header(buf) {
                            Some((chunk_size, header_size)) => {
                                if chunk_size < 0
                                    || chunk_size
                                        > i64::MAX - self.m_cur_chunk_end - header_size as i64
                                {
                                    self.state = State::Error;
                                    *error = true;
                                    return ret;
                                }
                                if chunk_size > 0 {
                                    self.m_chunked_ranges.push((
                                        self.m_cur_chunk_end + header_size as i64,
                                        self.m_cur_chunk_end + header_size as i64 + chunk_size,
                                    ));
                                }
                                self.m_cur_chunk_end += header_size as i64 + chunk_size;
                                if chunk_size == 0 {
                                    self.m_finished = true;
                                }
                                let hs =
                                    i64::from(header_size) - i64::from(self.m_partial_chunk_header);
                                self.m_partial_chunk_header = 0;
                                self.m_chunk_header_size += hs as i32;
                                self.m_recv_pos += hs;
                                ret.1 += hs as usize;
                                incoming -= hs as usize;
                            }
                            None => {
                                self.m_partial_chunk_header += incoming as i32;
                                let hs = incoming as i64;
                                self.m_chunk_header_size += hs as i32;
                                self.m_recv_pos += hs;
                                ret.1 += incoming;
                                incoming = 0;
                            }
                        }
                    }
                    if incoming > 0 {
                        self.m_recv_pos += incoming as i64;
                        ret.0 += incoming;
                    }
                } else {
                    let payload_received =
                        self.m_recv_pos - self.m_body_start_pos + incoming as i64;
                    if payload_received > self.m_content_length && self.m_content_length >= 0 {
                        incoming = usize::try_from(
                            self.m_content_length - self.m_recv_pos + self.m_body_start_pos,
                        )
                        .unwrap_or(0);
                    }
                    self.m_recv_pos += incoming as i64;
                    ret.0 += incoming;
                }

                if self.m_content_length >= 0
                    && self.m_recv_pos - self.m_body_start_pos >= self.m_content_length
                    && !self.m_chunked_encoding
                {
                    self.m_finished = true;
                }
            }
            return ret;
        }
    }

    /// `parse_chunk_header` — `(chunk_size, header_size)` or `None`
    /// when the buffer ends mid-header.
    pub fn parse_chunk_header(&mut self, buf: &[u8]) -> Option<(i64, i32)> {
        let mut pos = 0usize;
        if pos < buf.len() && buf[pos] == b'\r' {
            pos += 1;
        }
        if pos < buf.len() && buf[pos] == b'\n' {
            pos += 1;
        }
        if pos == buf.len() {
            return None;
        }
        let newline = buf[pos..]
            .iter()
            .position(|&b| b == b'\n')
            .map(|i| pos + i + 1)?;
        let mut header_size = newline as i32;

        // hex chunk length up to ';'
        let mut size: i64 = 0;
        for &b in &buf[pos..newline] {
            if b == b'\r' || b == b'\n' {
                continue;
            }
            if b == b';' {
                break;
            }
            let digit = (b as char).to_digit(16);
            match digit {
                Some(d) => {
                    if size >= i64::MAX / 16 {
                        return Some((-1, header_size));
                    }
                    size = size * 16 + i64::from(d);
                }
                None => return Some((-1, header_size)),
            }
        }
        if size != 0 {
            return Some((size, header_size));
        }

        // terminator chunk — read tail headers
        let mut tail: Vec<(String, String)> = Vec::new();
        let mut p = newline;
        loop {
            let nl = buf[p..].iter().position(|&b| b == b'\n').map(|i| p + i)?;
            let mut le = nl;
            if p != le && buf[le - 1] == b'\r' {
                le -= 1;
            }
            let line = &buf[p..le];
            p = nl + 1;
            let sep = match line.iter().position(|&b| b == b':') {
                Some(s) => s,
                None => {
                    header_size = p as i32;
                    for h in tail {
                        self.m_header.push(h);
                    }
                    return Some((0, header_size));
                }
            };
            let name = String::from_utf8_lossy(&line[..sep]).to_ascii_lowercase();
            let mut vs = sep + 1;
            while vs < line.len() && (line[vs] == b' ' || line[vs] == b'\t') {
                vs += 1;
            }
            tail.push((name, String::from_utf8_lossy(&line[vs..]).to_string()));
        }
    }
}

/// Parse `start-end` for `content-range` (`strtoll` semantics:
/// unparseable tail is an error).
/// `strtoll` semantics — parse the leading decimal prefix.
fn strtoll(s: &str) -> Option<(i64, usize)> {
    let s = s.trim_start();
    let mut end = 0;
    for (i, c) in s.char_indices() {
        if c.is_ascii_digit() || (i == 0 && (c == '+' || c == '-')) {
            end = i + c.len_utf8();
        } else {
            break;
        }
    }
    if end == 0 {
        return None;
    }
    s[..end].parse::<i64>().ok().map(|v| (v, end))
}

/// Parse `start-end` for `content-range`.
fn parse_range(v: &str) -> (bool, i64, i64) {
    let v = v.trim();
    let (s, used) = match strtoll(v) {
        Some((s, u)) if (0..i64::MAX).contains(&s) => (s, u),
        _ => return (false, -1, -1),
    };
    let rest = &v[used..];
    if !rest.starts_with('-') {
        return (false, -1, -1);
    }
    let (e, _) = match strtoll(&rest[1..]) {
        Some((e, u)) if (0..i64::MAX).contains(&e) => (e, u),
        _ => return (false, -1, -1),
    };
    if e < s {
        return (false, -1, -1);
    }
    (true, s, e)
}
