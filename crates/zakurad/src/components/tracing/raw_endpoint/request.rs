//! The HTTP trust boundary: what a request may ask for, and how its range and
//! preconditions apply to one trace file. Nothing here touches the filesystem.

use hyper::{
    header::{HeaderMap, HOST, IF_MATCH, IF_RANGE, ORIGIN, RANGE},
    Method, Request, StatusCode,
};

use super::file::is_table_name;

/// A request the endpoint refuses, with the reason sent as the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Refusal {
    pub(super) status: StatusCode,
    pub(super) reason: &'static str,
}

impl Refusal {
    pub(super) const fn new(status: StatusCode, reason: &'static str) -> Self {
        Self { status, reason }
    }
}

/// A validated request.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct TraceRequest {
    /// `HEAD` sends the same headers as `GET` and no body.
    pub(super) head: bool,
    pub(super) target: Target,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Target {
    /// `/traces`: the table index.
    Index,
    /// `/traces/<table>`: one table's file.
    Table(TableRequest),
}

/// A request for one table, with its `Range` and preconditions parsed.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct TableRequest {
    pub(super) name: String,
    range: Option<ByteRange>,
    if_match: Option<EntityTags>,
    if_range: Option<String>,
}

/// What to send for a [`TableRequest`] once the file's length and tag are known.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Plan {
    /// 200 with the whole file.
    Full,
    /// 206 with bytes `start..end`.
    Partial { start: u64, end: u64 },
    /// 416: the range starts at or past the end of the file.
    Unsatisfiable,
    /// 412: `If-Match` names a different file.
    PreconditionFailed,
}

/// One `bytes=` range. Ends are inclusive, as on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ByteRange {
    /// `bytes=<first>-`
    From(u64),
    /// `bytes=<first>-<last>`
    Span(u64, u64),
    /// `bytes=-<suffix>`: the last `suffix` bytes.
    Suffix(u64),
}

#[derive(Debug, PartialEq, Eq)]
enum EntityTags {
    Any,
    List(Vec<String>),
}

/// Validate `request` against the endpoint's bound `authority`.
///
/// Browsers and DNS rebinding are refused first: a request with `Origin`, an
/// absolute URI, or a `Host` other than the literal bound address is 403.
pub(super) fn parse<B>(request: &Request<B>, authority: &str) -> Result<TraceRequest, Refusal> {
    let headers = request.headers();
    if headers.contains_key(ORIGIN)
        || request.uri().authority().is_some()
        || headers.get(HOST).and_then(|host| host.to_str().ok()) != Some(authority)
    {
        return Err(Refusal::new(
            StatusCode::FORBIDDEN,
            "requests need Host set to the bound address, and no Origin",
        ));
    }
    let head = match *request.method() {
        Method::GET => false,
        Method::HEAD => true,
        _ => {
            return Err(Refusal::new(
                StatusCode::METHOD_NOT_ALLOWED,
                "only GET and HEAD are supported",
            ))
        }
    };
    if request.uri().query().is_some() {
        return Err(Refusal::new(
            StatusCode::BAD_REQUEST,
            "query parameters are not supported",
        ));
    }
    let target = match request.uri().path() {
        "/traces" | "/traces/" => Target::Index,
        path => {
            let name = path
                .strip_prefix("/traces/")
                .filter(|name| is_table_name(name))
                .ok_or(Refusal::new(StatusCode::NOT_FOUND, "no such table"))?;
            Target::Table(TableRequest::parse(name, headers)?)
        }
    };
    Ok(TraceRequest { head, target })
}

impl TableRequest {
    fn parse(name: &str, headers: &HeaderMap) -> Result<Self, Refusal> {
        let text = |name| {
            headers
                .get(name)
                .map(|value: &hyper::header::HeaderValue| {
                    value
                        .to_str()
                        .map(str::trim)
                        .map_err(|_| Refusal::new(StatusCode::BAD_REQUEST, "non-ASCII header"))
                })
                .transpose()
        };
        Ok(Self {
            name: name.to_owned(),
            range: text(RANGE)?.map(ByteRange::parse).transpose()?.flatten(),
            if_match: text(IF_MATCH)?.map(EntityTags::parse),
            if_range: text(IF_RANGE)?.map(str::to_owned),
        })
    }

    /// Apply the preconditions and range to a file of `len` bytes tagged `etag`,
    /// in the order RFC 9110 section 13.2.2 gives: `If-Match`, then `If-Range`,
    /// then `Range`.
    pub(super) fn plan(&self, len: u64, etag: &str) -> Plan {
        if self
            .if_match
            .as_ref()
            .is_some_and(|tags| !tags.matches(etag))
        {
            return Plan::PreconditionFailed;
        }
        // A failed If-Range asks for the whole current file instead of a part.
        let if_range_holds = self
            .if_range
            .as_deref()
            .is_none_or(|tag| strong_match(tag, etag));
        match self.range.filter(|_| if_range_holds) {
            None => Plan::Full,
            Some(range) => range.resolve(len),
        }
    }
}

impl ByteRange {
    /// Parse a `Range` value. Units other than `bytes` are ignored, as RFC 9110
    /// requires. Malformed and multiple ranges are refused, never served whole.
    fn parse(value: &str) -> Result<Option<Self>, Refusal> {
        let Some(spec) = value.strip_prefix("bytes=") else {
            return Ok(None);
        };
        let invalid = Refusal::new(
            StatusCode::RANGE_NOT_SATISFIABLE,
            "expected one range: bytes=<first>-[<last>] or bytes=-<suffix>",
        );
        let (first, last) = spec.trim().split_once('-').ok_or(invalid)?;
        let number = |digits: &str| -> Result<u64, Refusal> {
            if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid);
            }
            digits.parse().map_err(|_| invalid)
        };
        let range = match (first, last) {
            ("", suffix) => Self::Suffix(number(suffix)?),
            (first, "") => Self::From(number(first)?),
            (first, last) => {
                let (first, last) = (number(first)?, number(last)?);
                if last < first {
                    return Err(invalid);
                }
                Self::Span(first, last)
            }
        };
        Ok(Some(range))
    }

    fn resolve(self, len: u64) -> Plan {
        let (start, end) = match self {
            Self::From(first) => (first, len),
            // `last` is inclusive and may lie past the end of the file.
            Self::Span(first, last) => (first, last.saturating_add(1).min(len)),
            Self::Suffix(0) => return Plan::Unsatisfiable,
            Self::Suffix(suffix) => (len.saturating_sub(suffix), len),
        };
        if start >= len {
            Plan::Unsatisfiable
        } else {
            Plan::Partial { start, end }
        }
    }
}

impl EntityTags {
    fn parse(value: &str) -> Self {
        if value == "*" {
            return Self::Any;
        }
        Self::List(value.split(',').map(|tag| tag.trim().to_owned()).collect())
    }

    fn matches(&self, etag: &str) -> bool {
        match self {
            Self::Any => true,
            Self::List(tags) => tags.iter().any(|tag| strong_match(tag, etag)),
        }
    }
}

/// RFC 9110 strong comparison: weak tags and dates never match.
fn strong_match(tag: &str, etag: &str) -> bool {
    !tag.starts_with("W/") && tag == etag
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUTHORITY: &str = "127.0.0.1:9000";
    const TAG: &str = "\"1-2-3\"";

    fn request(method: Method, uri: &str, headers: &[(&str, &str)]) -> Request<()> {
        let mut builder = Request::builder()
            .method(method)
            .uri(uri)
            .header(HOST, AUTHORITY);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        builder.body(()).unwrap()
    }

    fn table(headers: &[(&str, &str)]) -> TableRequest {
        match parse(&request(Method::GET, "/traces/conn", headers), AUTHORITY)
            .unwrap()
            .target
        {
            Target::Table(table) => table,
            Target::Index => panic!("expected a table"),
        }
    }

    fn status(request: Request<()>) -> StatusCode {
        parse(&request, AUTHORITY).unwrap_err().status
    }

    #[test]
    fn browsers_and_foreign_hosts_are_refused() {
        let forbidden = StatusCode::FORBIDDEN;
        let origin = [("origin", "http://example.com")];
        assert_eq!(
            status(request(Method::GET, "/traces/conn", &origin)),
            forbidden
        );
        let absolute = request(Method::GET, "http://127.0.0.1:9000/traces/conn", &[]);
        assert_eq!(status(absolute), forbidden);
        let mut foreign = request(Method::GET, "/traces/conn", &[]);
        foreign
            .headers_mut()
            .insert(HOST, "attacker.example:9000".parse().unwrap());
        assert_eq!(status(foreign), forbidden);
        let mut missing = request(Method::GET, "/traces/conn", &[]);
        missing.headers_mut().remove(HOST);
        assert_eq!(status(missing), forbidden);
    }

    #[test]
    fn paths_methods_and_queries_are_checked() {
        let parsed = |method, uri| parse(&request(method, uri, &[]), AUTHORITY);
        assert_eq!(
            parsed(Method::GET, "/traces").unwrap().target,
            Target::Index
        );
        assert_eq!(
            parsed(Method::GET, "/traces/").unwrap().target,
            Target::Index
        );
        assert!(parsed(Method::HEAD, "/traces/conn").unwrap().head);
        assert!(!parsed(Method::GET, "/traces/block_sync").unwrap().head);
        for (method, uri, expected) in [
            (Method::POST, "/traces/conn", StatusCode::METHOD_NOT_ALLOWED),
            (
                Method::GET,
                "/traces/conn?offset=0",
                StatusCode::BAD_REQUEST,
            ),
            (Method::GET, "/traces/../secret", StatusCode::NOT_FOUND),
            (
                Method::GET,
                "/traces/%2e%2e%2fsecret",
                StatusCode::NOT_FOUND,
            ),
            (Method::GET, "/traces/conn.jsonl", StatusCode::NOT_FOUND),
            (Method::GET, "/traces/a/b", StatusCode::NOT_FOUND),
            (Method::GET, "/metrics", StatusCode::NOT_FOUND),
        ] {
            assert_eq!(parsed(method, uri).unwrap_err().status, expected, "{uri}");
        }
    }

    #[test]
    fn ranges_follow_rfc_9110() {
        let plan = |range: &str, len| table(&[("range", range)]).plan(len, TAG);
        let partial = |start, end| Plan::Partial { start, end };
        assert_eq!(plan("bytes=0-", 10), partial(0, 10));
        assert_eq!(plan("bytes=4-", 10), partial(4, 10));
        assert_eq!(plan("bytes=2-4", 10), partial(2, 5));
        assert_eq!(plan("bytes=2-999", 10), partial(2, 10));
        assert_eq!(plan("bytes=-3", 10), partial(7, 10));
        assert_eq!(plan("bytes=-30", 10), partial(0, 10));
        assert_eq!(plan(" bytes=0-0", 10), partial(0, 1));
        // A client that has read everything learns it from a 416.
        assert_eq!(plan("bytes=10-", 10), Plan::Unsatisfiable);
        assert_eq!(plan("bytes=0-", 0), Plan::Unsatisfiable);
        assert_eq!(plan("bytes=-0", 10), Plan::Unsatisfiable);
        assert_eq!(plan("items=0-1", 10), Plan::Full);
        assert_eq!(table(&[]).plan(10, TAG), Plan::Full);
        for range in [
            "bytes=4-3",
            "bytes=0-1,3-4",
            "bytes=-",
            "bytes=+1-",
            "bytes=x-",
            "bytes=18446744073709551616-",
        ] {
            let request = request(Method::GET, "/traces/conn", &[("range", range)]);
            assert_eq!(
                status(request),
                StatusCode::RANGE_NOT_SATISFIABLE,
                "{range}"
            );
        }
    }

    #[test]
    fn preconditions_compare_strong_tags() {
        let plan = |headers: &[(&str, &str)]| table(headers).plan(10, TAG);
        let resume = [("range", "bytes=4-"), ("if-range", TAG)];
        assert_eq!(plan(&resume), Plan::Partial { start: 4, end: 10 });
        // A different file sends the whole file instead of a part.
        for stale in ["\"9-9-9\"", "W/\"1-2-3\"", "Wed, 21 Oct 2015 07:28:00 GMT"] {
            assert_eq!(
                plan(&[("range", "bytes=4-"), ("if-range", stale)]),
                Plan::Full
            );
        }
        assert_eq!(plan(&[("if-match", TAG)]), Plan::Full);
        assert_eq!(plan(&[("if-match", "*")]), Plan::Full);
        assert_eq!(plan(&[("if-match", "\"a\", \"1-2-3\"")]), Plan::Full);
        for stale in ["\"9-9-9\"", "W/\"1-2-3\""] {
            assert_eq!(
                plan(&[("if-match", stale), ("range", "bytes=4-")]),
                Plan::PreconditionFailed
            );
        }
        // If-Match is checked before the range.
        assert_eq!(
            plan(&[("if-match", "\"9\""), ("range", "bytes=99-")]),
            Plan::PreconditionFailed
        );
    }
}
