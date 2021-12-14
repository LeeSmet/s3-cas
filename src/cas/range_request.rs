/// Requested bytes from a file.
#[derive(Debug)]
pub enum RangeRequest {
    /// All bytes, i.e. full file.
    All,
    /// A range of bytes from the file. The range is inclusive.
    Range(u64, u64),
    /// All bytes until the given position. This is equivalent to Range(0, value).
    ToBytes(u64),
    /// All bytes from a given position until the end of the file. This is equivalent to
    /// Range(value, EOF).
    FromBytes(u64),
}

impl RangeRequest {
    pub fn size(&self, file_size: u64) -> u64 {
        let (start, end) = match self {
            RangeRequest::All => (0, file_size - 1),
            RangeRequest::ToBytes(end) => (0, *end),
            RangeRequest::FromBytes(start) => (*start, file_size - 1),
            RangeRequest::Range(start, end) => (*start, *end),
        };
        return end - start + 1;
    }
}

// TODO: replace with a parse impl on RangeRequest
/// Parse a range request.
pub fn parse_range_request(input: &Option<String>) -> RangeRequest {
    if let Some(ref input) = input {
        if !input.starts_with("bytes=") {
            eprintln!("Invalid range input \"{}\"", input);
            return RangeRequest::All;
        }
        let (_, input) = input.split_at(6); // split of "bytes="
        let mut parts = input.split('-');
        let first = parts.next();
        let second = parts.next();
        if first.is_none() || second.is_none() {
            eprintln!("invalid range request structure {}", input);
            return RangeRequest::All;
        }
        let first = first.unwrap();
        let second = second.unwrap();
        if parts.next().is_some() {
            eprintln!("invalid range request structure {}", input);
            return RangeRequest::All;
        }
        if first == "" && second == "" {
            eprintln!("invalid range request - missing start AND end {}", input);
            return RangeRequest::All;
        }
        if first == "" {
            match second.parse() {
                Ok(end) => RangeRequest::ToBytes(end),
                Err(e) => {
                    eprintln!(
                        "invalid range request - could not parse end ({}): {}",
                        e, input
                    );
                    RangeRequest::All
                }
            }
        } else if second == "" {
            match first.parse() {
                Ok(start) => RangeRequest::FromBytes(start),
                Err(e) => {
                    eprintln!(
                        "invalid range request - could not parse start ({}): {}",
                        e, input
                    );
                    RangeRequest::All
                }
            }
        } else {
            let start = match first.parse() {
                Ok(start) => start,
                Err(e) => {
                    eprintln!(
                        "invalid range request - could not parse start from string ({}): {}",
                        e, input
                    );
                    return RangeRequest::All;
                }
            };
            let end = match second.parse() {
                Ok(end) => end,
                Err(e) => {
                    eprintln!(
                        "invalid range request - could not parse end from string ({}): {}",
                        e, input
                    );
                    return RangeRequest::All;
                }
            };
            if end < start {
                eprintln!("invalid range request - start bigger than end",);
                return RangeRequest::All;
            }
            RangeRequest::Range(start, end)
        }
    } else {
        RangeRequest::All
    }
}
