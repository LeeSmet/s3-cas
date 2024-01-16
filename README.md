# S3-CAS

A simple POC implementation of the (basic) S3 API using content addresses storage. The current implementation
has been running in production for 1.5 years storing some 250M objects.

## Building

To build it yourself, clone the repo and then use the standard rust tools. To build the server, the
`binary` feature is needed. There is also an optional `refcount` feature which adds reference counting
to data blocks. If this feature is not enabled, data blocks will never be deleted (even if they) aren't
used anymore. The `vendored` feature can be used if a static binary is needed.

```
git clone https://github.com/leesmet/s3-cas
cd s3-cas
cargo build --release --features binary,refcount
```

## Known issues

- The metadata database (sled) has unbounded memory growth related to the objects stored. This means
  the server will eventually consume all memory on the host and crash. To fix this the metadata database
  should either be replaced with a new version of sled (still in development) or a different one entirely
- Only the basic API is implemented, and even then it is not entirely implemented (for instance copy
  between servers is not implemented).
- The codebase is very much POC and not following a good abstraction structure
- Single key only, no support to add multiple keys with different permissions.
