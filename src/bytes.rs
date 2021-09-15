use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;
use hyper::body::Bytes;
use pin_project_lite::pin_project;

pin_project! {
    pub struct BytesStream<R>{
        #[pin]
        reader: R,
        buf_size: usize,
    }
}

impl<R> BytesStream<R> {
    /// Constructs a `BytesStream`
    pub const fn new(reader: R, buf_size: usize) -> Self {
        Self { reader, buf_size }
    }
}

impl<R: futures::AsyncRead> Stream for BytesStream<R> {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // FIXME: reuse the buf
        let mut buf = vec![0_u8; self.buf_size];

        let this = self.project();

        let ret: io::Result<usize> = futures::ready!(this.reader.poll_read(cx, &mut buf));
        let ans: Option<io::Result<Bytes>> = match ret {
            Ok(n) => {
                if n == 0 {
                    None
                } else {
                    buf.truncate(n);
                    Some(Ok(buf.into()))
                }
            }
            Err(e) => Some(Err(e)),
        };

        Poll::Ready(ans)
    }
}
