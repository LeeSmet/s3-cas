use super::fs::BLOCK_SIZE;
use futures::{ready, Stream};
use s3_server::dto::ByteStream;
use std::{
    io, mem,
    pin::Pin,
    task::{Context, Poll},
};

pub struct BufferedByteStream {
    // In a perfect world this would be an AsyncRead type, as that will likely be more performant
    // than reading bytes, and copying them. However the AyndRead implemented on this type is the
    // tokio one, which is not the same as the futures one. And I don't feel like adding a tokio
    // dependency here right now for that.
    // TODO: benchmark both approaches
    bs: ByteStream,
    buffer: Vec<u8>,
    finished: bool,
}

impl BufferedByteStream {
    pub fn new(bs: ByteStream) -> Self {
        Self {
            bs,
            buffer: Vec::with_capacity(BLOCK_SIZE),
            finished: false,
        }
    }
}

impl Stream for BufferedByteStream {
    type Item = io::Result<Vec<Vec<u8>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            match ready!(Pin::new(&mut self.bs).poll_next(cx)) {
                None => {
                    self.finished = true;
                    if !self.buffer.is_empty() {
                        // since we won't be using the vec anymore, we can replace it with a 0 capacity
                        // vec. This wont' allocate.
                        return Poll::Ready(Some(Ok(vec![mem::replace(
                            &mut self.buffer,
                            Vec::with_capacity(0),
                        )])));
                    }
                    return Poll::Ready(None);
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                Some(Ok(bytes)) => {
                    let mut buf_remainder = self.buffer.capacity() - self.buffer.len();
                    if bytes.len() < buf_remainder {
                        self.buffer.extend_from_slice(&bytes);
                    } else if self.buffer.len() == buf_remainder {
                        self.buffer.extend_from_slice(&bytes);
                        return Poll::Ready(Some(Ok(vec![mem::replace(
                            &mut self.buffer,
                            Vec::with_capacity(BLOCK_SIZE),
                        )])));
                    } else {
                        let mut out = Vec::with_capacity(
                            (bytes.len() - buf_remainder) / self.buffer.capacity() + 1,
                        );
                        self.buffer.extend_from_slice(&bytes[..buf_remainder]);
                        out.push(mem::replace(
                            &mut self.buffer,
                            Vec::with_capacity(BLOCK_SIZE),
                        ));
                        // repurpose buf_remainder as pointer to start of data
                        while bytes[buf_remainder..].len() > BLOCK_SIZE {
                            out.push(Vec::from(&bytes[buf_remainder..buf_remainder + BLOCK_SIZE]));
                            buf_remainder += BLOCK_SIZE;
                        }
                        // place the remainder in our buf
                        self.buffer.extend_from_slice(&bytes[buf_remainder..]);
                        return Poll::Ready(Some(Ok(out)));
                    };
                }
            };
        }
    }
}
