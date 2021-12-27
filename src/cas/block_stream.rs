use crate::metrics::SharedMetrics;

use super::range_request::RangeRequest;
use futures::{ready, AsyncRead, AsyncSeek, Future, Stream};
use hyper::body::Bytes;
use std::{
    io,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll},
};

/// Implementation of a single stream over potentially multiple on disk data block files.
pub struct BlockStream {
    paths: Vec<(PathBuf, usize)>,
    fp: usize, // pointer to current file path
    size: usize,
    metrics: SharedMetrics,
    processed: usize,
    has_seeked: bool,
    range: RangeRequest,
    file: Option<async_fs::File>, // current file to read
    open_fut: Option<Pin<Box<dyn Future<Output = io::Result<async_fs::File>> + Send>>>,
}

impl BlockStream {
    pub fn new(
        paths: Vec<(PathBuf, usize)>,
        size: usize,
        range: RangeRequest,
        metrics: SharedMetrics,
    ) -> Self {
        Self {
            paths,
            fp: 0,
            file: None,
            size,
            metrics,
            has_seeked: true,
            processed: 0,
            open_fut: None,
            range,
        }
    }
}

impl Stream for BlockStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (start, end) = match self.range {
            RangeRequest::Range(start, end) => (start, end),
            RangeRequest::ToBytes(end) => (0, end),
            RangeRequest::FromBytes(start) => (start, self.size as u64 + start),
            RangeRequest::All => (0, self.size as u64),
        };
        let processed = self.processed as u64;

        if processed > end {
            // we did all we need here, exit. This is here because we can't both return data in the
            // actual read, and indicate the stream is done
            return Poll::Ready(None);
        }

        // try to seek in the file to the correct offset
        // since we skip files we don't need to read from, this file always has at least _some_
        // bytes to read, and hence seek is always within bounds (even though it is technically not
        // an error if it isn't).
        if !self.has_seeked && start > processed {
            if let Some(ref mut file) = self.file {
                return match Pin::new(file)
                    .poll_seek(cx, io::SeekFrom::Current((start - processed) as i64))
                {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                    Poll::Ready(Ok(_)) => {
                        self.has_seeked = true;
                        // TODO: this can be `n`
                        self.processed += (start - processed) as usize;
                        self.poll_next(cx)
                    }
                };
            }
        }

        // if we have an open file, try to read it
        if let Some(ref mut file) = self.file {
            let mut cap = end - processed + 1;
            if cap > 4096 {
                cap = 4096;
            }
            let mut buf = vec![0; cap as usize];
            return match Pin::new(file).poll_read(cx, &mut buf) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Ready(Ok(0)) => {
                    self.file = None;
                    self.poll_next(cx)
                }
                Poll::Ready(Ok(n)) => {
                    self.processed += n;
                    buf.truncate(n);
                    self.metrics.bytes_sent(n);
                    Poll::Ready(Some(Ok(buf.into())))
                }
            };
        }

        // check if we even need bytes from the next files
        // make sure to only do this when we are not already polling. The issue is that opening a
        // file advanced fp even before the file is opened, which might cause an issue in the
        // calculation of a range request, if the new file is so small that it would be skipped.
        if self.open_fut.is_none() {
            loop {
                // TODO: Fix this crap
                let processed = self.processed as u64;
                match self.range {
                    RangeRequest::Range(start, end) => {
                        if processed > end {
                            return Poll::Ready(None);
                        } else if processed < start {
                            if processed + (self.paths[self.fp].1 as u64) < start {
                                // skip file entirely
                                self.processed += self.paths[self.fp].1;
                                self.fp += 1;
                                if self.fp > self.paths.len() {
                                    return Poll::Ready(None);
                                }
                                continue;
                            }
                            break;
                        } else {
                            break;
                        }
                    }
                    RangeRequest::ToBytes(end) => {
                        if processed > end {
                            return Poll::Ready(None);
                        }
                        break;
                    }
                    RangeRequest::FromBytes(start) => {
                        if processed < start && processed + (self.paths[self.fp].1 as u64) < start {
                            // skip file entirely
                            self.processed += self.paths[self.fp].1;
                            self.fp += 1;
                            if self.fp > self.paths.len() {
                                return Poll::Ready(None);
                            }
                            continue;
                        }
                        break;
                    }
                    RangeRequest::All => break,
                }
            }
        }

        // we don't have an open file, check if we have any more left
        if self.fp > self.paths.len() {
            return Poll::Ready(None);
        }

        // try to open the next file
        // if we are not opening one already start doing so
        if self.open_fut.is_none() {
            self.open_fut = Some(Box::pin(async_fs::File::open(
                self.paths[self.fp].0.clone(),
            )));
            // increment the file pointer for the next file
            self.fp += 1;
        };

        // this will always happen
        if let Some(ref mut open_fut) = self.open_fut {
            let file_res = ready!(open_fut.as_mut().poll(cx));
            // we opened a file, or there is an error
            // clear the open fut as it is done
            self.open_fut = None;
            match file_res {
                // if there is an error, we just return that. The next poll call will try to open the
                // next file
                Err(e) => return Poll::Ready(Some(Err(e))),
                // if we do have an open file, set it as open file, and immediately poll again to try
                // and read from it
                Ok(file) => {
                    self.file = Some(file);
                    self.has_seeked = false;
                    return self.poll_next(cx);
                }
            };
        };

        unreachable!();
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.size, Some(self.size))
    }
}
