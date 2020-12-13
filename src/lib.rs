//! This is a small crate for writing network services, with the following
//! particular features:
//!
//!   - Asynchronous communication of serde-serializable types.
//!   - Symmetric / peer-to-peer: no distinguished clients or servers.
//!   - Pipelined: many requests allowed in flight at once.
//!   - Support for "one-way" requests with no paired responses.
//!   - Async-ecosystem agnostic: doesn't drag in async_std or tokio (except as
//!     dev-dependencies for testing).
//!
//! There is no integrated event loop nor task spawning: you are expected to
//! call methods on this crate's main [Connection] type from your own tasks or
//! async functions.
//!
//! ## Usage
//!
//! This crate expects callers to take three main steps:
//!
//!   - Enqueueing a message to send, either a one-way message via
//!     [Connection::enqueue_oneway] which generates no corresponding future, or
//!     via [Connection::enqueue_request] which generates a future that will be
//!     filled in when a paired response arrives.
//!   - Calling [Connection::advance] and awaiting its returned future
//!     (typically in a loop) to advance the peer through internal steps of
//!     dequeueing, sending, receiveing, serving, responding, and fulfilling
//!     response futures. This requires the caller to provide callbacks.
//!   - Optionally awaiting the response future generated in the first step.
//!
//! Sequencing these steps and integrating them into a set of async tasks or
//! event loops is left to the caller. Some examples are present in the test
//! module.
//!
//! # Name
//!
//! Abraham Niclas Edelcrantz (1754-1821) developed the [Swedish optical
//! telegraph system](https://en.wikipedia.org/wiki/Optical_telegraph#Sweden),
//! which operated from 1795-1881.

// TODO: write tests that do multiple rounds of req/res and bi-directional req/res.

#![recursion_limit = "512"]
use future::FusedFuture;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::channel::oneshot::{channel, Sender};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use futures::lock::Mutex;
use futures::select;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::{self, Future};
use futures::{
    future,
    future::{BoxFuture, FutureExt, TryFutureExt},
};
use log::trace;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_cbor::{de::from_slice, ser::to_vec_packed};
use std::{collections::HashMap, pin::Pin, sync::Arc};
use thiserror::Error;

#[cfg(test)]
mod test;

/// Any IO facility that can be sent or received _on_ must implement this trait.
pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}

/// Any message that can be sent or received -- as a request, response, or
/// one-way -- must implement this trait.
pub trait Msg: Serialize + DeserializeOwned + Send + Sync + 'static {}
impl<T> Msg for T where T: Serialize + DeserializeOwned + Send + Sync + 'static {}

#[derive(Error, Debug)]
pub enum Error {
    #[error("queue management error")]
    Queue,

    #[error("unknown response {0}")]
    UnknownResponse(u64),

    #[error("response channel {0} dropped")]
    ResponseChannelDropped(u64),

    #[error(transparent)]
    Io(#[from] futures::io::Error),

    #[error(transparent)]
    Cbor(#[from] serde_cbor::Error),

    #[error(transparent)]
    Canceled(#[from] futures::channel::oneshot::Canceled),
}

struct EnvelopeWriter {
    wr: Box<dyn AsyncWrite + Unpin + Send + Sync + 'static>,
}

impl EnvelopeWriter {
    fn new<W: AsyncWrite + Unpin + Send + Sync + 'static>(wtr: W) -> Self {
        EnvelopeWriter { wr: Box::new(wtr) }
    }

    /// Send a length-prefixed envelope.
    async fn send<OneWay: Msg, Request: Msg, Response: Msg>(
        &mut self,
        e: Envelope<OneWay, Request, Response>,
    ) -> Result<(), Error> {
        use byteorder_async::{LittleEndian, WriterToByteOrder};
        let bytes = to_vec_packed(&e)?;
        let wsz: u64 = bytes.len() as u64;
        trace!("sending {}-byte envelope at IO level", wsz);
        self.wr.byte_order().write_u64::<LittleEndian>(wsz).await?;
        self.wr.write_all(bytes.as_slice()).await?;
        trace!("sent {}-byte envelope at IO level", wsz);
        Ok(())
    }
}

struct EnvelopeReader {
    rd: Box<dyn AsyncRead + Unpin + Send + Sync + 'static>,
    rdbuf: Vec<u8>,
}

impl EnvelopeReader {
    fn new<R: AsyncRead + Unpin + Send + Sync + 'static>(rdr: R) -> Self {
        EnvelopeReader {
            rd: Box::new(rdr),
            rdbuf: Vec::new(),
        }
    }

    /// Receive a length-prefixed envelope.
    async fn recv<OneWay: Msg, Request: Msg, Response: Msg>(
        &mut self,
    ) -> Result<Envelope<OneWay, Request, Response>, Error> {
        trace!("receiving envelope at IO level");
        use byteorder_async::{LittleEndian, ReaderToByteOrder};
        let rsz: u64 = self.rd.byte_order().read_u64::<LittleEndian>().await?;
        self.rdbuf.resize(rsz as usize, 0);
        self.rd.read_exact(self.rdbuf.as_mut_slice()).await?;
        trace!("received {}-byte envelope at IO level", rsz);
        Ok(from_slice(self.rdbuf.as_slice())?)
    }
}

// Each `Connection` has a `Reception` subobject that's shared in an
// `Arc<Mutex<Reception>>` between it and any number of `Queue`s that may have
// been cloned off from the `Connection`. It contains the necessary state to
// enqueue new requests and stores the `Sender`-ends of requests in flight so that
// the `Connection` can complete them when it receives a response.
//
// This is an implementation detail; users should use `Queue`, which is friendlier.
struct Reception<OneWay: Msg, Request: Msg, Response: Msg> {
    /// The next request number this peer is going to send.
    next_request: u64,

    /// Requests this peer has sent to the other peer, that this peer has handed
    /// out Recever<Res> futures to in [Connection::enqueue_request], that it needs to
    /// fill in when it receives a response.
    requests: HashMap<u64, Sender<Response>>,

    /// The sending side of a waitable queue for outgoing envelopes.
    enqueue: UnboundedSender<Envelope<OneWay, Request, Response>>,
}

/// A `Queue` is a shared handle that can be cloned off of a `Connection` and
/// used to enqueue messages even while the `Connection` is borrowed and/or
/// locked in a call to `advance` (eg. by a task service loop).
pub struct Queue<OneWay: Msg, Request: Msg, Response: Msg> {
    reception: Arc<Mutex<Reception<OneWay, Request, Response>>>,
}

impl<OneWay: Msg, Request: Msg, Response: Msg> Clone for Queue<OneWay, Request, Response> {
    fn clone(&self) -> Self {
        Self {
            reception: self.reception.clone(),
        }
    }
}

impl<OneWay: Msg, Request: Msg, Response: Msg> Queue<OneWay, Request, Response> {
    fn new(reception: Reception<OneWay, Request, Response>) -> Self {
        Self {
            reception: Arc::new(Mutex::new(reception)),
        }
    }

    /// Enqueue a OneWay message for sending.
    pub fn enqueue_oneway(
        &self,
        oneway: OneWay,
    ) -> impl Future<Output = Result<(), Error>> + 'static {
        let reception = self.reception.clone();
        async move {
            let env = Envelope::<OneWay, Request, Response>::OneWay(oneway);
            let guard = reception.lock().await;
            guard.enqueue.unbounded_send(env).map_err(|_| Error::Queue)
        }
    }

    /// Enqueue a Request message for sending, and return a future that will be
    /// filled in when the response arrives.
    pub fn enqueue_request(
        &self,
        req: Request,
    ) -> impl Future<Output = Result<Response, Error>> + 'static {
        let reception = self.reception.clone();
        async move {
            let (send_err, recv) = {
                let mut guard = reception.lock().await;
                let curr = guard.next_request;
                let env = Envelope::<OneWay, Request, Response>::Request(curr, req);
                let send_err = guard.enqueue.unbounded_send(env);
                let (send, recv) = channel();
                if send_err.is_ok() {
                    trace!("enqueued envelope for request {}", curr);
                    guard.next_request += 1;
                    guard.requests.insert(curr, send);
                }
                // Now release the reception mutex guard and let the send-error and recv-future escape.
                (send_err, recv)
            };
            if send_err.is_ok() {
                Ok(recv.await?)
            } else {
                Err(futures::future::ready(Error::Queue).await)
            }
        }
    }
}

type PendingWrite = Pin<Box<dyn FusedFuture<Output = Result<(), Error>> + Send + Sync + 'static>>;
type PendingRead<OneWay, Request, Response> = Pin<
    Box<
        dyn FusedFuture<Output = Result<Envelope<OneWay, Request, Response>, Error>>
            + Send
            + Sync
            + 'static,
    >,
>;
pub struct Connection<OneWay: Msg, Request: Msg, Response: Msg> {
    /// Split-off read half (plus read buffer) of the AsyncRead + AsyncWrite the
    /// connection owns, held in an Arc<Mutex<>> so it can be captured into
    /// read_in_progress future below without capturing self.
    reader: Arc<Mutex<EnvelopeReader>>,

    /// Split-off write half of the AsyncRead + AsyncWrite the connection owns,
    /// held in an Arc<Mutex<>> so it can be captured into write_in_progress
    /// future below without capturing self.
    writer: Arc<Mutex<EnvelopeWriter>>,

    /// A selectable future for a current IO read-in-progress on reader.
    /// If this is Some(x) then reader is locked.
    read_in_progress: Option<PendingRead<OneWay, Request, Response>>,

    /// A selectable future for a current IO write-in-progress on writer.
    /// If this is Some(x) then writer is locked.
    write_in_progress: Option<PendingWrite>,

    /// A queue that can be cloned out of the Connection and used to submit
    /// work without having exclusive ownership of the Connection.
    pub queue: Queue<OneWay, Request, Response>,

    /// The receiving side of a waitable queue for outgoing envelopes.
    dequeue: UnboundedReceiver<Envelope<OneWay, Request, Response>>,

    /// Futures being fulfilled by requests being served by this peer.
    responses: FuturesUnordered<BoxFuture<'static, (u64, Response)>>,
}

#[serde(bound = "")]
#[derive(Serialize, Deserialize)]
enum Envelope<OneWay: Msg, Request: Msg, Response: Msg> {
    OneWay(OneWay),
    Request(u64, Request),
    Response(u64, Response),
}

/// A connection encapsulates logic for sending and receiving a particular
/// vocabulary of messages: one-way messages, requests, and responses. The
/// message types may be different or all the same, and may have internal
/// structure or be enums that have further meaning to the caller: all the
/// connection knows is that messages of the request type will be responded-to
/// by messages of the response type, and messages of the one-way type will not
/// be responded to.
impl<OneWay: Msg, Request: Msg, Response: Msg> Connection<OneWay, Request, Response> {
    pub fn new<RW: AsyncReadWrite>(rw: RW) -> Self {
        let (rdr, wtr) = rw.split();
        let reader = Arc::new(Mutex::new(EnvelopeReader::new(rdr)));
        let writer = Arc::new(Mutex::new(EnvelopeWriter::new(wtr)));
        let next_request = 0;
        let requests = HashMap::new();
        let responses = FuturesUnordered::new();
        let (enqueue, dequeue) = unbounded();
        let queue = Queue::new(Reception {
            next_request,
            requests,
            enqueue,
        });

        let read_in_progress = None;
        let write_in_progress = None;
        Connection {
            reader,
            writer,
            queue,
            read_in_progress,
            write_in_progress,
            responses,
            dequeue,
        }
    }

    /// Just calls `self.queue.enqueue_oneway`.
    pub fn enqueue_oneway(
        &self,
        oneway: OneWay,
    ) -> impl Future<Output = Result<(), Error>> + 'static {
        self.queue.enqueue_oneway(oneway)
    }

    /// Just calls `self.queue.enqueue_request`.
    pub fn enqueue_request(
        &self,
        req: Request,
    ) -> impl Future<Output = Result<Response, Error>> + 'static {
        self.queue.enqueue_request(req)
    }

    /// Take the next available step on this connection. Either:
    ///
    ///   - Sending an enqueued envelope.
    ///   - Resolving and enqueueing the output of a request's service routine
    ///     future.
    ///   - Receiving an envelope and transferring it to either a service
    ///     routine or a response future created by [Connection::enqueue_request].
    ///
    /// Callers should supply a `srv_req` function to service request envelopes
    /// by issuing futures, and a `srv_ow` function to service one-way
    /// envelopes.
    pub async fn advance<ServeRequest, FutureResponse, ServeOneWay>(
        &mut self,
        srv_req: ServeRequest,
        srv_ow: ServeOneWay,
    ) -> Result<(), Error>
    where
        ServeRequest: FnOnce(Request) -> FutureResponse,
        FutureResponse: Future<Output = Response> + Send + 'static,
        ServeOneWay: FnOnce(OneWay) -> (),
    {
        let rdr = self.reader.clone();
        let wtr = self.writer.clone();
        let mut rip = self.read_in_progress.take().unwrap_or_else(|| {
            Box::pin(
                async move { rdr.lock().await.recv::<OneWay, Request, Response>().await }.fuse(),
            )
        });
        let mut wip = self
            .write_in_progress
            .take()
            .unwrap_or_else(|| Box::pin(future::pending()));
        select! {
            res = wip => {
                // Put back rip
                self.read_in_progress = Some(rip);
                res
            },
            next_enqueued = self.dequeue.next() => match next_enqueued {
                None => {
                    // Put back rip and wip
                    self.read_in_progress = Some(rip);
                    self.write_in_progress = Some(wip);
                    Ok(())
                }
                Some(env) => {
                    // Put back rip
                    self.read_in_progress = Some(rip);
                    trace!("dequeued envelope, sending");
                    let write_fut = Box::pin(async move { wtr.lock().await.send(env).await }.fuse().and_then(move |_| wip));
                    self.write_in_progress = Some(write_fut);
                    Ok(())
                }
            },
            next_response = self.responses.next() => {
                // Put back rip and wip
                self.read_in_progress = Some(rip);
                self.write_in_progress = Some(wip);
                match next_response {
                    None => Ok(()),
                    Some((n, response)) => {
                        let env = Envelope::Response(n, response);
                        trace!("finished serving request {}, enqueueing response", n);
                        self.queue.reception.lock().await.enqueue.unbounded_send(env).map_err(|_| Error::Queue)
                    }
                }
            },
            read_result = rip => {
                // Put back wip
                self.write_in_progress = Some(wip);
                let env = read_result?;
                match env {
                    Envelope::OneWay(ow) => {
                        trace!("received one-way envelope, calling service function");
                        Ok(srv_ow(ow))
                    },
                    Envelope::Request(n, req) => {
                        trace!("received request envelope {}, calling service function", n);
                        let res_fut = srv_req(req);
                        let boxed : BoxFuture<'static,_> = Box::pin(res_fut.map(move |r| (n, r)));
                        Ok(self.responses.push(boxed))
                    },
                    Envelope::Response(n, res) => {
                        trace!("received response envelope {}, transferring to future", n);
                        match self.queue.reception.lock().await.requests.remove(&n.clone()) {
                            None => Err(Error::UnknownResponse(n)),
                            Some(send) => {
                                match send.send(res) {
                                    Ok(_) => Ok(()),
                                    Err(_) => Err(Error::ResponseChannelDropped(n))
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
