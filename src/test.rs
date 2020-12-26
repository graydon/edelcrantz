use async_std::task::{self, block_on};
use duplexify::Duplex;
use futures::{pin_mut, select};
use serde::{Deserialize, Serialize};
use sluice::pipe::{pipe, PipeReader, PipeWriter};
use tracing::trace;
use tracing_subscriber;

use crate::*;

#[derive(Debug, Serialize, Deserialize)]
struct Request(String);
#[derive(Debug, Serialize, Deserialize)]
struct Response(String);
#[derive(Debug, Serialize, Deserialize)]
struct OneWay(String);

async fn serve_req(req: Request) -> Response {
    trace!("serving request: {:?}", req.0);
    Response(req.0.clone() + req.0.as_str())
}

fn serve_oneway(ow: OneWay) {
    trace!("serving one-way: {:?}", ow.0);
}

type PipeRw = Duplex<PipeReader, PipeWriter>;
fn duplex_pair() -> (PipeRw, PipeRw) {
    let (a_recv, b_send) = pipe();
    let (b_recv, a_send) = pipe();
    let a_end = Duplex::new(a_recv, a_send);
    let b_end = Duplex::new(b_recv, b_send);
    (a_end, b_end)
}

fn setup_tracing() {
    let _ = tracing_subscriber::fmt::try_init();
}

#[test]
fn one_way() {
    setup_tracing();
    let (a_end, b_end) = duplex_pair();
    let mut peer_a = Connection::<OneWay, Request, Response>::new(a_end);
    let mut peer_b = Connection::<OneWay, Request, Response>::new(b_end);

    block_on(async move {
        peer_a.enqueue_oneway(OneWay("hello".into())).await.unwrap();
        loop {
            let mut done = false;
            select! {
                r = peer_a.advance(serve_req, serve_oneway).fuse() => r.unwrap(),
                r = peer_b.advance(serve_req, |ow| { serve_oneway(ow); done = true } ).fuse() => {
                    r.unwrap();
                    if done {
                        break;
                    }
                }
            }
        }
    });
}

#[test]
fn req_res() {
    setup_tracing();
    let (a_end, b_end) = duplex_pair();
    let mut peer_a = Connection::<OneWay, Request, Response>::new(a_end);
    let mut peer_b = Connection::<OneWay, Request, Response>::new(b_end);

    let q = peer_a.queue.clone();
    let res_fut = q.enqueue_request(Request("hello".into())).fuse();
    pin_mut!(res_fut);
    block_on(async move {
        loop {
            select! {
                r = peer_a.advance(serve_req, serve_oneway).fuse() => r.unwrap(),
                r = peer_b.advance(serve_req, serve_oneway).fuse() => r.unwrap(),
                r = res_fut => {
                    let Response(b) = r.unwrap();
                    trace!("got response: {:?}", b);
                    break;
                }
            }
        }
    });
}

#[test]
fn december_deadlock() {
    // This test reproduced a deadlock we found in version 0.4.3 and before,
    // when two copies of edelcrantz were talking to one another: if the buffer
    // space between them was inadequate it could be the case that each would
    // await the other finishing a write in order to switch to reading.
    //
    // As of 0.5 we've redesigned the guts a bit and this is no longer possible;
    // the select loop sees pending IO-level reads and writes simultaneously.
    setup_tracing();
    let (a_end, b_end) = duplex_pair();
    let mut peer_a = Connection::<OneWay, Request, Response>::new(a_end);
    let mut peer_b = Connection::<OneWay, Request, Response>::new(b_end);

    let q = peer_a.queue.clone();
    let (send, recv) = channel();

    task::spawn(async move {
        let mut fu = FuturesUnordered::new();
        for i in 1..100 {
            fu.push(q.enqueue_request(Request(format!("hello {}", i).into())));
        }
        loop {
            select! {
                res = fu.next() => match res {
                    Some(res) => {
                        let Response(b) = res.unwrap();
                        trace!("got response: {:?}", b);
                    }
                    None => {
                        break;
                    }
                },
                r = peer_a.advance(serve_req, serve_oneway).fuse() => r.unwrap()
            }
        }
        trace!("sending shutdown signal");
        send.send(()).unwrap();
    });

    block_on(async move {
        let mut recv = recv.fuse();
        loop {
            select! {
                _ = peer_b.advance(serve_req, serve_oneway).fuse() => (),
                _ = recv => {
                    trace!("got shutdown signal");
                    break;
                }
            }
        }
    });
}
