use sluice::pipe::{pipe,PipeReader,PipeWriter};
use duplexify::Duplex;
use async_std::task::{block_on};
use serde::{Serialize,Deserialize};
use futures::{pin_mut,select};
use log::trace;

use crate::*;

#[derive(Serialize,Deserialize)]
struct Request(String);
#[derive(Serialize,Deserialize)]
struct Response(String);
#[derive(Serialize,Deserialize)]
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

#[test]
fn one_way() {

    let _ = pretty_env_logger::try_init();

    let (a_end, b_end) = duplex_pair();
    let mut peer_a = Connection::<OneWay, Request, Response>::new(a_end);
    let mut peer_b = Connection::<OneWay, Request, Response>::new(b_end);

    peer_a.enqueue_oneway(OneWay("hello".into())).unwrap();
    block_on(async move {   
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

    let _ = pretty_env_logger::try_init();

    let (a_end, b_end) = duplex_pair();
    let mut peer_a = Connection::<OneWay, Request, Response>::new(a_end);
    let mut peer_b = Connection::<OneWay, Request, Response>::new(b_end);

    let res_fut = peer_a.enqueue_request(Request("hello".into())).fuse();
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
