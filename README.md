# Edelcrantz

This is a small crate for writing network services, with the following
particular features:

  - Asynchronous communication of serde-serializable types.
  - Symmetric / peer-to-peer: no distinguished clients or servers.
  - Pipelined: many requests allowed in flight at once.
  - Support for "one-way" requests with no paired responses.
  - Async-ecosystem agnostic: doesn't drag in async_std or tokio (except as
    dev-dependencies for testing).

There is no integrated event loop nor task spawning: you are expected to
call methods on this crate's main `Connection` type from your own tasks or
async functions.

## Name

Abraham Niclas Edelcrantz (1754-1821) developed the [Swedish optical
telegraph system](https://en.wikipedia.org/wiki/Optical_telegraph#Sweden),
which operated from 1795-1881.
