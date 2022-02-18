# 🐔 fustercluck

This is a small framework for implementing applications built on top of Node.js's `cluster` functionality. Applications are broken into a single `primary` process and one or more `worker` processes. `primary` and `worker` processes talk to each other using message passing. You define a set of serializable messages they can pass back and forth to communicate.

## Examples

There are a couple of examples in [`src/examples`](./src/examples).
