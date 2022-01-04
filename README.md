# Streaming SQL on Onyx

An experiment to implement a StreamingSQL -> Onyx Job compiler.

## Reference

[Blog post on various techniques and algorithms](https://highlyscalable.wordpress.com/2013/08/20/in-stream-big-data-processing/)

## Idea

StreamingSQL is just like SQL, except that relations are now in streaming mode, and some additional features are supported:
- Windowing and Aggregation
- Stream join instead of relational join
- Complex Event Processing (CEP)

## Some algorithms

- Symmetric hash join for stream join
- Sketches and probabilisitic data structure for efficiently updating aggregate stats over a sliding window
- Convert nondeterministic FSM into a deterministic one for CEP

## Code on Gitpod

Click the button below to start a new development environment:

[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/lemonteaa/onyx-streaming-sql)

