# compression-consultant
Determine ideal zlib parameters for data streams based on captured examples (WIP)

Compare any combination of the following:
* stateless VS stateful
* different thresholds for triggering compression (0..infinity)
* different level values (0..9)
* different window_bits values (8..15)
* different mem_level values (1..9)
* different strategies (default, filtered, huffman_only, rle)

It receives 2 or more arguments:
* Number of runs (1 or more; it should minimise error on time measurements)
* Individual filenames with stream data messages encoded as unwrapped base64, one message per line

```shell
escript run_benchmarks.escript 10 session01.log session02.log
```
