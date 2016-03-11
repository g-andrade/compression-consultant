# compression-consultant
Determine ideal zlib parameters for captured data streams (WIP)

Compare any combination of the following:
* stateless VS stateful
* different thresholds for triggering compression (0..infinity)
* different level values (0..9)
* different window_bits values (8..15)
* different mem_level values (1..9)
* different strategies (default, filtered, huffman_only, rle)
