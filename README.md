# [One Billion Row Challenge](https://github.com/gunnarmorling/1brc)

## Results

On my machine (AMD Ryzen 7 5800X 8-Core; 32GB RAM) I achieve:

-   2s (parallel)
-   20s (non-parallel)

For reference:

-   Baseline: 2:10min
-   thomaswue (winning submission): 1.6s
-   jerrinot (winning submission for 32 threads): 1.42
-   artsiomkorzun (winning submission for 10k key set): 1.4s

## Setup

Clone the [1brc](https://github.com/gunnarmorling/1brc) and generate the data. Put it in `data/measurements.txt`.
Then build the binary: `cargo b -r`
Run with: `cargo r -r`

## Running

There are two function `get_map` and `get_map_par`.
For implementing improvements use `get_map`. This results in cleaner asm and easier debugging.
For the final evaluation use the parallel version.

## Helpers

Don't forget to rebuild before running helpers.

```sh
# Time a single run
time cargo r -r

# Get average time for multiple runs
hyperfine -w 3 ./target/release/onebrc-rust

# View the flamegraph
flamegraph -- ./target/release/onebrc-rust

# View raw asm. Note: You might have to set inline(never).
cargo asm --bin onebrc-rust extract_fields
```
