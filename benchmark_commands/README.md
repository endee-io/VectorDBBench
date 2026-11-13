# Benchmark Commands

Plain scripts: **every file is one complete `vectordbbench` command**.
Open a file, change what you need (collection name, k, concurrency, filter value ...) and run it with `./file.sh`.
All values are the ones of our final 1M runs (Cohere 1M, 768 dim, cosine, top_k 30, 16 concurrent for 30 s).

## Dense - `dense/<db>/<config>.sh`

One file per database and vector type / precision / quantization.

- **Running a file** = the LOAD command: delete the old collection, load 1M vectors, then search once.
- **To search again without loading**, use the "SEARCH ONLY" command commented at the bottom of the file.
  Remove the `#` at the start of its lines and put a `#` in front of the load command.

| Folder | Configs |
|---|---|
| `dense/endee/` | int16, int8, int8e, float16, float32, binary |
| `dense/milvus/` | hnsw_fp32, hnsw_vecfield_fp16 / bf16 / int8, hnsw_sq8_refine_fp32, hnsw_sq8_norefine, hnsw_sq6_refine_fp32, hnsw_fp16_refine_fp32, hnsw_bf16_refine_fp32, hnsw_sq4u_refine_fp32 (+ _k4), hnsw_pq384_refine_fp32 (+ _k4), ivf_flat, ivf_sq8, ivf_rabitq_refine_q8 |
| `dense/qdrant/` | fp32, datatype_float16 / turbo16 / turbo8 / turbo4, scalar_int8, turbo_8bit / 4bit / 2bit / 1_5bit / 1bit (+ oversample3), binary_2bit / 1_5bit / 1bit (+ oversample3), product_x16 / x4 |
| `dense/vespa/` | float, bfloat16, int8, binary |
| `dense/redis/` | float32, float16, bfloat16 |

**Milvus `_k4` files** are search only: refine_k 4 is a search setting. Load the matching config without `_k4` first.

## Filter - `filter/<db>/`

Each database uses its best data type: Endee int16, Milvus bfloat16, Qdrant turbo16, Vespa bfloat16, Redis float16.
Run the files in order:

| File | What it does |
|---|---|
| `1_load.sh` | load once (vectors + id + label); this collection serves everything below |
| `2_dense.sh` | plain dense search, no filter |
| `3_label_filter.sh` | label filter: `--label-percentage` = share of vectors that **match** (0.01 = 1 %) |
| `4_int_filter.sh` | int filter: `--filter-rate` = share of vectors **removed** (0.99 = only 1 % searched) |
| `5_opt_*.sh` ... | the optimisations of that database (search only) |

To test another filter value, change the number in `--label-percentage` or `--filter-rate`. Each file lists the values the Endee team runs:

- **label:** 0.5 0.2 0.1 0.05 0.02 0.01 0.002 0.001
- **int:** 0.01 0.5 0.8 0.99

**Optimisations:**

| Database | Files |
|---|---|
| Endee | prefilter_label (100000), prefilter_int (50000), boost (25/50/75/100), prefilter_boost |
| Qdrant | acorn |
| Milvus | iterative_filter |
| Vespa | tuned |
| Redis | adhoc_bf |

## Notes

- Qdrant files use `--prefer-grpc true` (gRPC). `false` = REST, which is slow at top_k 30 because of the client.
- Results are written to `vectordb_bench/results/<Database>/` as usual. The `--task-label` decides the file name, so give each run its own label to avoid overwriting.
- The files contain our server IP, the Endee token and the Redis password. Change them for another setup.
