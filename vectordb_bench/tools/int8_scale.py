"""Measure the int8 quantization scale for a dataset.

int8 cells hold whole numbers in [-128, 127], so a float vector has to be scaled
onto that grid. The usual ``v * 127`` assumes components already lie in [-1, 1],
which holds only for unit-normalized vectors. The benchmark datasets are not
normalized -- Cohere 1M reaches 5.5046 -- so that scale overflows and wraps,
flipping the sign of the largest components and corrupting every vector.

Scale by the corpus maximum instead, so the largest component lands exactly on
127. Run this once per dataset and pass the result to `--int8-max-abs`.

Pass every train shard -- the maximum has to cover the whole corpus, so a single
shard of a split dataset is not enough:

    python -m vectordb_bench.tools.int8_scale <dir>/cohere_medium_1m/shuffle_train.parquet
    python -m vectordb_bench.tools.int8_scale <dir>/cohere_large_10m/shuffle_train-*.parquet
"""

import argparse
import pathlib

import numpy as np
import pyarrow.parquet as pq

BATCH_SIZE = 50_000


def max_abs_component(path: pathlib.Path, column: str = "emb") -> tuple[float, int]:
    """Return the largest absolute component in the file, and the rows scanned."""
    largest = 0.0
    rows = 0
    for batch in pq.ParquetFile(path).iter_batches(batch_size=BATCH_SIZE, columns=[column]):
        block = np.stack(batch.column(column).to_pandas().to_numpy()).astype(np.float32)
        largest = max(largest, float(np.abs(block).max()))
        rows += len(block)
    return largest, rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "parquet",
        type=pathlib.Path,
        nargs="+",
        help="train parquet shard(s) holding the corpus vectors; pass all shards of a split dataset",
    )
    parser.add_argument("--column", default="emb", help="vector column name (default: emb)")
    args = parser.parse_args()

    largest = 0.0
    rows = 0
    for shard in args.parquet:
        shard_max, shard_rows = max_abs_component(shard, args.column)
        if len(args.parquet) > 1:
            print(f"  {shard.name}: {shard_rows:,} rows, max {shard_max:.4f}")
        largest = max(largest, shard_max)
        rows += shard_rows

    if largest <= 0:
        msg = f"no non-zero components found in {', '.join(str(p) for p in args.parquet)}"
        raise SystemExit(msg)

    print(f"shards scanned    : {len(args.parquet)}")
    print(f"rows scanned      : {rows:,}")
    print(f"max |component|   : {largest:.4f}")
    print(f"int8 scale        : 127 / {largest:.4f} = {127.0 / largest:.4f}")
    print(f"overflow if v*127 : {largest * 127:.1f} vs int8 ceiling of 127")
    print()
    print(f"pass to the benchmark as:  --int8-max-abs {largest:.4f}")


if __name__ == "__main__":
    main()
