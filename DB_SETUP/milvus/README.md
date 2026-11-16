# Milvus

| | |
|---|---|
| Version | `MILVUS_IMAGE` in `../db.env` (3.0.5) |
| Port | **19530** (9091 = health check) |
| Containers | 3: Milvus, etcd (metadata), MinIO (storage) |
| Data | `milvus/volumes/` |

## Start and check

```bash
./db.sh start milvus
./db.sh test milvus      # prints PASS
```

## Good to know

- After a restart, load a collection again before searching (`client.load_collection("<name>")`).
  The benchmark does this itself.
- Extra Milvus settings go in `user.yaml` (empty = Milvus defaults, used for all our results). Restart after a change.

## Delete a collection

```bash
curl -s -X POST localhost:19530/v2/vectordb/collections/drop -H 'Content-Type: application/json' \
  -d '{"collectionName":"<name>"}'
```

Milvus removes the files about 4 hours later. To free the disk at once: `./db.sh wipe milvus` (deletes everything).

## Change version

Set `MILVUS_IMAGE` in `../db.env`, then `./db.sh wipe milvus && ./db.sh start milvus`.
If that Milvus release uses other etcd / MinIO versions, change `MILVUS_ETCD_IMAGE` / `MILVUS_MINIO_IMAGE` too.

## Build from source (optional)

```bash
git clone https://github.com/milvus-io/milvus.git ~/milvus
./milvus/build-from-source.sh       # about 2.5 h the first time; prints the image name at the end
```

Put that image name in `MILVUS_IMAGE` and restart Milvus. Run long builds inside `tmux`.

`docker-run/` holds Milvus' official single-container script; it is not used for the benchmarks.
