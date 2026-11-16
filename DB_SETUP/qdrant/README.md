# Qdrant

| | |
|---|---|
| Version | `QDRANT_IMAGE` in `../db.env` (1.20.0) |
| Ports | **6333** REST, **6334** gRPC |
| Data | `qdrant/volumes/` |
| Web UI | `http://<server>:6333/dashboard` |

## Start and check

```bash
./db.sh start qdrant
./db.sh test qdrant      # prints PASS
```

## Good to know

- Use **gRPC** in the benchmark (`--prefer-grpc true`): REST is much slower.
- On start Qdrant loads all its collections, which takes a few minutes for large ones.

## Delete a collection

```bash
curl -s localhost:6333/collections                     # list
curl -s -X DELETE localhost:6333/collections/<name>    # delete (immediate, cannot be undone)
```

## Change version

Set `QDRANT_IMAGE` in `../db.env`, then `./db.sh wipe qdrant && ./db.sh start qdrant`.

## Build from source (optional)

```bash
git clone https://github.com/qdrant/qdrant.git ~/qdrant
./qdrant/build-from-source.sh image    # about 30 min; builds the image qdrant-local:<tag>-local
```

Put that image name in `QDRANT_IMAGE` and restart Qdrant.
