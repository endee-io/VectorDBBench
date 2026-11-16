# Backup and restore

Use this to reuse a loaded collection on another server instead of loading it again.
Both servers need the **same database versions** (`db.env`).

| Database | How | Index kept? |
|---|---|---|
| Endee | backup API | yes |
| Qdrant | snapshot API | yes |
| Milvus | copy `milvus/volumes/` (or the `milvus-backup` tool) | yes |
| Redis | copy `dump.rdb` | no, Redis re-indexes on start |
| Vespa | copy the `vespa_vespa-var` volume | yes |

## Copy a whole database (Milvus, Qdrant, Redis, Vespa)

On the old server:

```bash
docker exec redis redis-cli SAVE                       # Redis only: write the latest data to disk
./db.sh stop <db>
sudo tar czf <db>.tgz <db>/volumes                     # Milvus, Qdrant, Redis
docker run --rm -v vespa_vespa-var:/v -v "$PWD":/b alpine tar czf /b/vespa.tgz -C /v .   # Vespa
```

Copy the `.tgz` to the new server (same `DB_SETUP` folder and versions), then:

```bash
sudo tar xzf <db>.tgz                                  # Milvus, Qdrant, Redis
docker volume create vespa_vespa-var && \
  docker run --rm -v vespa_vespa-var:/v -v "$PWD":/b alpine tar xzf /b/vespa.tgz -C /v   # Vespa
./db.sh start <db>
```

## Qdrant: one collection

```bash
curl -X POST "http://<old>:6333/collections/<coll>/snapshots?wait=true"            # create, prints the snapshot name
curl -o <snap> http://<old>:6333/collections/<coll>/snapshots/<snap>               # download
curl -X POST "http://<new>:6333/collections/<coll>/snapshots/upload?priority=snapshot&wait=true" -F "snapshot=@<snap>"
```

## Milvus: one collection

Use the official [milvus-backup](https://github.com/zilliztech/milvus-backup) tool:
`milvus-backup create -n <backup>`, then `milvus-backup restore -n <backup>`. Test it once before relying on it.

## Endee: one collection

```bash
A='Authorization: root/bench:1234'; J='Content-Type: application/json'
curl -X POST http://<old>:8080/api/v2/collection/<coll>/backup -H "$A" -H "$J" -d '{"name":"<bk>"}'    # create
curl -o <bk>.tar http://<old>:8080/api/v2/backup/<bk>/download -H "$A"                                # download
curl -X POST "http://<new>:8080/api/v2/backup/upload?name=<bk>" -H "$A" \
     -H 'Content-Type: application/x-tar' --data-binary @<bk>.tar                                       # upload
curl -X POST http://<new>:8080/api/v2/backup/<bk>/restore -H "$A" -H "$J" -d '{"target_collection_name":"<coll>"}'
```

Progress: `GET /api/v2/status/backup` and `GET /api/v2/status/restore`.
