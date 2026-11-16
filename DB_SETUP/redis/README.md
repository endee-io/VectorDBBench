# Redis

| | |
|---|---|
| Version | `REDIS_IMAGE` in `../db.env` (8.10.3, vector search included) |
| Port | **6379** |
| Password | `REDIS_PASSWORD` in `../db.env` (always required) |
| Data | `redis/volumes/redis/dump.rdb` |

## Start and check

```bash
./db.sh start redis
./db.sh test redis       # prints PASS
```

## Good to know

- Redis keeps everything in **memory**; `dump.rdb` is only a snapshot. Before stopping, run
  `docker exec redis redis-cli SAVE`, or the latest data is lost. (The benchmark does this itself.)
- On start Redis rebuilds its vector indexes from `dump.rdb`: about 40 min for 10M vectors.
- Searches longer than **500 ms** are cut off and return partial results (Redis' default). For large collections
  with broad filters, add `"--search-timeout", "30000"` to the `command` in `docker-compose.yml` and restart.
- For very large loads (10M) set `REDIS_SAVE=` (empty) in `../db.env`: background snapshots can double the memory.

## Delete an index ("collection")

```bash
docker exec redis redis-cli FT._LIST                    # list
docker exec redis redis-cli FT.DROPINDEX <index> DD     # delete the index and its data
```

## Change version

Set `REDIS_IMAGE` in `../db.env`, then `./db.sh wipe redis && ./db.sh start redis`.

## Build from source (optional)

```bash
git clone https://github.com/redis/redis.git ~/redis
./redis/build-from-source.sh           # about 20 min; builds the image redis-local:<tag>
```

Put that image name in `REDIS_IMAGE` and restart Redis.
