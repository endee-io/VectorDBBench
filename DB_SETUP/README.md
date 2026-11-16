# Database Setup

Runs the databases we benchmark against Endee (**Milvus, Qdrant, Redis, Vespa**) in Docker on the database server.
One settings file (`db.env`) and one command (`./db.sh`).

| Database | Version | Port |
|---|---|---|
| Milvus | 3.0.5 | 19530 |
| Qdrant | 1.20.0 | 6333 (REST), 6334 (gRPC) |
| Redis | 8.10.3 | 6379 (password in `db.env`) |
| Vespa | 8.781.11 | 8080 (19071 to deploy) |

## Setup (once per server)

1. Install Docker and the tools (Linux):
   ```bash
   curl -fsSL https://get.docker.com | sudo sh
   sudo usermod -aG docker $USER          # then log out and log in again
   sudo apt-get install -y curl jq zip python3
   ```
2. Copy this `DB_SETUP` folder to the server.
3. Open `db.env` and replace every `CHANGE_ME`:
   - `BIND_IP`: the server's private IP (`hostname -I` shows it)
   - `REDIS_PASSWORD`: any password; use the same one in the benchmark commands (`--password`)
   - `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`: any login for Milvus' storage (secret at least 8 characters)
4. Start a database and check it:
   ```bash
   ./db.sh start qdrant
   ./db.sh test qdrant                    # prints PASS
   ```
5. In the AWS security group, open the database's port **only** to the benchmark client's private IP.

Then run the benchmark from the client: see [../benchmark_commands/README.md](../benchmark_commands/README.md).

## Commands

| Command | What it does |
|---|---|
| `./db.sh start <db>` | start (`all` = every database) |
| `./db.sh stop <db>` | stop; the data is kept |
| `./db.sh status` | show what is running |
| `./db.sh test <db>` | quick check, prints PASS or FAIL |
| `./db.sh logs <db>` | show the live logs |
| `./db.sh wipe <db>` | stop and **delete all data** of that database |
| `./db.sh versions` | show if a newer version exists |

`<db>` is `milvus`, `qdrant`, `redis` or `vespa`.

## Good to know

- **One database at a time** for benchmarks: they share the server's memory and CPU.
- **Vespa and Endee both use port 8080:** stop one before starting the other.
- **Restarting** keeps the data, but:
  - Milvus: a collection must be loaded again before searching (the benchmark does it).
  - Redis: it rebuilds its indexes on start, which can take as long as the first load (about 40 min at 10M).
    Run `docker exec redis redis-cli SAVE` before stopping it, or the latest data is lost.
- **Run `./db.sh test vespa` only before loading benchmark data:** it deploys its own small test app.
- **Large collections (10M):**
  - Redis cuts off searches after 500 ms (its default) and returns partial results. For slow filtered searches add
    `"--search-timeout", "30000"` to the `command` in `redis/docker-compose.yml` and restart Redis.
  - Redis: set `REDIS_SAVE=` (empty) in `db.env` so background snapshots don't double the memory.
  - Vespa stops accepting data when its disk is about 75 % full; keep enough free disk.
- **Qdrant:** use gRPC in the benchmark (`--prefer-grpc true`), it is much faster than REST.

## Change a version

Change the image in `db.env`, then:

```bash
./db.sh wipe <db>        # a new version may not read the old data
./db.sh start <db>
```

To change any other setting in `db.env`: `./db.sh stop <db> && ./db.sh start <db>`.

## Where the data is

On the server's disk (not inside the containers), so the data survives stopping or removing a container:

| Database | Folder on the server |
|---|---|
| Milvus | `DB_SETUP/milvus/volumes/` |
| Qdrant | `DB_SETUP/qdrant/volumes/` |
| Redis | `DB_SETUP/redis/volumes/redis/dump.rdb` |
| Vespa | `/var/lib/docker/volumes/vespa_vespa-var/` (a Docker volume) |

Docker creates these folders on the first start. Only `./db.sh wipe <db>` deletes them.
The files belong to root: use `sudo` to look inside. To move a collection to another server, see [BACKUP.md](BACKUP.md).

## Troubleshooting

| Problem | Fix |
|---|---|
| `cannot assign requested address` | `BIND_IP` in `db.env` is not this server's private IP |
| `address already in use` | another service uses the port (8080: Endee or Vespa); check `docker ps` |
| `permission denied ... docker` | `sudo usermod -aG docker $USER`, then log out and in |
| `'compose' is not a docker command` | install Docker with the command in step 1 |
| `missing settings file` | `db.env` must be in the same folder as `db.sh` |
| `NOT ready after 2 min` | look at `./db.sh logs <db>`; Milvus and Vespa can take 1-2 min |
| Milvus: "collection not loaded" | load it again after a restart (`load_collection`) |
| Vespa refuses data | disk over ~75 % or memory over ~80 % |
| Disk full | `./db.sh wipe <db>` for databases you don't need; `docker image prune` |

## One database in detail

[Milvus](milvus/README.md) · [Qdrant](qdrant/README.md) · [Redis](redis/README.md) · [Vespa](vespa/README.md)
(delete a collection, change version, build from source).
