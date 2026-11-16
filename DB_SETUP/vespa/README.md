# Vespa

| | |
|---|---|
| Version | `VESPA_IMAGE` in `../db.env` (8.781.11) |
| Ports | **8080** search and feed, **19071** deploy |
| Data | Docker volume `vespa_vespa-var` |

## Start and check

```bash
./db.sh start vespa
./db.sh test vespa       # deploys the small test app in app/, prints PASS
```

Run the test only on an **empty** Vespa (before loading benchmark data), because it deploys its own test app.

## Good to know

- Vespa serves nothing until an application (its schemas) is deployed. The benchmark deploys its own;
  `app/` is only for the test.
- Each collection is a schema. Several can live side by side; the benchmark keeps the existing ones.
- **Port 8080 is also Endee's:** stop one before starting the other.
- Vespa refuses new data when its disk is about 75 % full (or memory about 80 %). Keep enough free disk for large loads.

## Delete data

```bash
# all documents of one schema (the schema stays):
curl -s -X DELETE 'localhost:8080/document/v1/<namespace>/<schema>/docid?selection=true&cluster=<content-id>'
```

Everything at once: `./db.sh wipe vespa`.

## Change version

Set `VESPA_IMAGE` in `../db.env`, then `./db.sh wipe vespa && ./db.sh start vespa`.

## Build from source (optional, untested)

```bash
git clone https://github.com/vespa-engine/vespa.git ~/vespa
./vespa/build-from-source.sh 8.999.1   # 4-5 hours; builds RPMs
```

This script has never been run end to end, so expect to fix things on the first run.
