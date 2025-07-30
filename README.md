# DSQL PingPong Python Demo

A demonstration script for Amazon Aurora DSQL using SQLAlchemy.

## Usage

First, initialize the database schema:
```bash
uv run src/demo.py --identifier $your_cluster_id --initialize
```

Then start the first client:
```bash
uv run src/demo.py --identifier $your_cluster_id --start
```

Connect additional clients (without --start):
```bash
uv run src/demo.py --identifier $your_cluster_id
```

## Multi-Region

You can connect the second client to a different cluster that is peered with the first cluster:

```bash
# First client (region 1)
uv run src/demo.py --identifier $your_cluster_id --start

# Second client (region 2, peered cluster)
uv run src/demo.py --identifier $your_peered_cluster_id
```

## GitHub

Run directly from GitHub:
```bash
uvx --from git+https://github.com/marcbowes/dsql-pingpong-py dsql-demo --identifier $your_cluster_id --start
```
