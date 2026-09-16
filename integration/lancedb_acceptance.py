"""Acceptance: the stock lancedb SDK against a running Walleye node.

    WALLEYE_URL=http://localhost:8080 WALLEYE_TOKEN=... python integration/lancedb_acceptance.py
"""
import os
import sys
import time

import lancedb
import pyarrow as pa

url = os.environ.get("WALLEYE_URL", "http://localhost:8080")
token = os.environ["WALLEYE_TOKEN"]
name = os.environ.get("WALLEYE_TABLE", f"sdk_{int(time.time())}")

db = lancedb.connect("db://walleye", api_key=token, host_override=url, region="local")

rows = [
    {"id": 1, "city": "seattle", "vector": [0.0, 1.0]},
    {"id": 2, "city": "seattle", "vector": [1.0, 0.0]},
    {"id": 3, "city": "portland", "vector": [0.0, -1.0]},
]
tbl = db.create_table(name, data=rows, mode="overwrite")
print("created", name, "schema:", tbl.schema)

tbl.add([{"id": 4, "city": "boise", "vector": [-1.0, 0.0]}])
tbl.add([{"id": 4, "city": "boise", "vector": [-1.0, 0.0]}])  # retried insert: must collapse
assert tbl.count_rows() == 4, tbl.count_rows()
assert tbl.count_rows("city = 'seattle'") == 2
assert name in db.list_tables().tables

scan = tbl.search().where("id > 1").select(["id", "city"]).to_list()
assert sorted(r["id"] for r in scan) == [2, 3, 4], scan

nearest = tbl.search([0.0, 0.9]).limit(2).to_list()
assert [r["id"] for r in nearest] == [1, 2], nearest
assert "_distance" in nearest[0] and "_walleye_pk" not in nearest[0], nearest[0]

filtered = tbl.search([0.0, 0.9]).where("city = 'portland'").limit(5).to_list()
assert [r["id"] for r in filtered] == [3], filtered

df = tbl.search().to_pandas()
assert len(df) == 4 and list(df.columns) == ["id", "city", "vector"], df

reopened = db.open_table(name)
assert reopened.count_rows() == 4

try:
    tbl.create_index(metric="l2", vector_column_name="vector")
    print("index created (base table had rows)")
except Exception as e:  # expected until LSM compaction lands
    assert "compaction" in str(e), e
    print("index creation refused as documented")

db.drop_table(name)
assert name not in db.list_tables().tables
print("OK: create, add (idempotent retry), count, filter, project, vector search, pandas, open, drop")
