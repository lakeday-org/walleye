"""Concurrent HTTP writes to independent memshards, using either durability mode."""
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import urllib.request

TOKEN = 'acceptance-walleye-token'

def request(base, path, body):
    req = urllib.request.Request(base + path, json.dumps(body).encode(), headers={
        'Content-Type': 'application/json', 'Authorization': 'Bearer ' + TOKEN})
    try:
        with urllib.request.urlopen(req, timeout=90) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        # The node says why in the body; a bare status turns a one-line
        # diagnosis into a bisect.
        raise AssertionError(
            f'{path} answered {error.code}: {error.read().decode()[:400]}') from None

def verify(base, streams):
    for stream in streams:
        sql = f'SELECT count(*) AS n, count(DISTINCT id) AS unique_ids, sum(value) AS total FROM {stream}'
        assert request(base, '/v1/query', {'sql': sql}) == [
            {'n': 1024, 'unique_ids': 1024, 'total': 523776}], stream

def exercise(base, prefix):
    streams = [f'{prefix}_{i}' for i in range(4)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        definitions = [{'name': name, 'columns': [
            {'name': 'id', 'type': 'int64'}, {'name': 'value', 'type': 'int64'}],
            'primary_key': ['id']} for name in streams]
        # Concurrent repeated definitions must not create competing shard writers.
        defined = list(pool.map(lambda d: request(base, '/v1/streams', d), definitions * 2))
        assert sorted(r['stream'] for r in defined) == sorted(streams * 2)
        start = threading.Event()
        def ingest(stream, batch):
            start.wait(timeout=10)
            rows = [{'id': i, 'value': i} for i in range(batch*128, (batch+1)*128)]
            result = request(base, f'/v1/streams/{stream}/events', {'rows': rows})
            assert result['ingested'] == 128
        pending = [pool.submit(ingest, stream, batch) for batch in range(8) for stream in streams]
        start.set()
        reads_during_writes = 0
        # SQL runs alongside writers and must see a complete prefix for each
        # stream. Exact final counts/checksums below detect lost or duplicate rows.
        while not all(f.done() for f in pending):
            counts = request(base, '/v1/query', {'sql': ' UNION ALL '.join(
                f'SELECT count(*) AS n FROM {stream}' for stream in streams)})
            assert len(counts) == 4
            assert all(0 <= row['n'] <= 1024 and row['n'] % 128 == 0 for row in counts), counts
            reads_during_writes += 1
        for future in pending:
            future.result()
    verify(base, streams)
    return {'streams': streams, 'concurrent_clients': 8, 'rows_per_stream': 1024,
            'total_rows': 4096, 'queries_during_writes': reads_during_writes,
            'row_counts_distinct_ids_and_sums_verified': True}
