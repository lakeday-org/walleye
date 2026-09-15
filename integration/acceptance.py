#!/usr/bin/env python3
"""Run the standalone S3 and three-node acceptance contract and preserve evidence."""
import datetime
import json
import os
from pathlib import Path
import subprocess
import urllib.request
from concurrent_streams import exercise, verify

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / '.acceptance'
COMPOSE = ['docker', 'compose', '-f', str(ROOT / 'integration/compose.yaml')]

def run(args, *, capture=False, timeout=180):
    return subprocess.run(args, cwd=ROOT, env=os.environ, check=True,
                          text=True, stdout=subprocess.PIPE if capture else None,
                          timeout=timeout)

def main():
    REPORTS.mkdir(exist_ok=True)
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')
    os.environ['WALLEYE_RUN_ID'] = run_id
    run(['docker', 'build', '-t', 'walleye:local', '.'], timeout=1800)
    run(COMPOSE + ['up', '-d', '--wait', '--wait-timeout', '120', 'node-1', 'node-2', 'node-3', 'single'], timeout=180)
    reports = {}
    for mode in ('single', 'cluster'):
        # Separate prefixes allow repeated runs without altering earlier evidence.
        os.environ['WALLEYE_RUN_ID'] = run_id + mode
        output = subprocess.run(
            COMPOSE + ['run', '--rm', '--no-deps', 'workload', mode],
            cwd=ROOT, env=os.environ, text=True, capture_output=True, timeout=180)
        (REPORTS / f'{mode}.log').write_text(output.stderr + output.stdout)
        if output.returncode:
            print(output.stderr)
        output.check_returncode()
        report = json.loads(output.stdout)
        (REPORTS / f'{mode}.json').write_text(json.dumps(report, indent=2) + '\n')
        reports[mode] = report
        print(f'{mode}: {report["rows"]} rows; S3 CAS, hot/checkpoint/reopen reads and resource reclamation verified')
    concurrent = {}
    for mode, base in [('single', 'http://127.0.0.1:18084'), ('cluster', 'http://127.0.0.1:18081')]:
        concurrent[mode] = exercise(base, 'concurrent_' + run_id + mode)
        print(f'{mode}: four streams, eight concurrent clients, 4096 rows verified')
    reports['concurrent_streams'] = concurrent
    api_stream = 'api_' + run_id
    def request(path, body):
        req = urllib.request.Request('http://127.0.0.1:18081' + path,
            json.dumps(body).encode(), headers={'Content-Type': 'application/json',
            'Authorization': 'Bearer acceptance-walleye-token'})
        with urllib.request.urlopen(req, timeout=90) as response:
            return json.load(response)
    definition = {'name': api_stream, 'columns': [
        {'name': 'id', 'type': 'int64'}, {'name': 'city', 'type': 'string'},
        {'name': 'value', 'type': 'float64'}], 'primary_key': ['id']}
    assert request('/v1/streams', definition)['stream'] == api_stream
    rows = [{'id': i, 'city': 'seattle' if i % 2 else 'portland', 'value': float(i)} for i in range(1000)]
    assert request(f'/v1/streams/{api_stream}/events', {'rows': rows})['ingested'] == 1000
    sql = f'SELECT city, count(*) AS n, sum(value) AS total FROM {api_stream} GROUP BY city ORDER BY city'
    expected = [{'city': 'portland', 'n': 500, 'total': 249500.0},
                {'city': 'seattle', 'n': 500, 'total': 250000.0}]
    assert request('/v1/query', {'sql': sql}) == expected
    # Hard-stop ingress: acknowledged hot rows must recover from Bitr without a
    # graceful Lance checkpoint. Other two replicas retain the quorum.
    run(COMPOSE + ['kill', '-s', 'SIGKILL', 'node-1'])
    run(COMPOSE + ['up', '-d', '--wait', '--wait-timeout', '120', 'node-1'], timeout=180)
    assert request('/v1/query', {'sql': sql}) == expected
    verify('http://127.0.0.1:18081', concurrent['cluster']['streams'])
    run(COMPOSE + ['kill', '-s', 'SIGKILL', 'single'])
    run(COMPOSE + ['up', '-d', '--wait', '--wait-timeout', '120', 'single'], timeout=180)
    verify('http://127.0.0.1:18084', concurrent['single']['streams'])
    for report in concurrent.values():
        report['all_streams_verified_after_crash'] = True
    reports['api'] = {'stream': api_stream, 'ingested': 1000, 'sql_aggregation': True,
                      'recovery_after_ingress_crash': True, 'result': expected}
    evidence = {'run_id': run_id, 'reports': reports}
    (REPORTS / 'acceptance.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(f'Evidence: {REPORTS / "acceptance.json"}')

if __name__ == '__main__':
    main()
