#!/usr/bin/env python3
"""Exercise Kubernetes discovery, scaling, PVC replacement, and stream recovery.

Uses its own kind cluster and kubeconfig; never changes the user's current context.
Set WALLEYE_K8S_SKIP_SETUP=1 to test the already-running dedicated fixture.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.request
from concurrent_streams import exercise as exercise_concurrent, verify as verify_concurrent

ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / '.acceptance'
KIND = os.environ.get('KIND', 'kind')
KUBE = ['kubectl', '--kubeconfig', str(REPORTS / 'kubeconfig'), '--context', 'kind-walleye', '-n', 'walleye']
TOKEN = 'acceptance-walleye-token'

def run(args, timeout=180):
    return subprocess.check_output(args, cwd=ROOT, text=True, timeout=timeout)

def kube(*args, timeout=180):
    return run(KUBE + list(args), timeout)

def wait_until(check, message, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except Exception as error:
            last = str(error)
        time.sleep(1)
    raise AssertionError(f'{message}; last error: {last}')

class Forward:
    def __enter__(self):
        self.log = (REPORTS / 'kubernetes-port-forward.log').open('a')
        self.proc = subprocess.Popen(KUBE + ['port-forward', 'service/walleye-api', '18085:8080'], cwd=ROOT, stdout=self.log, stderr=self.log)
        wait_until(lambda: self.request('/healthz', text=True) == 'ok', 'API forwarding failed', 30)
        return self
    def __exit__(self, *_):
        self.proc.terminate()
        self.proc.wait(timeout=10)
        self.log.close()
    def request(self, path, body=None, text=False):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request('http://127.0.0.1:18085' + path, data,
            headers={'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=90) as response:
            value = response.read().decode()
            return value if text else json.loads(value)
    def members(self, count):
        return wait_until(lambda: (s if len(s['members']) == count else None)
            if (s := self.request('/internal/cache/stats')) else None,
            f'cache must discover {count} members', 60)

def replace_pod(name, force=False):
    before = json.loads(kube('get', 'pod', name, '-o', 'json'))['metadata']['uid']
    args = ['delete', 'pod', name]
    if force:
        args += ['--grace-period=0', '--force']
    kube(*args)
    def replacement():
        pod = json.loads(kube('get', 'pod', name, '-o', 'json'))
        return pod if pod['metadata']['uid'] != before and any(
            c['type'] == 'Ready' and c['status'] == 'True'
            for c in pod.get('status', {}).get('conditions', [])) else None
    return wait_until(replacement, f'{name} must be replaced and ready', 180)

def main():
    REPORTS.mkdir(exist_ok=True)
    if os.environ.get('WALLEYE_K8S_SKIP_SETUP') != '1':
        if not shutil.which(KIND):
            raise SystemExit('Install kind or set KIND to its executable path.')
        clusters = run([KIND, 'get', 'clusters']).splitlines()
        if 'walleye' not in clusters:
            print(run([KIND, 'create', 'cluster', '--name', 'walleye', '--kubeconfig', str(REPORTS / 'kubeconfig'), '--config', 'integration/kind.yaml', '--wait', '120s'], 300))
        print(run(['docker', 'build', '-t', 'walleye:local', '.'], 1800))
        images = ['walleye:local', 'quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z',
                  'quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z']
        for image in images[1:]:
            run(['docker', 'pull', image], 180)
        platform = run(['docker', 'image', 'inspect', 'walleye:local', '--format', '{{.Os}}/{{.Architecture}}']).strip()
        archive = str(REPORTS / 'kubernetes-images.tar')
        # Export one platform: Docker Desktop's multi-platform image index may
        # reference attestations that are absent from its local content store.
        run(['docker', 'image', 'save', '--platform', platform, '-o', archive] + images, 180)
        print(run([KIND, 'load', 'image-archive', archive, '--name', 'walleye'], 240))
        Path(archive).unlink()
        print(kube('apply', '-k', 'integration/kubernetes'))
        kube('rollout', 'restart', 'statefulset/walleye-bitr', 'statefulset/walleye-cache')
        for name in ('walleye-bitr', 'walleye-cache'):
            print(kube('rollout', 'status', 'statefulset/' + name, '--timeout=180s', timeout=200))
    run_id = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')
    stream = 'k8s_' + run_id
    n = 8192
    def rows(start, end):
        return [{'id': i, 'payload': ''.join(hashlib.sha256(f'{i}:{p}'.encode()).hexdigest() for p in range(4))} for i in range(start, end)]
    def verify(api, count):
        result = api.request('/v1/query', {'sql': f'SELECT count(*) AS n, sum(id) AS total FROM {stream}'})
        assert result == [{'n': count, 'total': count * (count - 1) // 2}], result
    with Forward() as api:
        initial = api.members(3)
        concurrent = exercise_concurrent('http://127.0.0.1:18085', 'concurrent_k8s_' + run_id)
        api.request('/v1/streams', {'name': stream, 'columns': [
            {'name': 'id', 'type': 'int64'}, {'name': 'payload', 'type': 'string'}], 'primary_key': ['id']})
        for start in range(0, n, 1024):
            assert api.request(f'/v1/streams/{stream}/events', {'rows': rows(start, start + 1024)})['ingested'] == 1024
        verify(api, n)
        kube('scale', 'statefulset/walleye-cache', '--replicas=4')
        grown = api.members(4)
        verify(api, n)
        kube('scale', 'statefulset/walleye-cache', '--replicas=3')
        shrunk = api.members(3)
        verify(api, n)
    # SIGTERM lets the ingress checkpoint into Lance before Kubernetes replaces it.
    replaced_ingress = replace_pod('walleye-cache-0')
    with Forward() as api:
        api.members(3)
        for _ in range(3):
            verify(api, n)
        # Read ordinary persisted data blocks through the distributed Foyer backend.
        result = api.request('/v1/query', {'sql': f'SELECT id, payload FROM {stream} ORDER BY id LIMIT 1000'})
        assert len(result) == 1000 and result[0] == rows(0, 1)[0]
        api.request('/v1/query', {'sql': f'SELECT id, payload FROM {stream} ORDER BY id LIMIT 1000'})
    replaced_bitr = replace_pod('walleye-bitr-1')
    with Forward() as api:
        assert api.request(f'/v1/streams/{stream}/events', {'rows': rows(n, n + 128)})['ingested'] == 128
        n += 128
        verify(api, n)
    # Stop the process without a checkpoint; Kubernetes replacement must recover the tail.
    previous = json.loads(kube('get', 'pod', 'walleye-cache-0', '-o', 'json'))['status']['containerStatuses'][0]['restartCount']
    pod = json.loads(kube('get', 'pod', 'walleye-cache-0', '-o', 'json'))
    worker = pod['spec']['nodeName']
    assert worker.startswith('walleye-'), worker
    container = pod['status']['containerStatuses'][0]['containerID'].split('://', 1)[1]
    # Signal from the kind worker's host namespace: PID 1 ignores some signals
    # sent by children inside its own PID namespace.
    run(['docker', 'exec', worker, 'ctr', '--namespace', 'k8s.io', 'tasks', 'kill', '--signal', 'SIGKILL', container])
    def restarted():
        pod = json.loads(kube('get', 'pod', 'walleye-cache-0', '-o', 'json'))
        return pod['status'].get('containerStatuses', [{}])[0].get('restartCount', 0) > previous and any(
            c['type'] == 'Ready' and c['status'] == 'True' for c in pod['status'].get('conditions', []))
    wait_until(restarted, 'ingress process must restart after SIGKILL', 180)
    with Forward() as api:
        wait_until(lambda: (verify(api, n) is None), 'WAL tail recovery after process crash', 120)
        api.members(3)
        verify_concurrent('http://127.0.0.1:18085', concurrent['streams'])
        concurrent['all_streams_verified_after_crash'] = True
    stats = []
    for i in range(3):
        stats.append(json.loads(kube('exec', 'walleye-cache-0', '--', 'curl', '-fsS', '-H',
            'Authorization: Bearer ' + TOKEN, f'http://walleye-cache-{i}.walleye-cache:8080/internal/cache/stats')))
    assert all(s['hits'] > 0 for s in stats), stats
    pods = json.loads(kube('get', 'pods', '-l', 'app=walleye-bitr', '-o', 'json'))['items']
    assert len(pods) == 3
    report = {'run_id': run_id, 'rows': n, 'cache_members': [len(initial['members']), len(grown['members']), len(shrunk['members'])],
        'sql_through_scaling': True, 'ingress_replacement': replaced_ingress['metadata']['uid'],
        'bitr_replacement': replaced_bitr['metadata']['uid'], 'wal_tail_recovery': True,
        'bitr_replicas': len(pods), 'cache_stats': stats, 'concurrent_streams': concurrent}
    (REPORTS / 'kubernetes.json').write_text(json.dumps(report, indent=2) + '\n')
    print(f'Kubernetes acceptance passed: {n} rows, cache 3 -> 4 -> 3, pod replacement and WAL recovery.')

if __name__ == '__main__':
    main()
