#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license
# agreements. See the NOTICE file distributed with this work for additional information.
# Licensed under the Apache License, Version 2.0, http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
"""Real data-scan >=8MiB reservation failure and original B/C cleanup, in owned RAM services."""
from pathlib import Path
import argparse
import ast
import base64
import collections
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import time
import traceback
import zipfile

HERE = Path(__file__).resolve().parent
JAVA = Path('/usr/lib/jvm/java-17-openjdk-amd64/bin/java')
NODES = [(side, node) for side in ('candidate', 'control')
         for node in ('confignode', 'datanode-1', 'datanode-2')]
HUGE = 8 * 1024 * 1024


def require(value, message):
    if not value:
        raise AssertionError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            digest.update(block)
    return digest.hexdigest()


def save(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False)
        stream.write('\n')


def read(path):
    return json.loads(Path(path).read_text())


def bound(item):
    path = Path(item['path'])
    require(path.is_absolute() and path.is_file() and sha(path) == item['sha256'],
            'Entry identity differs: ' + str(path))
    return path


def listeners():
    text = subprocess.check_output(['ss', '-H', '-ltnp'], text=True)
    return text, {int(line.split()[3].rsplit(':', 1)[-1]) for line in text.splitlines()}


def error_bytes(message):
    require('Fetch Table Device Schema failed' not in message, 'Schema failure is not data oversize')
    patterns = [r'bytesToReserve\s*[:：]\s*(\d+)B?[,，]\s*maxBytesCanReserve\s*[:：]\s*(\d+)B?',
                r'maxBytesPerFI\s*[:：]\s*(\d+)[,，]\s*bytesToReserve\s*[:：]\s*(\d+)']
    direct = re.search(patterns[0], message)
    if direct:
        request, cap = map(int, direct.groups())
    else:
        reverse = re.search(patterns[1], message)
        require(reverse is not None, 'No real numeric MemoryPool reservation failure')
        cap, request = map(int, reverse.groups())
    require(request >= HUGE and request > cap >= 1048576, 'Not an actual >=8MiB data request over a valid bootstrap cap')
    return {'actualRequestBytes': request, 'actualCapBytes': cap}




def memory_failure_provenance(message, server_log):
    """Keep client errors; unwrap only a scheduler error linked to the exact failing driver."""
    try:
        numeric = error_bytes(message)
    except AssertionError:
        task = re.search(r'DriverTask ([A-Za-z0-9_]+\.\d+\.\d+\.\d+) is aborted by internal error scheduled', message)
        require(task is not None, 'No numeric client memory error or exact scheduler driver identity')
        driver_id = task.group(1)
        lines = server_log.splitlines()
        matches = []
        for index, line in enumerate(lines[:-1]):
            if re.search(r'\[Query-Worker-Thread-\d+\$' + re.escape(driver_id) + r'\]', line) and '[ExecuteFailed]' in line:
                numeric = error_bytes(lines[index + 1])
                matches.append({'lineNumber': index + 1, 'header': line, 'cause': lines[index + 1],
                                'numericMemoryPoolFailure': numeric})
        require(len(matches) == 1, 'No unique exact-driver numeric MemoryPool failure in server log')
        return {'source': 'EXACT_CLIENT_DRIVER_MATCHED_SERVER_EXCEPTION',
                'clientMessage': message, 'driverTaskId': driver_id,
                'queryId': driver_id.rsplit('.', 3)[0],
                'fragmentInstanceId': driver_id.rsplit('.', 1)[0],
                'numericMemoryPoolFailure': matches[0]['numericMemoryPoolFailure'],
                'matchedServerException': matches[0]}
    return {'source': 'NUMERIC_CLIENT_MEMORY_POOL_EXCEPTION', 'clientMessage': message,
            'numericMemoryPoolFailure': numeric}


def profiled_terminal_statistics(responses, failure_identity=None):
    """Classify only explicit NOT_FOUND as unprofiled; never accept it as a source witness."""
    require(responses, 'Missing actual production FI statistics')
    available, unprofiled = [], []
    for item in responses:
        require('rpcFailure' not in item, 'Production FI statistics RPC failed')
        if item.get('statusCode') == 200:
            require(item.get('state') in {'FINISHED', 'FAILED', 'ABORTED', 'CANCELLED'}
                    and item.get('isTerminalSet') and item.get('isTerminal'),
                    'An available FI lacks frozen terminal state')
            available.append(item)
        else:
            reason = item.get('rawThrift', {}).get('23', {}).get('lst')
            require(item.get('statusCode') == 301 and reason == ['str', 1, 'NOT_FOUND'],
                    'Unexpected production FI statistics status or validity reason')
            unprofiled.append(item)
    require(available, 'No available profiled production FI statistics')
    started = [item for item in available if item.get('state') == 'FAILED' and item.get('dataRegion')
               and any(driver.get('firstProcessPresent') and driver.get('firstProcessNanos', 0) > 0
                       and driver.get('wallPresent') and driver.get('wallNanos', 0) > 0
                       for driver in item.get('drivers', []))]
    if failure_identity and 'fragmentInstanceId' in failure_identity:
        started = [item for item in started
                   if item.get('queryId') == failure_identity['queryId']
                   and item.get('fragmentInstanceId') == failure_identity['fragmentInstanceId']
                   and any(driver.get('driverTaskId') == failure_identity['driverTaskId']
                           and driver.get('firstProcessPresent') and driver.get('firstProcessNanos', 0) > 0
                           and driver.get('wallPresent') and driver.get('wallNanos', 0) > 0
                           for driver in item.get('drivers', []))]
    require(started, 'No failed actual data-region FI with a genuinely started driver matching the memory failure')
    scan_witnesses = [item for item in started if any('TableScan' in op.get('operatorType', '')
                      and op.get('nextCalledCount', 0) > 0 for op in item.get('operators', []))]
    emitted = [item for item in started if any(driver.get('outputRowsPresent') and driver.get('outputRows', 0) > 0
               and driver.get('tsBlockCountPresent') and driver.get('tsBlockCount', 0) > 0
               for driver in item.get('drivers', []))]
    require(scan_witnesses or emitted, 'Data driver did not actually scan/emit before the error')
    for item in started:
        require(item.get('snapshotChecksum') and item.get('planIdentityChecksum'),
                'Missing frozen source identity/checksum')
    return available, unprofiled, started, scan_witnesses, emitted


def actual_classpath_identity(actual, expected_jars, expected_sha):
    entries = actual.split(os.pathsep)
    expected = list(map(str, expected_jars))
    require(entries and all(Path(entry).is_absolute() and not any(c in entry for c in "*?[]")
                            for entry in entries), "Actual service classpath contains wildcard/relative/empty entry")
    require(len(entries) == len(set(entries)) and set(entries) == set(expected),
            "Actual service classpath library set differs")
    libraries = []
    for entry in entries:
        require(sha(entry) == expected_sha[entry], "Actual service classpath JAR SHA differs: " + entry)
        libraries.append({"path": entry, "sha256": expected_sha[entry]})
    return {"actualOrderedClasspath": entries, "actualOrderedLibraries": libraries,
            "orderingPolicy": "Preserve actual launcher order; require complete exact library set and every JAR SHA"}


def distribution_identity(distribution, packaged_files):
    actual = {str(path.relative_to(distribution)): sha(path)
              for path in distribution.rglob("*") if path.is_file()}
    missing = sorted(set(packaged_files) - set(actual))
    changed = sorted(name for name in set(packaged_files) & set(actual)
                     if packaged_files[name] != actual[name])
    added = sorted(set(actual) - set(packaged_files))
    generated_logs = []
    unexpected = []
    for name in added:
        relative = Path(name)
        if len(relative.parts) == 2 and relative.parts[0] == "logs" and re.fullmatch(
                r"[A-Za-z0-9_.-]+\.log(?:\.[A-Za-z0-9_.-]+)?", relative.name):
            generated_logs.append({"path": name, "sha256": actual[name],
                                   "bytes": (distribution / name).stat().st_size})
        else:
            unexpected.append(name)
    expected_libs = {name for name in packaged_files if name.startswith("lib/") and name.endswith(".jar")}
    actual_libs = {name for name in actual if name.startswith("lib/") and name.endswith(".jar")}
    passed = not missing and not changed and not unexpected and expected_libs == actual_libs
    return {"status": "PASS" if passed else "FAIL", "allPackagedFilesUnchanged": not missing and not changed,
            "exactLibrarySetUnchanged": expected_libs == actual_libs,
            "packagedFileCount": len(packaged_files), "packagedFilesMissing": missing,
            "packagedFilesChanged": changed, "generatedRuntimeLogs": generated_logs,
            "unexpectedAddedFiles": unexpected, "addedFiles": added,
            "actualLibraryFiles": sorted(actual_libs), "expectedLibraryFiles": sorted(expected_libs)}


class Runtime:
    def __init__(self, manifest, ram, evidence):
        self.m, self.ram, self.evidence = manifest, ram, evidence
        self.cluster, self.dist, self.run = ram / 'cluster', ram / 'distribution', ram / 'run'
        self.run.mkdir()
        self.classes = self.run / 'classes'
        self.jars = []
        self.cp = ''
        self.backups = []
        self.processes = []
        self.phases = []
        self.counter = 0
        self.open_windows = []
        self.distribution_runtime_paths = set()
        self.base_ports = [item['port'] for item in read(bound(manifest['coreAudit']))['closedOwnedPorts']]
        self.offset = manifest['preferredCorePortOffset']
        live, occupied = listeners()
        forbidden = {value + 14000 for value in self.base_ports} | {61230, 61231, 61240, 62230, 62231, 62240}
        while ({value + self.offset for value in self.base_ports} & (occupied | forbidden)):
            self.offset += 500
        self.ports = sorted({value + self.offset for value in self.base_ports})
        require(len(self.ports) == len(set(self.base_ports)) == 34, 'Core complete endpoint set differs')
        require(max(self.ports) < 65536 and self.offset <= 26000, 'No supported free port offset')
        self.sql_ports = {'candidate': 27667 + self.offset, 'control': 37667 + self.offset}
        self.jmx_ports = [33130 + self.offset, 34130 + self.offset, 43130 + self.offset, 44130 + self.offset]
        save(self.run / 'port-binding.json', {'coreBasePorts': self.base_ports, 'selectedOffset': self.offset,
             'allOwnedPortsIncludingJmx': self.ports, 'queryPorts': self.sql_ports, 'jmxPorts': self.jmx_ports,
             'initialSsRaw': live, 'occupiedBefore': sorted(occupied), 'forbiddenPorts': sorted(forbidden)})

    def command(self, label, command, timeout=180):
        path = self.run / label
        save(path.with_suffix('.before.json'), {'command': list(map(str, command)), 'cwd': str(self.run),
             'orchestrator': str(Path(__file__).resolve()), 'orchestratorSha256': sha(__file__),
             'java': str(JAVA.resolve()), 'javaSha256': sha(JAVA.resolve()),
             'runtimeClasspath': self.cp, 'sourceManifest': self.m['sourceManifest'],
             'distributionServerJarSha256': sha(self.dist / 'lib/iotdb-server-2.0.11-SNAPSHOT.jar')
                if self.dist.exists() else None})
        with path.with_suffix('.log').open('x') as stream:
            result = subprocess.run(list(map(str, command)), cwd=self.run, stdout=stream,
                                    stderr=subprocess.STDOUT, timeout=timeout, check=False)
        save(path.with_suffix('.exit.json'), {'exitCode': result.returncode})
        print(json.dumps({'step': label, 'exitCode': result.returncode}), flush=True)
        require(result.returncode == 0, label + ' failed; original log retained')

    def java(self, label, main, args, timeout=180):
        self.command(label, [JAVA, '--add-opens=java.base/java.lang=ALL-UNNAMED', '-Xmx768m', '-cp', self.cp, main, *args], timeout)

    def launch(self, label, mode):
        self.command(label, ['bash', bound(self.m['launcher']), '--mode', mode, '--root', self.cluster,
                            '--distribution-root', self.dist, '--port-offset', str(self.offset),
                            '--startup-timeout-seconds', '120'], 600)

    def sql(self, label, side, text):
        path = self.run / (label + '.sql')
        path.write_text(text)
        result = self.run / (label + '.json')
        self.java(label, 'A6SqlProbe', [str(self.sql_ports[side]), path, result], 300)
        return read(result)['results']

    def jmx(self, label, mode, state):
        args = [mode, state]
        output = self.run / (label + '.json')
        if mode == 'open':
            args.append(','.join(map(str, self.jmx_ports)))
        elif mode == 'read':
            args.append(output)
        self.java(label, 'BResourceJmxProbe', args, 90)
        return read(output) if mode == 'read' else None

    def initial_bind(self):
        for key in ('formalDriver', 'launcher', 'sourceManifest', 'package', 'coreAudit'):
            bound(self.m[key])
        source_manifest = read(self.m['sourceManifest']['path'])
        source_root = Path(self.m['frozenSourceRoot'])
        for relative in self.m['requiredProductionSources']:
            require(sha(source_root / relative) == source_manifest[relative], '021 source binding differs: ' + relative)
        for item in self.m['directJavaSources']:
            bound(item)
            require(sha(item['historicalCompileInput']) == item['sha256'], 'Historical Java compile identity differs')
        history = read(bound(self.m['historicalFormalExecution']))
        require(history['scriptSha256'] == self.m['formalDriver']['sha256'], 'Formal entry does not match its execution history')
        original = Path(self.m['originalDistribution'])
        server = original / 'lib/iotdb-server-2.0.11-SNAPSHOT.jar'
        require(sha(server) == self.m['serverJarSha256'], '024d4 distribution server differs')
        with zipfile.ZipFile(self.m['package']['path']) as archive:
            entries = [name for name in archive.namelist() if name.endswith('/lib/' + server.name)]
            require(len(entries) == 1 and hashlib.sha256(archive.read(entries[0])).hexdigest()
                    == self.m['serverJarSha256'], 'Frozen package server entry differs')
            prefix = entries[0].rsplit("/lib/", 1)[0] + "/"
            packaged = [item for item in archive.infolist() if not item.is_dir()]
            require(all(item.filename.startswith(prefix) for item in packaged), "Unexpected package root")
            self.dist_snapshot = {}
            for item in packaged:
                relative = item.filename[len(prefix):]
                require(relative and not Path(relative).is_absolute() and ".." not in Path(relative).parts,
                        "Unsafe immutable package entry")
                require(relative not in self.dist_snapshot, "Duplicate immutable package file")
                self.dist_snapshot[relative] = hashlib.sha256(archive.read(item)).hexdigest()
        save(self.run / 'execution-before.json', {'manifest': self.m,
             'orchestrator': {'path': str(Path(__file__).resolve()), 'sha256': sha(__file__)},
             'branch': subprocess.check_output(['git', 'branch', '--show-current'], cwd='/root/iotdb', text=True).strip(),
             'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd='/root/iotdb', text=True).strip(),
             'python': sys.executable, 'pythonVersion': sys.version, 'sysPath': sys.path,
             'java': str(JAVA.resolve()), 'javaSha256': sha(JAVA),
             'javaVersion': subprocess.run([JAVA, '-version'], capture_output=True, text=True).stderr,
             'plan': 'ordinary SELECT substring(payload,1,8388608) AS payload FROM b6.oversize; public fixed DOP4 vs DOP1',
             'modelsWeights': 'Not applicable; Java IoTDB services only'})
        shutil.copytree(original, self.dist)
        copied_identity = distribution_identity(self.dist, self.dist_snapshot)
        require(copied_identity["status"] == "PASS", "Copied distribution differs from immutable package files")
        self.jars = sorted((self.dist / 'lib').glob('*.jar'))
        self.jar_sha = {str(path): self.dist_snapshot[str(path.relative_to(self.dist))] for path in self.jars}
        self.cp = str(self.classes) + ':' + ':'.join(map(str, self.jars))
        save(self.run / 'distribution-copy.json', {'original': str(original), 'copy': str(self.dist),
             'files': self.dist_snapshot, 'package': self.m['package'],
             'immutableOrigin': 'Every original ZIP file by SHA, with exact lib set',
             'copiedIdentity': copied_identity})
        self.launch('prepare', 'prepare')
        for side, node in NODES:
            path = self.cluster / side / node / 'conf/iotdb-system.properties'
            self.backups.append({'path': str(path), 'sha256': sha(path), 'mode': path.stat().st_mode & 0o777,
                                 'contentsBase64': base64.b64encode(path.read_bytes()).decode()})
            values = path.read_text()
            updates = {'degree_of_query_parallelism': '4' if side == 'candidate' else '1',
                       'enable_property_driven_planning': 'true', 'enable_dop_estimation': 'false',
                       'enable_table_group_by_hash_repartition': 'false', 'query_thread_count': '32',
                       'enable_timepartition_morsel': 'false', 'enable_ordered_parallel_scan': 'true',
                       'data_region_group_extension_policy': 'CUSTOM', 'default_data_region_group_num_per_database': '2',
                       'enable_query_memory_estimation': 'true',
                       'chunk_timeseriesmeta_free_memory_proportion': self.m['publicQueryMemoryProportion']}
            for key, value in updates.items():
                values = re.sub(r'^' + re.escape(key) + r'=.*\n?', '', values, flags=re.M)
                values += key + '=' + value + '\n'
            require('disk_space_warning_threshold' not in values, 'Default disk threshold must remain 0.05')
            path.write_text(values)
        self.manifest_backup = self.cluster / 'isolation-manifest.env'
        self.manifest_bytes = self.manifest_backup.read_bytes()
        self.manifest_mode = self.manifest_backup.stat().st_mode & 0o777
        save(self.run / 'six-config-backups.json', self.backups)
        save(self.run / 'active-configs.json', [{'path': str(self.cluster / side / node / 'conf/iotdb-system.properties'),
              'sha256': sha(self.cluster / side / node / 'conf/iotdb-system.properties'),
              'contents': (self.cluster / side / node / 'conf/iotdb-system.properties').read_text()}
             for side, node in NODES])
        self.command('compile-original-direct-entries', [sys.executable, bound(self.m['formalDriver']),
             '--root', self.run, '--distribution', self.dist, '--query-ports', ','.join(map(str, self.sql_ports.values())),
             '--jmx-ports', ','.join(map(str, self.jmx_ports)), '--phase', 'compile'], 300)
        for item in self.m['directJavaSources']:
            require(sha(self.run / Path(item['path']).name) == item['sha256'], 'Actual direct compiler source differs')
        source = HERE / 'BDataScanOversizeRpcProbe.java'
        shutil.copy2(source, self.run / source.name)
        self.command('compile-ordinary-statistics-probe', [JAVA.with_name('javac'), '-encoding', 'UTF-8',
             '-cp', self.cp, '-d', self.classes, self.run / source.name], 120)
        self.class_snapshot = {str(path): sha(path) for path in self.classes.glob('*.class')}
        save(self.run / 'actual-compiled-entries.json', self.class_snapshot)
        # Reuse only the already reviewed pure cleanup predicates, without executing old argv/phase.
        tree = ast.parse(Path(self.m['formalDriver']['path']).read_text())
        functions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                     and node.name in ('snapshots', 'terminal_problems')]
        require(len(functions) == 2, 'Original cleanup contract functions disappeared')
        namespace = {'collections': collections}
        exec(compile(ast.Module(body=functions, type_ignores=[]), self.m['formalDriver']['path'], 'exec'), namespace)
        self.terminal_problems = namespace['terminal_problems']

    def runtime_identity(self, label):
        records = []
        for side, node in NODES:
            base = self.cluster / side / node
            info = dict(line.split('=', 1) for line in (base / 'process.env').read_text().splitlines())
            pid = int(info['pid'])
            proc = Path('/proc') / str(pid)
            args = [arg.decode() for arg in (proc / 'cmdline').read_bytes().split(b'\0') if arg]
            require(info['class'] in args and any(arg.endswith('_CONF=' + str(base / 'conf')) for arg in args)
                    and any(arg.endswith('_HOME=' + str(self.dist)) for arg in args), 'Actual process is not this owned node')
            classpath = actual_classpath_identity(args[args.index('-cp') + 1], self.jars, self.jar_sha)
            require((proc / 'exe').resolve() == JAVA.resolve(), 'Actual service Java differs')
            stat = (proc / 'stat').read_text()
            start = stat[stat.rfind(')') + 2:].split()[19]
            maps = (proc / 'maps').read_text()
            (self.run / (label + '-' + side + '-' + node + '.maps')).write_text(maps)
            native = []
            for name in sorted({line.split()[-1] for line in maps.splitlines()
                                if len(line.split()) >= 6 and line.split()[-1].startswith('/') and '.so' in line.split()[-1]}):
                if Path(name).is_file():
                    native.append({'path': name, 'sha256': sha(name)})
            record = {'side': side, 'node': node, 'pid': pid, 'startTicks': start,
                      'class': info['class'], 'args': args, 'native': native, 'classpathIdentity': classpath,
                      'configSha256': sha(base / 'conf/iotdb-system.properties')}
            if label == 'after-cases' and node.startswith('datanode'):
                log = (base / 'logs/classload.log').read_text(errors='replace')
                loaded = [line for line in log.splitlines() if any(' ' + name + ' source:' in line for name in (
                    'org.apache.iotdb.db.queryengine.execution.memory.MemoryPool',
                    'org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager',
                    'org.apache.iotdb.db.queryengine.execution.driver.DataDriver',
                    'org.apache.iotdb.db.queryengine.execution.driver.Driver',
                    'org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator'))]
                require(loaded and all(str(self.dist / 'lib/iotdb-server-2.0.11-SNAPSHOT.jar') in line
                                       for line in loaded), 'Actual relevant loaded server source differs')
                record['actualLoadedServerClasses'] = loaded
            records.append(record)
        self.processes.extend(records)
        save(self.run / (label + '-loaded-runtime.json'), records)

    def load_fixture(self):
        for side in ('candidate', 'control'):
            self.sql(side + '-schema', side, 'CREATE DATABASE b6 WITH (max_data_region_group_num=2); USE b6; '
                     'CREATE TABLE oversize(device_id STRING TAG,payload STRING FIELD);')
            self.sql(side + '-small-row', side, "INSERT INTO b6.oversize(time,device_id,payload) VALUES (0,'huge','schema-ok')")
            self.sql(side + '-huge-row', side, "INSERT INTO b6.oversize(time,device_id,payload) VALUES (1,'huge','" + 'y' * HUGE + "'); FLUSH;")
            result = self.sql(side + '-same-column-schema-scan-success', side,
                              'SELECT substring(payload,1,8388608) AS payload FROM b6.oversize WHERE time=0')[0]
            require(result['rows'] == [['schema-ok']] and result['types'] == ['STRING'],
                    'Same table/column schema and ordinary data scan preflight did not pass')
            nodes = self.sql(side + '-actual-two-datanodes', side, 'SHOW DATANODES')[0]
            names = [name.lower().replace('_', '') for name in nodes['columns']]
            id_index = next(i for i, name in enumerate(names) if 'node' in name and 'id' in name)
            status_index = names.index('status')
            require(len(nodes['rows']) == 2 and len({row[id_index] for row in nodes['rows']}) == 2
                    and all(row[status_index] == 'Running' for row in nodes['rows']), 'Actual two running DataNodes missing')

    def error_case(self, side):
        state = self.run / (side + '-error-windows.json')
        self.jmx(side + '-error-window-open', 'open', state)
        self.open_windows.append((side, state))
        raw_final = None
        try:
            baseline = self.jmx(side + '-pool-baseline', 'read', state)
            budget = []
            for node in baseline:
                total = node['pool']['exchangeRemainingBytes']
                cap = total // 32
                require(1048576 <= cap < HUGE, 'Actual public memory budget cannot isolate a data-block oversize')
                budget.append({'jmxPort': node['port'], 'actualExchangeTotalBytes': total,
                               'actualComputedMaxBytesPerFI': cap, 'queryWorkers': 32})
            save(self.run / (side + '-real-memory-budget.json'), budget)
            sql = self.run / (side + '-oversize.sql')
            sql.write_text('EXPLAIN ANALYZE SELECT substring(payload,1,8388608) AS payload FROM b6.oversize WHERE time=1')
            ready = self.run / (side + '-oversize-result.json')
            self.java(side + '-actual-data-error-query', 'BHeldQueryProbe',
                      [str(self.sql_ports[side]), sql, ready, self.run / (side + '-unused-release'), 'error'], 300)
            error = read(str(ready) + '.terminal.json')
            require(error['status'] == 'QUERY_ERROR' and error['expectedErrorObserved'], 'Query did not fail at the real error path')
            server_log = self.dist / 'logs/log_datanode_all.log'
            capture = self.run / (side + '-memory-error-server-log-at-capture.log')
            capture.write_bytes(server_log.read_bytes())
            provenance = memory_failure_provenance(error['message'], capture.read_text(errors='replace'))
            provenance['serverLogCapture'] = {'path': str(capture), 'sha256': sha(capture)}
            save(self.run / (side + '-memory-error-provenance.json'), provenance)
            numeric = provenance['numericMemoryPoolFailure']
            require(any(item['actualComputedMaxBytesPerFI'] == numeric['actualCapBytes'] for item in budget),
                    'Error cap does not match actual configured pool/worker bound')
            last = None
            for sample in range(60):
                label = side + '-terminal-%03d' % sample
                data = self.jmx(label, 'read', state)
                problems = self.terminal_problems(data)
                identities = sorted((node['port'], item['queryId'], item['fragmentInstanceId'])
                                    for node in data for item in node['snapshots'])
                if not problems and identities == last:
                    raw_final = self.run / (label + '.json')
                    save(self.run / (side + '-independent-zero-terminal.json'), {'status': 'PASS',
                         'stableSamples': 2, 'data': data, 'identities': identities})
                    break
                last = identities if not problems else None
                time.sleep(0.25)
            require(raw_final is not None, 'Original B cleanup contract did not quiesce: ' + str(problems))
            rpc_path = self.run / (side + '-source-driver-terminal-rpc.json')
            self.java(side + '-fetch-source-driver-statistics', 'BDataScanOversizeRpcProbe', [raw_final, rpc_path], 120)
            rpc = read(rpc_path)
            responses = rpc['responses']
            available, unprofiled, started, scan_witnesses, emitted = profiled_terminal_statistics(responses, provenance)
            verdict = {'status': 'PASS', 'schemaSameTableColumnPreflightPassed': True,
                       'profiledDataQueryFailed': True, 'ordinarySmallDataQueryPassed': True,
                       'profiledTerminalStatisticsCount': len(available),
                       'unprofiledAuxiliaryRecords': [{'queryId': item['queryId'],
                           'fragmentInstanceId': item['fragmentInstanceId'], 'statusCode': item['statusCode'],
                           'validityReason': 'NOT_FOUND'} for item in unprofiled],
                       'numericMemoryPoolFailure': numeric, 'memoryFailureProvenance': provenance,
                       'actualStartedSourceFIs': [item['fragmentInstanceId'] for item in started],
                       'actualScanOperatorWitnesses': len(scan_witnesses), 'actualDriverEmissionWitnesses': len(emitted),
                       'sourceRpcPath': str(rpc_path), 'originalBTerminalContractPassed': True,
                       'publicQueryMemoryProportion': self.m['publicQueryMemoryProportion'],
                       'queryWorkers': 32, 'configuredDop': 4 if side == 'candidate' else 1}
            save(self.run / (side + '-oversize-verdict.json'), verdict)
            self.phases.append(side)
        finally:
            try:
                self.jmx(side + '-finally-original-observation', 'read', state)
            finally:
                self.jmx(side + '-error-window-close', 'close', state)
                self.open_windows.remove((side, state))

    def cleanup(self):
        errors = []
        for side, state in self.open_windows[:]:
            try:
                self.jmx(side + '-recovery-window-close', 'close', state)
            except BaseException as failure:
                errors.append('window ' + repr(failure))
        if (self.cluster / 'isolation-manifest.env').exists():
            try:
                self.launch('finally-stop-owned-services', 'stop')
            except BaseException as failure:
                errors.append('launcher stop ' + repr(failure))
        # Recover startup failures that happen before process.env. Exact own *_CONF only.
        leftovers = []
        for proc in Path('/proc').glob('[0-9]*'):
            try:
                args = [arg.decode() for arg in (proc / 'cmdline').read_bytes().split(b'\0') if arg]
                if any(arg.startswith(('-DIOTDB_CONF=', '-DCONFIGNODE_CONF='))
                       and arg.split('=', 1)[1].startswith(str(self.cluster) + '/') for arg in args):
                    require(any(name in args for name in ('org.apache.iotdb.db.service.DataNode',
                            'org.apache.iotdb.confignode.service.ConfigNode')), 'Own config belongs to unexpected process')
                    os.kill(int(proc.name), signal.SIGTERM)
                    leftovers.append(proc)
            except (FileNotFoundError, ProcessLookupError):
                pass
        time.sleep(0.3)
        for proc in leftovers:
            try:
                if (proc / 'cmdline').read_bytes():
                    os.kill(int(proc.name), signal.SIGKILL)
            except (FileNotFoundError, ProcessLookupError):
                pass
        for item in self.backups:
            path = Path(item['path'])
            path.write_bytes(base64.b64decode(item['contentsBase64']))
            path.chmod(item['mode'])
            if sha(path) != item['sha256'] or path.stat().st_mode & 0o777 != item['mode']:
                errors.append('config restore ' + str(path))
        if hasattr(self, 'manifest_bytes'):
            self.manifest_backup.write_bytes(self.manifest_bytes)
            self.manifest_backup.chmod(self.manifest_mode)
        _, occupied = listeners()
        if set(self.ports) & occupied:
            errors.append('owned ports remain ' + str(sorted(set(self.ports) & occupied)))
        pids = []
        for item in {item['pid']: item for item in self.processes}.values():
            proc = Path('/proc') / str(item['pid']) / 'cmdline'
            live = proc.exists() and bool(proc.read_bytes())
            pids.append({'pid': item['pid'], 'startTicks': item['startTicks'], 'stillLive': live})
            if live:
                errors.append('owned pid remains ' + str(item['pid']))
        distribution = (distribution_identity(self.dist, self.dist_snapshot)
                        if hasattr(self, "dist_snapshot") and self.dist.is_dir()
                        else {"status": "FAIL", "reason": "No immutable copied package baseline"})
        unchanged = distribution["status"] == "PASS"
        self.distribution_runtime_paths = set(distribution.get("addedFiles", [])) | set(
            distribution.get("packagedFilesChanged", []))
        if not unchanged:
            errors.append('copied distribution identity changed')
        save(self.run / "copied-distribution-final-identity.json", distribution)
        save(self.run / 'cleanup-six-configs-pids-ports.json', {'status': 'PASS' if not errors else 'FAIL',
             'errors': errors, 'restoredConfigs': len(self.backups), 'allOwnedPorts': self.ports,
             'remainingOwnedPorts': sorted(set(self.ports) & occupied), 'ownedPids': pids,
             'distributionUnchanged': unchanged, 'distributionIdentity': distribution, 'diskThresholdChanged': False})
        return errors

    def archive(self):
        # Original ZIP files reconstruct the immutable distribution. Added runtime logs and
        # any unexpected/changed distribution evidence are archived separately and preserved.
        paths = [path for path in sorted(self.ram.rglob('*')) if path.is_file()
                 and (not path.is_relative_to(self.dist)
                      or str(path.relative_to(self.dist)) in self.distribution_runtime_paths)]
        members = [{'path': str(path.relative_to(self.ram)), 'bytes': path.stat().st_size, 'sha256': sha(path)} for path in paths]
        save(self.evidence / 'archive-members.json', members)
        target = self.evidence / 'complete-raw-service-data-fixture.tar.gz'
        with tarfile.open(target, 'x:gz') as output:
            for path in paths:
                output.add(path, arcname=str(path.relative_to(self.ram)), recursive=False)
        actual = {}
        with tarfile.open(target, 'r|gz') as source:
            for item in source:
                require(item.isfile(), 'Unexpected non-file evidence archive member')
                digest = hashlib.sha256()
                stream = source.extractfile(item)
                for block in iter(lambda: stream.read(1048576), b''):
                    digest.update(block)
                actual[item.name] = digest.hexdigest()
        require(actual == {item['path']: item['sha256'] for item in members}, 'Raw archive round-trip SHA differs')
        require(target.stat().st_size <= self.m['persistentArchiveBudgetBytes'], 'Persistent evidence exceeded planned budget')
        save(self.evidence / 'archive-receipt.json', {'status': 'PASS', 'path': str(target), 'sha256': sha(target),
             'bytes': target.stat().st_size, 'members': len(members), 'allMembersDecompressedAndShaChecked': True,
             'ramOriginalsPreserved': True, 'copiedDistributionReconstruction': self.m['package']})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--ram', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    require(not args.ram.exists() and not args.evidence.exists(), 'Every round needs new RAM/evidence roots')
    require(subprocess.check_output(['stat', '-f', '-c', '%T', str(args.ram.parent)], text=True).strip() == 'tmpfs',
            'Services/data/dist/large fixture must be in RAM')
    m = read(args.manifest)
    require(m['bindingState'] == 'BOUND_CANONICAL_RESOURCE_ACCOUNTING_FIX_FOR_RUNTIME',
            '021 is tool-debug-only; bind the new canonical resource-accounting package before services')
    accounting = read(bound(m['canonicalResourceAccountingAcceptance']))
    require(accounting['status'] == 'PASS'
            and accounting['sourceManifestSha256'] == m['sourceManifest']['sha256']
            and accounting['serverJarSha256'] == m['serverJarSha256']
            and accounting['duplicateLocalWrapperAccountingRejected'] is True,
            'New package canonical identity/count/lifecycle regression acceptance missing')
    require(shutil.disk_usage(args.ram.parent).free >= 1024 * 1024 * 1024, 'Need >=1GiB free tmpfs')
    require(shutil.disk_usage(args.evidence.parent).free >= m['persistentMinimumFreeBytes'], 'Need persistent evidence headroom')
    args.ram.mkdir()
    args.evidence.mkdir()
    shutil.copy2(args.manifest, args.evidence / 'bound-manifest.json')
    shutil.copy2(Path(__file__), args.evidence / Path(__file__).name)
    shutil.copy2(HERE / 'BDataScanOversizeRpcProbe.java', args.evidence / 'BDataScanOversizeRpcProbe.java')
    runtime = Runtime(m, args.ram.resolve(), args.evidence.resolve())
    failure, cleanup_errors, archive_error = None, [], None
    try:
        runtime.initial_bind()
        runtime.launch('start-fresh-owned-services', 'start')
        runtime.runtime_identity('startup')
        runtime.load_fixture()
        runtime.error_case('candidate')
        runtime.error_case('control')
        runtime.runtime_identity('after-cases')
    except BaseException as error:
        failure = {'type': type(error).__name__, 'message': str(error), 'traceback': traceback.format_exc()}
        save(runtime.run / 'failure.json', failure)
    finally:
        try:
            cleanup_errors = runtime.cleanup()
        except BaseException as error:
            cleanup_errors.append(repr(error))
        save(runtime.run / 'runtime-verdict.json', {'status': 'PASS' if not failure and not cleanup_errors else 'FAIL',
             'completedSides': runtime.phases, 'failure': failure, 'cleanupErrors': cleanup_errors})
        try:
            runtime.archive()
        except BaseException as error:
            archive_error = repr(error)
        for name in ('runtime-verdict.json', 'failure.json', 'port-binding.json', 'execution-before.json',
                     'cleanup-six-configs-pids-ports.json', 'candidate-oversize-verdict.json', 'control-oversize-verdict.json',
                     'candidate-oversize-result.json.terminal.json', 'control-oversize-result.json.terminal.json',
                     'candidate-independent-zero-terminal.json', 'control-independent-zero-terminal.json',
                     'candidate-source-driver-terminal-rpc.json', 'control-source-driver-terminal-rpc.json'):
            if (runtime.run / name).is_file():
                shutil.copy2(runtime.run / name, args.evidence / name)
        final = {'status': 'PASS' if not failure and not cleanup_errors and not archive_error else 'FAIL',
                 'failure': failure, 'cleanupErrors': cleanup_errors, 'archiveFailure': archive_error,
                 'completedSides': runtime.phases, 'scope': 'REAL_DATA_SCAN_SINGLE_BLOCK_OVERSIZE_AND_ORIGINAL_BC_CLEANUP',
                 'sourceManifest': m['sourceManifest'], 'serverJarSha256': m['serverJarSha256'],
                 'sixConfigOriginalModesAndShaRestored': len(runtime.backups) == 6 and not cleanup_errors,
                 'diskThresholdChanged': False, 'ramOriginals': str(args.ram), 'evidence': str(args.evidence)}
        save(args.evidence / 'acceptance-final.json', final)
        print(json.dumps(final), flush=True)
    require(final['status'] == 'PASS', 'Real data-scan oversize acceptance failed; all original evidence preserved')


if __name__ == '__main__':
    main()
