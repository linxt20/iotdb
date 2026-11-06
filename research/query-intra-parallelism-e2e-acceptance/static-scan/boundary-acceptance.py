#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""B6 boundary cases using the existing A6SqlProbe and isolated service binding."""

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import traceback

HERE = Path(__file__).resolve().parent
JAVA = Path('/usr/lib/jvm/java-17-openjdk-amd64/bin/java')
JAVAC = JAVA.with_name('javac')
PROBE_SOURCE = Path('/root/property-d-artifacts/AD-converged-runtime-20261103-r2/A6SqlProbe.java')
PROBE_SHA = 'abda71aa3fb5e01b5d9bcc8ec7647ba4668f36852d8e22b3905424a531d0b690'
OLD_MANIFEST = '021fdefeb3d69ffe998f9d55d6d13af9ab7719df8dbef60c83098ddd38cb500d'
OLD_SERVER = '24d4b440a195109623d8ac2ee80bb54b9df602f0b11e90999cab211cbfe25733'
SIDES = ('candidate', 'control')


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def save(path, data):
    path = Path(path)
    with path.open('x') as out:
        json.dump(data, out, ensure_ascii=False, indent=2, allow_nan=False)
        out.write('\n')


def read(path):
    return json.loads(Path(path).read_text())


def canonical(rows):
    return ''.join(x + '\n' for x in sorted(
        json.dumps(row, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
        for row in rows))


def walk(tree):
    if isinstance(tree, dict):
        yield tree
        for child in tree.get('children', []):
            yield from walk(child)


def fixture_rows(spec):
    """Generate literal input rows; no IoTDB or planner helper is imported."""
    interval = spec['partitionIntervalMillis']
    main = []
    for device in range(32):
        for index, when in enumerate([-interval-1, -interval, -interval+1, -1,
                                      0, 1, interval-1, interval, interval+1]):
            value = 100 * device + index
            main.append([f'd{device:02}', when, value,
                         None if (device+index) % 4 == 0 else value + 0.5])
    single = [[f'd{d:02}', t, d*10+t] for d in range(32) for t in [1, 2, 3, 4]]
    ties = [[None if d < 8 else 'g'+str((d-8)//8), f'v{d:02}', t, 7]
            for d in range(32) for t in [-interval, 0, interval]]
    diff = [[f'd{d:02}', t, 7] for d in range(32)
            for t in [-interval, -1, 0, 1, interval]]
    skew = []
    for device in range(32):
        for partition in range(4):
            for index in range(512 if partition == 0 else 8):
                when = partition*interval + device*1024 + index
                value = device*100000 + partition*10000 + index
                payload = hashlib.sha256(f'{device}/{partition}/{index}'.encode()).hexdigest()
                skew.append([f'd{device:02}', when, value, payload])
    return {'main': main, 'singlepart': single, 'ties': ties, 'diff': diff, 'skew': skew}


def oracle(spec):
    """Evaluate finite fixture relations independently of actual service output."""
    rows = fixture_rows(spec)
    interval = spec['partitionIntervalMillis']
    main = rows['main']
    ordered_time = sorted(main, key=lambda r: (r[1], r[0]))
    ordered_device = sorted(main, key=lambda r: (r[0], r[1]))
    tied = [[r[0], r[2]] for r in rows['ties']]
    tied.sort(key=lambda r: (r[0] is not None, r[0] or '', -r[1]))
    return {
        'single-device': [r for r in main if r[0] == 'd00'],
        'single-partition': rows['singlepart'],
        'negative-boundaries': main,
        'adjacent-half-open': [r for r in main if interval-1 <= r[1] < interval+1],
        'nonempty-time-filter': [r for r in main if -interval <= r[1] < interval],
        'device-limit': ordered_time[:11],
        'global-limit-offset': ordered_device[3:14],
        'duplicate-keys-nulls': tied,
        # All input values equal7: first diff is null, every subsequent diff is0.
        # The predicate includes both values, so the oracle is independent of stream order.
        'filter-diff': rows['diff'],
        'skewed-partitions': [[r[0], r[1], r[2]] for r in rows['skew']],
    }


def sql_literal(value):
    if value is None:
        return 'null'
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def fixture_sql(spec):
    out = []
    layouts = {
        'main': ('device_id STRING TAG,s1 INT64 FIELD,s2 DOUBLE FIELD',
                 'device_id,time,s1,s2', lambda row: row),
        'singlepart': ('device_id STRING TAG,s1 INT64 FIELD',
                       'device_id,time,s1', lambda row: row),
        'ties': ('group_tag STRING TAG,variant STRING TAG,s1 INT64 FIELD',
                 'group_tag,variant,time,s1', lambda row: row),
        'diff': ('device_id STRING TAG,s1 INT64 FIELD',
                 'device_id,time,s1', lambda row: row),
        'skew': ('device_id STRING TAG,s1 INT64 FIELD,payload STRING FIELD',
                 'device_id,time,s1,payload', lambda row: row),
    }
    for name, rows in fixture_rows(spec).items():
        columns, inputs, convert = layouts[name]
        # A single data region keeps diff's Filter directly above its Scan so
        # the actual local non-fusion guard is exercised. Other fixtures retain
        # two data regions, with CUSTOM extension and a cluster default of one.
        groups = 1 if name == 'diff' else 2
        out.append(f'CREATE DATABASE b6_boundary_{name} WITH (max_data_region_group_num={groups});')
        out.append(f'USE b6_boundary_{name};CREATE TABLE telemetry({columns});')
        for start in range(0, len(rows), 512):
            values = ','.join('('+','.join(map(sql_literal, convert(row)))+')'
                              for row in rows[start:start+512])
            out.append(f'INSERT INTO telemetry({inputs}) VALUES {values};')
        out.append('FLUSH;')
    license_lines = Path(__file__).read_text().splitlines()[1:17]
    return '\n'.join('--'+line[1:] for line in license_lines) + '\n' + '\n'.join(out) + '\n'


def verify_result(case, result, expected):
    assert result['columns'] == case['columns'], (case['id'], 'columns', result['columns'])
    assert result['types'] == case['types'], (case['id'], 'types', result['types'])
    actual = result['rows']
    assert canonical(actual) == canonical(expected), (case['id'], 'complete typed multiset')
    if case.get('ordered'):
        assert actual == expected, (case['id'], 'complete output sequence')
    return {'rows': len(actual), 'fullTypedMultisetCompared': True,
            'orderedSequenceCompared': bool(case.get('ordered')),
            'resultSha256': hashlib.sha256(canonical(actual).encode()).hexdigest()}


def registered_nodes(result, side_binding):
    columns = result['columns']
    rows = [dict(zip(columns, row)) for row in result['rows']]
    assert len(rows) == 2 and len({row['NodeID'] for row in rows}) == 2
    assert all(row['Status'] == 'Running' for row in rows)
    bindings = {node['queryPort']: node for node in side_binding['nodes']}
    assert set(bindings) == {row['RpcPort'] for row in rows}
    return {str(row['NodeID']): bindings[row['RpcPort']] for row in rows}


def verify_runtime(case, side, doc, registered):
    fragments = doc['fragmentInstances']
    assert doc['fragmentInstancesCount'] == len(fragments) and fragments
    ids = [f['id'] for f in fragments]
    assert len(ids) == len(set(ids)), 'duplicate FI identity'
    all_work, all_drivers, managed = [], [], []
    for fragment in fragments:
        snap = fragment['runtimeSnapshot']
        node = registered[str(snap['sourceNodeId'])]
        assert fragment['ip'] == snap['sourceEndpoint'] == node['internalEndpoint']
        assert snap['isTerminal'] and snap['generation'] > 0
        assert snap['checksum'] and snap['planIdentityChecksum']
        assert snap['admissionStatus'] == 'ADMISSION_ACCEPTED'
        for key in ['resourceObjectCount', 'resourceUniqueMemoryKeyCount', 'resourceNominalShareBytes']:
            assert snap[key] > 0, (fragment['id'], 'missing actual resource field', key)
        drivers = snap['drivers']
        assert drivers and snap['actualPipelineCount'] == len(drivers)
        for driver in drivers:
            assert driver['fragmentInstanceId'] == fragment['id']
            assert driver['driverSampleGeneration'] == snap['generation']
            assert driver['driverTaskId'] == fragment['id']+'.'+driver['driverPipelineId']
            all_drivers.append(driver['driverTaskId'])
        for key in ['workUnitIds', 'bucketIds', 'exchangeEdgeIds']:
            assert len(snap[key]) == len(set(snap[key])), (fragment['id'], key, 'duplicate')
        all_work.extend(snap['workUnitIds'])
        if snap['splitMode'] == 'NO_MANAGED_SCAN':
            assert snap['plannedDop'] == 0 and not snap['workUnitIds'] and not snap['bucketIds']
        else:
            managed.append(fragment)
            assert snap['plannedDop'] >= 1 and snap['workUnitIds'] and snap['bucketIds']
            if side == 'control':
                assert snap['plannedDop'] == 1, 'control actual scan DOP is not1'
    assert managed and len(all_work) == len(set(all_work))
    assert len(all_drivers) == len(set(all_drivers))
    if side == 'candidate':
        expected_mode = case.get('candidateMode')
        if expected_mode:
            assert all(f['runtimeSnapshot']['splitMode'] == expected_mode for f in managed), case['id']
        if case.get('candidateReason'):
            assert all(f['runtimeSnapshot']['splitReason'] == case['candidateReason'] for f in managed)
        if case.get('candidateParallel'):
            assert all(f['runtimeSnapshot']['plannedDop'] > 1 for f in managed)
    return managed


def verify_plan(case, plan):
    scans = [node for node in walk(plan) if node.get('name', '').startswith('DeviceTableScanNode-')]
    assert scans, (case['id'], 'no actual scan in plan')
    if case['id'] in ['device-limit', 'global-limit-offset']:
        expected = 'true' if case['id'] == 'device-limit' else 'false'
        for node in scans:
            properties = node['properties']
            assert int(properties['PushDownLimit']) > 0
            assert properties['PushDownLimitToEachDevice'] == expected
    if case['id'] == 'global-limit-offset':
        assert any(node.get('name', '').startswith('OffsetNode-') and
                   int(node['properties']['Count']) == 3 for node in walk(plan))
    if case['id'] == 'filter-diff':
        direct = [node for node in walk(plan) if node.get('name', '').startswith('FilterNode-')
                  and 'diff(' in node.get('properties', {}).get('Predicate', '').lower()
                  and any(child.get('name', '').startswith('DeviceTableScanNode-')
                          for child in node.get('children', []))]
        assert direct, 'diff Filter must directly retain a DeviceTableScan child'
    return {'scanNodeIds': [node['id'] for node in scans], 'planVerified': True}


def verify_diff_placement(managed):
    for fragment in managed:
        filters = [op for op in walk(fragment['operators'])
                   if op.get('operatorType') == 'FilterAndProjectOperator']
        assert filters, 'diff filter missing from managed root operator tree'
        assert all(op.get('operatorType') != 'FilterAndProjectOperator'
                   for pipeline in fragment.get('parallelPipelines', []) for op in walk(pipeline)), \
            'diff filter was fused into a parallel morsel'


def properties(path):
    found = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            found[key.strip()] = value.strip()
    return found


def receipt_reference(reference):
    path = Path(reference['path']).resolve()
    assert sha(path) == reference['sha256'], ('receipt changed', str(path))
    return read(path)


def preflight(binding_path, spec):
    binding = read(binding_path)
    assert binding['status'] == 'READY_B4_REPAIRED_BOUNDARY'
    source = binding['sourceManifest']
    assert source['sha256'] != OLD_MANIFEST, 'old B4-defective source is blocked'
    assert sha(source['path']) == source['sha256']
    dist = Path(binding['distribution']).resolve()
    assert binding['serverJarSha256'] != OLD_SERVER, 'old B4-defective service package is blocked'
    assert sha(dist/'lib/iotdb-server-2.0.11-SNAPSHOT.jar') == binding['serverJarSha256']
    assert sha(dist/'lib/iotdb-thrift-2.0.11-SNAPSHOT.jar') == binding['protocolJarSha256']
    for key in ['serviceIdentityReceipt', 'b4RepairReceipt']:
        receipt = receipt_reference(binding[key])
        assert receipt['status'] == 'PASS'
        assert receipt['sourceManifestSha256'] == source['sha256']
        if key == 'serviceIdentityReceipt':
            assert receipt['serverJarSha256'] == binding['serverJarSha256']
    nodes = []
    for side in SIDES:
        side_binding = binding['sides'][side]
        assert len(side_binding['nodes']) == 2
        assert len({node['pid'] for node in side_binding['nodes']}) == 2
        for node in side_binding['nodes']:
            proc = Path('/proc')/str(node['pid'])
            command = (proc/'cmdline').read_bytes().replace(b'\0', b' ').decode()
            assert 'org.apache.iotdb.db.service.DataNode' in command
            config = Path(node['config']).resolve()
            actual = properties(config)
            expected = {
                'degree_of_query_parallelism': '4' if side == 'candidate' else '1',
                'enable_timepartition_morsel': 'true',
                'enable_timepartition_morsel_size_weighting': 'true',
                'enable_ordered_parallel_scan': 'true',
                'time_partition_interval': str(spec['partitionIntervalMillis']),
                'time_partition_origin': '0', 'timestamp_precision': 'ms',
            }
            assert all(actual.get(key) == value for key, value in expected.items()), \
                (side, str(config), 'effective configuration must be explicit', expected)
            assert all(Path(directory).is_dir() for directory in node['dataDirectories'])
            nodes.append({'side': side, 'pid': node['pid'], 'command': command,
                          'config': str(config), 'configSha256': sha(config),
                          'procStartTicks': (proc/'stat').read_text().split()[21]})
    assert len({node['pid'] for node in nodes}) == 4
    return binding, dist, nodes


def files_by_partition(binding, side, database):
    found, seen = [], set()
    for node in binding['sides'][side]['nodes']:
        for directory in node['dataDirectories']:
            for path in Path(directory).rglob('*.tsfile'):
                path = path.resolve()
                if path in seen or not any(p in [database, 'root.'+database] for p in path.parts):
                    continue
                seen.add(path)
                found.append({'path': str(path), 'region': path.parent.parent.name,
                              'partition': int(path.parent.name), 'queryPort': node['queryPort'],
                              'size': path.stat().st_size, 'sha256': sha(path)})
    assert found, ('no flushed files', side, database)
    return sorted(found, key=lambda item: item['path'])


def verify_metadata(metadata, side, managed=None):
    single = metadata[side]['singlepart']
    assert {file['partition'] for file in single} == {0}, 'single-partition DB polluted'
    skew = metadata[side]['skew']
    groups = defaultdict(lambda: defaultdict(int))
    for file in skew:
        groups[(file['queryPort'], file['region'])][file['partition']] += file['size']
    assert groups
    for key, sizes in groups.items():
        assert set(sizes) == {0, 1, 2, 3}, (side, key, sizes)
        assert min(sizes.values()) > 0 and max(sizes.values()) >= 4*min(sizes.values()), \
            ('actual TsFile bytes not skewed', side, key, sizes)
    if managed is not None:
        registered = {str(node['internalEndpoint']): node for node in metadata[side]['nodes']}
        for fragment in managed:
            node = registered[fragment['ip']]
            sizes = groups[(node['queryPort'], str(fragment['dataRegion']))]
            pipelines = fragment['parallelPipelines']
            diagnostics = [op['specifiedInfo'] for pipeline in pipelines for op in walk(pipeline)
                           if 'MORSEL_SCHEDULING' in op.get('specifiedInfo', {})]
            assert len(diagnostics) == fragment['runtimeSnapshot']['plannedDop']
            covered = set()
            for diag in diagnostics:
                assert diag['MORSEL_SCHEDULING'] == 'LPT_TSFILE_BYTES'
                partitions = json.loads(diag['MORSEL_TIME_PARTITIONS'])
                assert partitions and len(partitions) == len(set(partitions))
                assert int(diag['MORSEL_ESTIMATED_TSFILE_BYTES']) == sum(sizes[p] for p in partitions)
                covered.update(partitions)
            assert covered == {0, 1, 2, 3}


def run_command(directory, label, command, timeout=300):
    save(directory/(label+'.before.json'), {'command': list(map(str, command)),
                                          'cwd': str(directory), 'python': sys.executable,
                                          'sysPath': sys.path})
    with (directory/(label+'.log')).open('x') as out:
        result = subprocess.run(list(map(str, command)), cwd=directory, stdout=out,
                                stderr=subprocess.STDOUT, timeout=timeout)
    (directory/(label+'.exit')).write_text(str(result.returncode)+'\n')
    result.check_returncode()


def executable_sql(text):
    # A6SqlProbe splits input on semicolons, including semicolons inside the
    # Apache license. Keep the archived fixture intact and remove only whole
    # comment lines from the separately recorded executable SQL input.
    return '\n'.join(line for line in text.splitlines()
                     if not line.lstrip().startswith('--'))+'\n'


def sql(directory, label, side, text, binding, dist, compiled):
    inp, out = directory/(label+'.sql'), directory/(label+'.json')
    with inp.open('x') as source:
        source.write(executable_sql(text))
    cp = str(compiled/'classes')+':'+':'.join(str(p) for p in sorted((dist/'lib').glob('*.jar')))
    save(directory/(label+'.identity.json'), {'sqlSha256': sha(inp),
                                            'originalSqlSha256': hashlib.sha256(text.encode()).hexdigest(),
                                            'wholeLineCommentsRemovedForProbe': True,
                                            'driver': str(compiled/'classes/A6SqlProbe.class'),
                                            'driverSha256': sha(compiled/'classes/A6SqlProbe.class'),
                                            'classpath': cp})
    run_command(directory, label, [JAVA, '-Xmx512m', '-cp', cp, 'A6SqlProbe',
                                   binding['sides'][side]['queryPort'], inp, out], 360)
    data = read(out)
    for klass in data['loadedJdbcClasses']:
        assert str(dist/'lib') in klass['codeSource'], ('unexpected JDBC runtime class', klass)
        assert klass['classSha256']
    return data['results']


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', required=True, choices=['prepare', 'compile', 'fixture', 'run'])
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--binding', type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    output = root/args.phase
    output.mkdir(exist_ok=False)
    spec = read(HERE/'boundary-fixture.json')
    manifest = read(HERE/'boundary-manifest.json')
    file_identity = {str(path): sha(path) for path in [Path(__file__).resolve(),
                     HERE/'boundary-fixture.json', HERE/'boundary-manifest.json', HERE/'boundary-fixture.sql']}
    before = {'phase': args.phase, 'script': str(Path(__file__).resolve()), 'files': file_identity,
              'python': sys.executable, 'pythonVersion': sys.version, 'sysPath': sys.path,
              'branch': subprocess.check_output(['git', '-C', '/root/iotdb', 'branch', '--show-current'], text=True).strip(),
              'head': subprocess.check_output(['git', '-C', '/root/iotdb', 'rev-parse', 'HEAD'], text=True).strip(),
              'manifestStatus': manifest['status']}
    assert before['branch'] == 'feat/next-p0' and before['head'] == manifest['baselineHead']
    if args.phase == 'prepare':
        assert fixture_sql(spec) == (HERE/'boundary-fixture.sql').read_text()
        expected = oracle(spec)
        save(output/'execution-before.json', before)
        save(output/'expected-results.json', expected)
        for filename in ['boundary-manifest.json', 'boundary-fixture.json', 'boundary-fixture.sql']:
            shutil.copy2(HERE/filename, output/filename)
        save(output/'verdict.json', {'status': 'PREPARED_RUNTIME_PENDING',
                                     'cases': len(manifest['cases']),
                                     'expectedRows': {key: len(value) for key, value in expected.items()}})
        return
    assert args.binding, '--binding is required for a compiled or service phase'
    binding, dist, nodes = preflight(args.binding, spec)
    before.update(binding=str(args.binding.resolve()), bindingSha256=sha(args.binding),
                  nodes=nodes, java=str(JAVA), javaSha256=sha(JAVA),
                  jars={str(path): sha(path) for path in sorted((dist/'lib').glob('*.jar'))})
    save(output/'execution-before.json', before)
    if args.phase == 'compile':
        assert sha(PROBE_SOURCE) == PROBE_SHA
        original_probe = PROBE_SOURCE.read_text()
        timestamp_read = 'Object value = rs.getObject(i);'
        assert original_probe.count(timestamp_read) == 1
        actual_probe = original_probe.replace(timestamp_read,
            'Object value = md.getColumnTypeName(i).equals("TIMESTAMP") '
            '? Long.valueOf(rs.getLong(i)) : rs.getObject(i);')
        (output/'A6SqlProbe.java').write_text(actual_probe)
        save(output/'probe-source-overlay.json', {
            'originalSource': str(PROBE_SOURCE), 'originalSourceSha256': PROBE_SHA,
            'actualSource': str(output/'A6SqlProbe.java'),
            'actualSourceSha256': sha(output/'A6SqlProbe.java'),
            'change': 'Read TIMESTAMP as raw epoch milliseconds through JDBC getLong; preserve negative timestamps without the existing getObject Timestamp nanos conversion',
            'otherColumnsAndNullHandling': 'Original probe unchanged',
            'productionSourceOrJarChanged': False})
        (output/'classes').mkdir()
        run_command(output, 'javac', [JAVAC, '-cp', str(dist/'lib/*'), '-d', output/'classes', output/'A6SqlProbe.java'])
        save(output/'verdict.json', {'status':'COMPILED_RUNTIME_PENDING', 'probeSource': str(PROBE_SOURCE),
                                     'probeSourceSha256': sha(PROBE_SOURCE),
                                     'actualProbeSource': str(output/'A6SqlProbe.java'),
                                     'actualProbeSourceSha256': sha(output/'A6SqlProbe.java'),
                                     'probeClassSha256': sha(output/'classes/A6SqlProbe.class')})
        return
    compiled = root/'compile'
    assert read(compiled/'verdict.json')['probeClassSha256'] == sha(compiled/'classes/A6SqlProbe.class')
    assert read(compiled/'execution-before.json')['bindingSha256'] == sha(args.binding), \
        'service/package binding changed after probe compilation'
    prepared = root/'prepare'
    assert read(prepared/'execution-before.json')['files'] == file_identity, \
        'tool identity changed after preparation'
    for name in ['boundary-manifest.json', 'boundary-fixture.json', 'boundary-fixture.sql']:
        assert sha(prepared/name) == sha(HERE/name), ('tool changed after prepare', name)
    if args.phase == 'fixture':
        for side in SIDES:
            sql(output, side, side, (prepared/'boundary-fixture.sql').read_text(), binding, dist, compiled)
        save(output/'verdict.json', {'status':'FIXTURE_LOADED_RUNTIME_PENDING',
                                     'fixtureSha256': sha(prepared/'boundary-fixture.sql')})
        return
    assert read(root/'fixture/verdict.json')['status'] == 'FIXTURE_LOADED_RUNTIME_PENDING'
    expected = read(prepared/'expected-results.json')
    assert expected == oracle(spec), 'prepared independent oracle changed'
    registered, metadata = {}, {}
    for side in SIDES:
        topology = sql(output, side+'-topology', side, 'SHOW DATANODES;SHOW REGIONS;', binding, dist, compiled)
        registered[side] = registered_nodes(topology[0], binding['sides'][side])
        metadata[side] = {name: files_by_partition(binding, side, 'b6_boundary_'+name)
                          for name in ['main', 'singlepart', 'skew']}
        metadata[side]['nodes'] = binding['sides'][side]['nodes']
        verify_metadata(metadata, side)
    save(output/'physical-files-before.json', metadata)
    receipts = []
    for repeat in range(manifest['repeats']):
        for case in manifest['cases']:
            results = {}
            for side in SIDES:
                label = f'{case["id"]}-{side}-{repeat}'
                query = case['sql']
                data = sql(output, label, side, query+';EXPLAIN (FORMAT JSON) '+query+
                           ';EXPLAIN ANALYZE VERBOSE (FORMAT JSON) '+query+';', binding, dist, compiled)
                assert len(data) == 3
                result = verify_result(case, data[0], expected[case['id']])
                assert len(data[1]['rows']) == len(data[2]['rows']) == 1
                plan = json.loads(data[1]['rows'][0][0])
                runtime = json.loads(data[2]['rows'][0][0])
                save(output/(label+'.plan.json'), plan)
                save(output/(label+'.runtime.json'), runtime)
                plan_receipt = verify_plan(case, plan)
                managed = verify_runtime(case, side, runtime, registered[side])
                if case['id'] == 'filter-diff':
                    verify_diff_placement(managed)
                if case['id'] == 'skewed-partitions' and side == 'candidate':
                    verify_metadata(metadata, side, managed)
                results[side] = data[0]
                receipts.append(dict(side=side, case=case['id'], repeat=repeat,
                                     fragmentIds=[f['id'] for f in runtime['fragmentInstances']],
                                     actualDataNodeIds=sorted({str(f['runtimeSnapshot']['sourceNodeId'])
                                                              for f in runtime['fragmentInstances']}),
                                     **result, **plan_receipt))
            assert results['candidate']['columns'] == results['control']['columns']
            assert results['candidate']['types'] == results['control']['types']
            assert canonical(results['candidate']['rows']) == canonical(results['control']['rows'])
            if case.get('ordered'):
                assert results['candidate']['rows'] == results['control']['rows']
    for side in SIDES:
        for name in ['main', 'singlepart', 'skew']:
            assert files_by_partition(binding, side, 'b6_boundary_'+name) == metadata[side][name], \
                ('physical metadata changed during acceptance', side, name)
    _, _, after_nodes = preflight(args.binding, spec)
    assert after_nodes == nodes, 'bound processes/configuration changed during acceptance'
    save(output/'verdict.json', {'status':'PASS', 'scope':'B6 boundary business, plan and actual runtime shapes only',
                                 'sourceManifestSha256':binding['sourceManifest']['sha256'],
                                 'serverJarSha256':binding['serverJarSha256'],
                                 'cases':len(manifest['cases']), 'runs':len(receipts),
                                 'candidateScanDop':4,'controlActualManagedScanDop':1,
                                 'receipts':receipts,'performanceClaim':False,
                                 'singleSource32PressureCovered':False,'dataScanOversizeCovered':False})


if __name__ == '__main__':
    try:
        main()
    except Exception as failure:
        # Preserve an explicit failed attempt without reusing or overwriting any prior output.
        if '--root' in sys.argv and '--phase' in sys.argv:
            failure_root = Path(sys.argv[sys.argv.index('--root')+1]).resolve()
            failure_phase = sys.argv[sys.argv.index('--phase')+1]
            directory = failure_root/failure_phase
            if directory.is_dir() and not (directory/'failure.json').exists():
                save(directory/'failure.json', {'status':'FAIL', 'type':type(failure).__name__,
                                               'message':str(failure), 'traceback':traceback.format_exc(),
                                               'argv':sys.argv, 'scriptSha256':sha(__file__)})
        raise
