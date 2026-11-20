#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Convert UCI321 and independently derive exact oracles from its original wide ZIP."""
import argparse
import calendar
import csv
import datetime
import decimal
import hashlib
import io
import json
import math
from pathlib import Path
import re
import struct
import subprocess
import sys
import zipfile
import zlib

EXPECTED = 'f6c4d0e0df12ecdb9ea008dd6eef3518adb52c559d04a9bac2e1b81dcfc8d4e1'
MEMBER = 'LD2011_2014.txt'
ROWS = 140256
CLIENTS = ['MT_%03d' % i for i in range(1, 371)]
SCALES = {35040: 'one-year', 70176: 'two-years', ROWS: 'four-years'}
START_MS = 1293840000000
DATABASE = 'uci_electricity_v4'
TABLE = 'electricity_nominal'


def sha(p):
    h = hashlib.sha256()
    with Path(p).open('rb') as f:
        for data in iter(lambda: f.read(4 * 1024 * 1024), b''):
            h.update(data)
    return h.hexdigest()


def write(p, value):
    p.parent.mkdir(parents=True, exist_ok=True)
    assert not p.exists(), str(p)
    p.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n')


def identity(source, out, mode):
    assert sha(source) == EXPECTED
    entry = Path(__file__).resolve()
    repo = entry.parents[3]
    git = lambda *a: subprocess.check_output(['git', *a], cwd=repo, text=True).strip()
    metadata = source.parent / 'official-metadata.json'
    assert metadata.exists()
    write(out / 'preexecution-identity.json', {'entry': str(entry), 'entrySha256': sha(entry), 'mode': mode,
        'repository': str(repo), 'branch': git('branch', '--show-current'), 'baseCommit': git('rev-parse', 'HEAD'),
        'sourceArchive': str(source), 'sourceArchiveSha256': EXPECTED, 'metadataSha256': sha(metadata),
        'python': str(Path(sys.executable).resolve()), 'pythonSha256': sha(Path(sys.executable).resolve()),
        'zipfileModule': str(Path(zipfile.__file__).resolve()), 'zipfileSha256': sha(Path(zipfile.__file__).resolve()),
        'zlibExtension': zlib.__file__, 'zlibExtensionSha256': sha(zlib.__file__), 'zlibRuntime': zlib.ZLIB_RUNTIME_VERSION,
        'schema': ['time TIMESTAMP TIME', 'client_id STRING TAG', 'power_kw DOUBLE FIELD', 'source_row_no INT64 FIELD', 'record_present BOOLEAN FIELD'],
        'semantics': 'Preserve all zeros and kW; source wall-time label assigned nominal UTC without claiming actual UTC/DST reconstruction.',
        'fingerprint': 'UTF-8 time_ms TAB client_id TAB 16 lowercase IEEE754 binary64 raw-bit hex TAB source_row_no TAB true LF'})


def convert(source, out):
    csv_root = out / 'csv'
    csv_root.mkdir()
    digests = [hashlib.sha256() for _ in CLIENTS]
    files = [csv_root / (c + '.csv') for c in CLIENTS]
    streams = [p.open('x', encoding='ascii', newline='\n', buffering=1024 * 1024) for p in files]
    zeros = [0] * len(CLIENTS)
    negative_zero = [0] * len(CLIENTS)
    pattern = re.compile(r'[+-]?\d+(?:[.,]\d+)?(?:[eE][+-]?\d+)?\Z')
    count = 0
    prefix = {}
    try:
        for f in streams:
            f.write('time,client_id,power_kw,source_row_no,record_present\n')
        with zipfile.ZipFile(source) as z, z.open(MEMBER) as raw:
            reader = io.TextIOWrapper(raw, encoding='utf-8-sig')
            header = next(reader).rstrip('\r\n').split(';')
            assert [v.strip('"') for v in header[1:]] == CLIENTS
            assert len(header) == 371
            for count, line in enumerate(reader, 1):
                fields = line.rstrip('\r\n').split(';')
                assert len(fields) == 371
                t = datetime.datetime.fromisoformat(fields[0].strip('"'))
                assert t.tzinfo is None
                stamp = calendar.timegm(t.timetuple()) * 1000
                assert stamp == START_MS + count * 900000
                for i, client in enumerate(CLIENTS):
                    token = fields[i + 1].strip('"')
                    assert pattern.fullmatch(token), (count, client, token)
                    token = token.replace(',', '.')
                    value = float(token)
                    assert math.isfinite(value)
                    zero = value == 0
                    zeros[i] += zero
                    negative_zero[i] += zero and math.copysign(1, value) < 0
                    streams[i].write(f'{stamp},{client},{token},{count},true\n')
                    encoded = f'{stamp}\t{client}\t{struct.pack(">d", value).hex()}\t{count}\ttrue\n'.encode('ascii')
                    digests[i].update(encoded)
                if count in SCALES:
                    prefix[SCALES[count]] = [{'client': c, 'rows': count, 'contentSha256': h.copy().hexdigest()} for c,h in zip(CLIENTS,digests)]
                if count % 5000 == 0:
                    print('CONVERT', count, 'time rows', count * 370, 'value rows', flush=True)
                    (out / 'progress.json').write_text(json.dumps({'timeRows': count, 'valueRows': count * 370}) + '\n')
        assert count == ROWS
    finally:
        for f in streams:
            f.close()
    records = [{'client': c, 'rows': count, 'zeroRows': zeros[i], 'negativeZeroRows': negative_zero[i],
                'path': str(p), 'sha256': sha(p), 'bytes': p.stat().st_size, 'contentSha256': digests[i].hexdigest()}
               for i,(c,p) in enumerate(zip(CLIENTS,files))]
    write(out / 'transformation-manifest.json', {'status': 'CONVERTED_ALL_SOURCE_VALUES_NOT_IMPORTED', 'sourceArchiveSha256': EXPECTED,
        'timeRows': count, 'clientCount': 370, 'rows': count * 370, 'zeroRows': sum(zeros), 'negativeZeroRows': sum(negative_zero),
        'database': DATABASE, 'table': TABLE, 'csvFiles': records, 'prefixContentFingerprints': prefix,
        'firstNominalTimeMs': START_MS + 900000, 'lastNominalTimeMs': START_MS + count * 900000,
        'ddl': f'CREATE TABLE {TABLE} (time TIMESTAMP TIME,client_id STRING TAG,power_kw DOUBLE FIELD,source_row_no INT64 FIELD,record_present BOOLEAN FIELD)',
        'sqlImport': 'NOT_STARTED', 'independentOracle': 'SEPARATE_ORIGINAL_ZIP_PASS_REQUIRED'})
    print('CONVERTED', count * 370, flush=True)


def oracle(source, out):
    # This parser reads the original ZIP again and never opens generated CSV files.
    hashes = {c: hashlib.sha256() for c in CLIENTS}
    count = {c: 0 for c in CLIENTS}
    zeros = {c: 0 for c in CLIENTS}
    mins = {c: math.inf for c in CLIENTS}
    maxs = {c: -math.inf for c in CLIENTS}
    early = {c: [] for c in CLIENTS}
    scales = {}
    rowno = 0
    for client in CLIENTS:
        assert client.isascii()
    with zipfile.ZipFile(source) as archive, archive.open(MEMBER) as data:
        rows = csv.DictReader(io.TextIOWrapper(data, encoding='utf-8-sig'), delimiter=';')
        assert rows.fieldnames == [''] + CLIENTS
        for rowno, row in enumerate(rows, 1):
            nominal = datetime.datetime.strptime(row[''], '%Y-%m-%d %H:%M:%S')
            milliseconds = (nominal - datetime.datetime(1970, 1, 1)) // datetime.timedelta(milliseconds=1)
            assert milliseconds == START_MS + rowno * 900000
            for client in CLIENTS:
                number = float(decimal.Decimal(row[client].replace(',', '.')))
                assert math.isfinite(number)
                bits = struct.pack('!d', number).hex()
                hashes[client].update(('\t'.join([str(milliseconds), client, bits, str(rowno), 'true']) + '\n').encode('utf-8'))
                count[client] += 1
                zeros[client] += number == 0
                mins[client] = min(mins[client], number)
                maxs[client] = max(maxs[client], number)
                if rowno <= 64:
                    early[client].append([milliseconds, client, number, rowno, True])
            if rowno in SCALES:
                scales[SCALES[rowno]] = {'timeRows': rowno, 'rows': rowno * 370, 'endNominalTimeMs': milliseconds,
                    'clients': [{'client': c, 'rows': count[c], 'zeroRows': zeros[c], 'minimum': mins[c], 'maximum': maxs[c],
                                 'contentSha256': hashes[c].copy().hexdigest()} for c in CLIENTS]}
            if rowno % 5000 == 0:
                print('ORACLE', rowno, 'time rows', flush=True)
                (out / 'progress.json').write_text(json.dumps({'timeRows': rowno, 'valueRows': rowno * 370}) + '\n')
    assert rowno == ROWS
    queries = []

    def save(name, sql, values, types, ordered):
        path = out / 'oracles' / (name + '.jsonl')
        path.parent.mkdir(exist_ok=True)
        with path.open('x') as f:
            for value in values:
                f.write(json.dumps(value, separators=(',', ':'), allow_nan=False) + '\n')
        sql_path = out / 'sql' / (name + '.sql')
        sql_path.parent.mkdir(exist_ok=True)
        sql_path.write_text(sql)
        queries.append({'name': name, 'sql': sql, 'sqlFile': str(sql_path), 'sqlSha256': sha(sql_path),
                        'oracle': str(path), 'oracleSha256': sha(path), 'rows': len(values), 'types': types, 'ordered': ordered})

    for label, scale in scales.items():
        where = f'time > {START_MS} AND time <= {scale["endNominalTimeMs"]}'
        relation = DATABASE + '.' + TABLE
        global_min = min(v['minimum'] for v in scale['clients'])
        global_max = max(v['maximum'] for v in scale['clients'])
        save('global-' + label, f'SELECT COUNT(*) AS n,COUNT(power_kw) AS valid_n,MIN(power_kw) AS lo,MAX(power_kw) AS hi FROM {relation} WHERE {where}',
             [[scale['rows'], scale['rows'], global_min, global_max]], ['INT64', 'INT64', 'DOUBLE', 'DOUBLE'], False)
        save('grouped-' + label, f'SELECT client_id,COUNT(*) AS n,MIN(power_kw) AS lo,MAX(power_kw) AS hi FROM {relation} WHERE {where} GROUP BY client_id',
             [[v['client'], v['rows'], v['minimum'], v['maximum']] for v in scale['clients']], ['STRING', 'INT64', 'DOUBLE', 'DOUBLE'], False)
    relation = DATABASE + '.' + TABLE
    fields = 'time,client_id,power_kw,source_row_no,record_present'
    where = f'time > {START_MS} AND time <= {START_MS + 64 * 900000}'
    ordered = [row for c in CLIENTS for row in early[c]]
    save('ordered-window', f'SELECT {fields} FROM {relation} WHERE {where} ORDER BY client_id,time', ordered,
         ['TIMESTAMP', 'STRING', 'DOUBLE', 'INT64', 'BOOLEAN'], True)
    save('ordered-limit257', f'SELECT {fields} FROM {relation} WHERE {where} ORDER BY client_id,time LIMIT 257', ordered[:257],
         ['TIMESTAMP', 'STRING', 'DOUBLE', 'INT64', 'BOOLEAN'], True)
    save('zero-groups', f'SELECT client_id,COUNT(*) AS n FROM {relation} WHERE power_kw=0 GROUP BY client_id',
         [[c, zeros[c]] for c in CLIENTS if zeros[c]], ['STRING', 'INT64'], False)
    # SIN(angle)^2+COS(angle)^2 is separated from 0.5 for finite values.
    # This is explicitly a CPU stress query over real values, not a business-query claim.
    angle = 'CAST(power_kw AS DOUBLE)'
    for _ in range(5):
        angle = 'SIN(' + angle + ')'
    expression = f'(SIN({angle})*SIN({angle})+COS({angle})*COS({angle}))'
    cpu_where = f'(CASE WHEN {expression}>0.5 THEN CASE WHEN {where} THEN 1 ELSE 0 END ELSE 0 END)=1'
    save('cpu-full-window', f'SELECT {fields} FROM {relation} WHERE {cpu_where}', ordered,
         ['TIMESTAMP', 'STRING', 'DOUBLE', 'INT64', 'BOOLEAN'], False)
    duplicate = [[r[1], r[0], r[2], r[2], r[3], r[4]] for r in ordered]
    save('reordered-duplicate', f'SELECT client_id,time,power_kw,power_kw AS power_copy,source_row_no,record_present FROM {relation} WHERE {where} ORDER BY client_id,time',
         duplicate, ['STRING', 'TIMESTAMP', 'DOUBLE', 'DOUBLE', 'INT64', 'BOOLEAN'], True)
    write(out / 'oracle-manifest.json', {'status': 'INDEPENDENT_ORIGINAL_ZIP_ORACLES_GENERATED_NOT_SQL_VALIDATED', 'sourceArchiveSha256': EXPECTED,
        'timeRows': rowno, 'clientCount': len(CLIENTS), 'rows': sum(count.values()), 'zeroRows': sum(zeros.values()),
        'scales': scales, 'queries': queries, 'sqlImport': 'NOT_STARTED', 'noGeneratedCsvRead': True,
        'floatingAggregatePolicy': 'COUNT/MIN/MAX exact binary64 only; SUM/AVG deferred until independent bounded-error contract exists.'})
    print('ORACLES', len(queries), sum(count.values()), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['convert', 'oracle'], required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    out = args.out / args.mode
    out.mkdir(parents=True)
    identity(args.source.resolve(), out, args.mode)
    (convert if args.mode == 'convert' else oracle)(args.source.resolve(), out)


if __name__ == '__main__':
    main()
