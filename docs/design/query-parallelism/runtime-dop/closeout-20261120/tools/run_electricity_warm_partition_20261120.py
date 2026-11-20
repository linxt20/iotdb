# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Qualify deterministic partition growth, then run the separately assigned history contrast."""
from pathlib import Path
import fcntl
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import traceback
import time
from collections import Counter
from urllib.parse import unquote, urlsplit

M=Path('/tmp/Dev_0')
R=Path('/root/rivermind-data/iotdb-paper-experiments-20261117')
F=R/'shared-pressure-feedback-candidate-01'
W=M/'sxing-iotdb-warm-overlapping-files-service-20261119-01'
D=M/'sxing-iotdb-warm-overlapping-files-driver-20261119-01'
MODE=sys.argv[2] if len(sys.argv) in (3,4) else ''
assert MODE in ('qualification','contrast')
OUT=M/('sxing-iotdb-warm-partition-'+MODE+'-20261120-03')
QUAL=M/'sxing-iotdb-warm-partition-qualification-20261120-03/evidence/summary.json'
E=OUT/'evidence'
DRIVER=Path(__file__).resolve().parent
FROZEN=DRIVER/'partition-growth-plan.json'
GROW=DRIVER/'ElectricityWarmPartitionGrowthAppend.java'
INPUT=R/'datasets/uci-321-nominal-v1-20261118/convert/transformation-manifest.json'
CANDIDATE=M/'sxing-iotdb-warm-tie-publish-fix-20261119-01/packaged-01/release.json'
RELEASE_SHA=sys.argv[1]
P=D/'run_electricity_warm_overlapping_files_20261119.py'
O=W/'service/week-probe-config.json'
I=M/'sxing-iotdb-warm-stopped-overlap-audit-20261119-01/physical-file-adjudication.json'
Q=M/'sxing-iotdb-warm-overlap-resource-audit-20261119-03'
BIND={
    P:'70815b4d1947501299b55aefa76d6ab940ec83b0a64257ed7c10f649324d18f1',
    O:'d063237a9e7a6597f73f34c028eea316181be1cb07152169283ae37132f1f12f',
    I:'4231e1a0159c41abd788c6065f5f63905fc111dbef16b68ccaf87d877759795e',
    Q/'summary.json':'b5df7ab0f9ea3334e870122ef581d3ba9d5c86a5bdca0540e16bb6fd79c84a2d',
    W/'evidence/week-preregistration.json':'ae03c73c6361ff7e29d42f6f69efe7c6f8b1f0720c55ab8c320ac9f87eac3f3d',
    W/'evidence/overlap-execution-binding.json':'edb3e9697ebd30272b6a335e4d7607f0e22f2b80f496e1a4d47b9d96b1a26733',
    W/'evidence/after-week-probe-shutdown.json':'ec4525913635d83affbdfe20ab3d19275f9ecdec6d4028051809550d4a50cab5',
}
NODE_BIND={
    'confignode':('0efddee9579e2455f45cf4640b241ef9458f291a84b28dcebe6d1e820233a2e7',
                '1f93cd9bf23b3eda55d45e92fbdaebea3017d4244a93268165a6265f09316cb6'),
    'datanode-1':('858e68f4fb52c99d42e767875b4d5b3767544d85d06b744acd6ddce91efc686e',
                  'c5a04e60697884405be19be279492523786977263c6b6eaf241a17c654caf419'),
    'datanode-2':('bd254bcbf2de3d1762459defbf2c34893e04032905de23abeae8158d541eb846',
                  'dc9a3f4a418df1a86273c985d40e2112d8051ee32b08c9ce0acc89a230c1f381')
}

def sha(p):
    with Path(p).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def read(p):return json.loads(Path(p).read_bytes())

def put(p,x):
    with Path(p).open('x') as f:json.dump(x,f,indent=2);f.write('\n')

def load(p):
    s=importlib.util.spec_from_file_location(p.stem,p)
    o=importlib.util.module_from_spec(s);s.loader.exec_module(o)
    assert Path(o.__file__).resolve()==p
    return o

def scratch_storage(volume):
    # This workload writes only to MOUNT. Durable artifacts are read-only inputs.
    native=volume.headroom
    assert volume.MOUNT==M and volume.DURABLE==R
    def headroom(scratch_bytes,durable_bytes):
        assert durable_bytes==0
        receipt=native(scratch_bytes,durable_bytes)
        mount=receipt['scratch']
        receipt['legacyDualVolumeGate']=receipt['allowed']
        receipt['durablePolicy']='read-only inputs; write budget zero'
        receipt['allowed']=mount['freeBytes']>=mount['fivePercentFloorBytes']+scratch_bytes
        return receipt
    volume.headroom=headroom
    return volume

def main():
    assert all(sha(p)==v for p,v in BIND.items()) and not OUT.exists()
    overlap=load(P)
    assert all(sha(p)==v for p,v in overlap.PINS.items())
    pilot=load(overlap.PILOT)
    assert all(sha(p)==v for p,v in pilot.PINS.items())
    old=load(pilot.DRIVER)
    pilot.S=OUT;pilot.E=E;pilot.PINS={**pilot.PINS,**overlap.PINS,**BIND}
    real_load=old.load
    def load_for_scratch(p):
        module=real_load(p)
        if p!=old.V:
            return module
        assert sha(p)=='970658a8a76680aff638bff29bdee4e49e5e0235d665ca1c07e0405a078e3f9c'
        return scratch_storage(module)
    old.load=load_for_scratch
    b,v,h,c,phase,dist,seed,files=pilot.related(old)
    assert sha(dist['reactorReceipt'])==dist['reactorReceiptSha256']
    rel=Path(read(E/'preexecution.json')['release']['manifest'])
    assert sha(rel)==read(overlap.PLAN)['releaseManifestSha256']
    orig=read(O)
    assert orig['runtimeJars']==dist['jars'] and orig['serverJarSha256']==dist['serverJarSha256']
    assert sha(b.JAVA)==orig['javaSha256'] and Path(b.JAVA).resolve()==Path(b.JAVA)
    profile=orig['runtimeProfile']
    assert profile['historyEnabled'] and profile['runtimeScanHistoryMode']==overlap.MODE=='STARTUP_ONLY'
    assert not profile['sharedPressureFeedback'] and profile['initialWorkers']==profile['fixedWorkers']==0
    original=orig['configNodes']+orig['nodes']
    stopped=read(W/'evidence/after-week-probe-shutdown.json')['stopped']
    assert {(n['name'],n['pid'],n['configSha256']) for n in original}=={
        (n['name'],n['pid'],n['configSha256']) for n in stopped}
    assert not any(b.h.current_process(n,n['pid']) for n in original)
    metadata=read(Q/'summary.json')
    assert metadata['status']=='ALL_49_NEW_TSFILES_OVERLAP_BASELINE_FOR_ALL_CONTAINED_DEVICES'
    qp=Q/'preexecution.json'; assert sha(qp)==metadata['preexecutionSha256']
    sidecars=read(qp)['sidecarSha256']
    inventory=read(I)['perDataNode']
    grown={n['name']:inventory.get(n['name'],{}).get('fullStoppedInventory',{}) for n in original}
    assert old.inventory(b,original)==grown
    assert all(sha(q)==d for q,d in sidecars.items())
    prereg=read(W/'evidence/week-preregistration.json')
    assert list(overlap.ROLES)==prereg['roles']
    previous=overlap.D/'warm-disjoint-week-v1-native-ownership-20261119.json'
    sql=overlap.D/'disjoint-week-probe.sql';oracle=overlap.D/'complete-typed-oracle.jsonl'
    plan_path=overlap.D/'plan.json'; plan=read(plan_path)
    assert sha(previous)==plan['previousNativeOwnershipSha256']
    prior=read(previous)['regions']
    wanted={w['timePartition']:w for w in plan['windows']}
    def sql_for(n):
        assert n==8
        return sql.read_text()
    b.common.cpu_sql=sql_for
    o,expected=pilot.verify_query_loader(b,c,phase,oracle)
    assert expected.total()==740740
    assert sha(FROZEN)=='a8e285105e5e47d63909b8936ded6a96d77877fc4a257ff66c2e87f2e90c3bfd'
    if MODE=='contrast':
        assert len(sys.argv)==4 and sha(QUAL)==sys.argv[3]
        q=read(QUAL)
        assert q['status']=='PARTITION_MAJOR_IDENTICAL_PHYSICAL_FIXTURE_AND_NATIVE_LAYOUT' and q['releaseSha256']==RELEASE_SHA
        assert q['planSha256']==sha(FROZEN)
    formal=read(FROZEN)
    assert sha(GROW)==formal['growthClientSha256'] and sha(INPUT)==formal['sourceManifestSha256']
    manifest=read(INPUT)
    input_pins={x['path']:x['sha256'] for x in manifest['csvFiles']}
    assert len(input_pins)==370 and all(sha(p)==h for p,h in input_pins.items())
    assert sha(plan_path)==formal['originalWindowPlanSha256'] and sha(oracle)==formal['originalWindowOracleSha256']
    assert RELEASE_SHA==sha(CANDIDATE)
    candidate=read(CANDIDATE)
    assert candidate['status']=='WARM_EXACT_EXTREMA_AND_PUBLICATION_CANDIDATE_PACKAGED_NOT_SERVICE_VERIFIED'
    parent=Path(candidate['parentRelease'])
    assert sha(parent)==candidate['parentReleaseSha256']=='36a5b2edd84877d1e4a498fa637a6a8ae7d709f6af9f64ea92ec9a3745a09ee1'
    assert read(parent)['parentReleaseSha256']==sha(rel)
    assert candidate['testedClassesCompared']==4530 and not candidate['testedClassesMismatched']
    full=Path(candidate['reactorReceipt'])
    assert sha(full)==candidate['reactorReceiptSha256']
    assert read(full)['reactor']==dict(tests=8118,failures=0,errors=0,skipped=56)
    assert candidate['nativeReplaySha256']=='60130c3f08752d5d8b01567cde6cd191bc4b794d33d041632bb4c306a318754d'
    distjars={Path(x['path']).name:x['path'] for x in candidate['jars']}
    originals={Path(x['path']).name:x['path'] for x in dist['jars']}
    assert set(distjars)==set(originals)
    assert all(sha(x['path'])==x['sha256'] for x in candidate['jars'])
    repo=Path(candidate['repository'])
    def git(*x):return subprocess.check_output(['git','-C',str(repo),*x])
    assert git('rev-parse','HEAD').decode().strip()==candidate['baseCommit']
    assert hashlib.sha256(git('diff','HEAD','--full-index','--binary')).hexdigest()==candidate['sourceDiffSha256']
    assert all(sha(repo/x['path'])==x['sha256'] for x in candidate['sourceFiles'])
    assert all(sha(x['path'])==x['sha256'] for x in dist['jars'])
    rt=Path('/proc/self/exe').resolve()
    javac=Path(b.JAVA).with_name('javac')
    assert sha(javac)=='bafd67b6b902abb284b24c168ac90eb1490dac6448054a71ccb26c4918577b22'
    jarpaths=os.pathsep.join(distjars.values())
    cdir=E/'growth-client-classes';cdir.mkdir()
    compiler=[str(javac),'--release','17','-cp',jarpaths,'-d',str(cdir),str(GROW)]
    put(E/'trial-preexecution.json',dict(entry=str(__file__),entrySha256=sha(__file__),directDriver=str(P),directDriverSha256=sha(P),
         release=str(CANDIDATE),releaseSha256=sha(CANDIDATE),repository=str(repo),baseCommit=candidate['baseCommit'],
         branch=git('branch','--show-current').decode().strip() or 'detached',sourceDiffSha256=candidate['sourceDiffSha256'],
         runtime=str(rt),runtimeSha256=sha(rt),runtimeImport=list(os.sys.path),pythonpath=os.getenv('PYTHONPATH'),
         java=str(b.JAVA),javaSha256=sha(b.JAVA),javacSha256=sha(javac),growthDriver=str(GROW),growthDriverSha256=sha(GROW),
         plan=str(FROZEN),planSha256=sha(FROZEN),sqlSha256=sha(sql),oracleSha256=sha(oracle),compiler=compiler,
         serverJarSha256=candidate['serverJarSha256'],sourceInventorySha256=sha(I),inputSHA=sha(INPUT),
         allInputFiles=input_pins,mode=MODE,qualificationSha256=sha(QUAL) if MODE=='contrast' else None))
    with (E/'growth-client-compile.log').open('x') as log:
        assert subprocess.run(compiler,stdout=log,stderr=subprocess.STDOUT,timeout=90).returncode==0
    cls=cdir/'ElectricityWarmPartitionGrowthAppend.class'
    assert cls.is_file()
    compiler_identity=sha(cls)

    def factory(leaf,mode):
        def specs(target,orig_m,release,feedback,inv):
            assert not feedback and target==leaf and orig_m==orig and release==candidate and inv==grown
            arr=[]
            for n in original:
                name=n['name']; src=Path(n['nodeRoot']); dst=target/'cluster'/name
                assert src==W/'service/cluster'/name
                p=src/'identity/prelaunch.json';k=src/'identity/week-profile.json'
                assert (sha(p),sha(k))==NODE_BIND[name]
                oldcfg=Path(n['configPath']); conf=read(k)
                assert sha(oldcfg)==n['configSha256']==conf['configSha256']
                argv=read(p)['argv']
                assert conf['argvSha256']==hashlib.sha256(os.fsencode('\0'.join(argv))).hexdigest()
                for r0 in ('data','conf'):
                    assert not any(q.is_symlink() for q in (src/r0).rglob('*'))
                for rr in ('tmp','conf','logs','identity'):(dst/rr).mkdir(parents=True)
                shutil.copytree(src/'data',dst/'data')
                assert b.tsfiles(dst/'data')==grown[name]
                for p0 in (src/'conf').iterdir():
                    x=p0.read_text().replace(str(src),str(dst))
                    if p0==oldcfg:
                        key='runtime_scan_history_mode=STARTUP_ONLY'
                        assert x.splitlines().count(key)==1
                        for key2 in ('enable_seq_space_compaction=false','enable_unseq_space_compaction=false',
                            'enable_cross_space_compaction=false','enable_runtime_scan_shared_pressure_feedback=false'):
                            if n['role']=='DataNode':assert key2 in x.splitlines(),key2
                        x=x.replace(key+'\n','runtime_scan_history_mode='+mode+'\n')
                        for option in formal['automaticTimedFlush']:
                            prop=option.partition('=')[0]
                            assert all(not line.startswith(prop+'=') for line in x.splitlines())
                            x+=option+chr(10)
                        if n['role']=='DataNode':
                            option=formal['walFixtureIsolation']['dataNodeProperty']
                            prop=option.partition('=')[0]
                            assert all(not line.startswith(prop+'=') for line in x.splitlines())
                            x+=option+chr(10)
                    (dst/'conf'/p0.name).write_text(x)
                for sidecar,hsh in sidecars.items():
                    q=Path(sidecar)
                    if q.is_relative_to(src):assert sha(dst/q.relative_to(src))==hsh
                def remap(x):return x.replace(str(src),str(dst)).replace(dist['distributionRoot'],candidate['distributionRoot'])
                real=list(map(remap,argv))
                assert real[0]==str(b.JAVA)
                assert set(real[real.index('-cp')+1].split(':'))==set(distjars.values())
                assert all(str(W) not in x and dist['distributionRoot'] not in x for x in real)
                ports={n['internalEndpoint']['port'],*(n[i] for i in ('rpc','jmxPort','consensus','mpp','schema','metric') if n.get(i))}
                assert not any(b.owners(q) for q in ports)
                cfg=dst/'conf'/oldcfg.name
                repl={**n,'nodeRoot':str(dst),'configPath':str(cfg),'configSha256':sha(cfg),
                      'distributionRoot':candidate['distributionRoot'],'serverJarSha256':candidate['serverJarSha256']}
                repl.pop('pid')
                put(dst/'identity/prelaunch.json',dict(mode=mode,argv=real,configSha256=sha(cfg),releaseSha256=sha(CANDIDATE),
                      oldLaunchSha256=NODE_BIND[name][0],oldProfileSha256=NODE_BIND[name][1]))
                arr.append((repl,real))
            return arr
        return specs

    def growth(ep,nodes,leaf):
        leaf.mkdir()
        jdbc=urlsplit(ep['jdbcUrl'].replace('jdbc:iotdb:','http:',1))
        n,=[n for n in nodes if n['rpc']==jdbc.port]
        assert jdbc.hostname=='127.0.0.1' and b.owners(jdbc.port)=={n['pid']}
        assert sha(cls)==compiler_identity
        args=leaf/'parameters.json'
        put(args,dict(scope='WARM_PARTITION_STARTUP_OUTSIDE_FROZEN_WINDOWS',rpc=jdbc.netloc,
            sourceManifest=str(INPUT),sourceManifestSha256=sha(INPUT),shiftMs=formal['samePartitionShiftMs'],
            frozenWindowsPlan=str(plan_path),endpointManifest=ep['manifest'],endpointManifestSha256=ep['manifestSha256']))
        classlog=leaf/'loaded-classes.log'
        argv=[str(b.JAVA),'-Xmx2g',f'-Xlog:class+load=info:file={classlog}:time,level,tags:filecount=0',
              '-cp',str(cdir)+os.pathsep+jarpaths,'ElectricityWarmPartitionGrowthAppend',str(args),str(leaf)]
        put(leaf/'preexecution.json',dict(argv=argv,parametersSha256=sha(args),compileClassSha256=compiler_identity,
             releaseSha256=sha(CANDIDATE),planSha256=sha(FROZEN),endpointManifestSha256=ep['manifestSha256']))
        start=time.monotonic_ns()
        with (leaf/'run.log').open('x') as log:
            p=subprocess.Popen(argv,env=old.proc_env(),stdout=log,stderr=subprocess.STDOUT)
            (leaf/'process.pid').write_text(f'{p.pid}\n')
            observed=False
            try:
                while p.poll() is None:
                    argv0=b.argv_of(p.pid)
                    if argv0:
                        assert argv0==list(map(os.fsencode,argv));observed=True
                    assert all(b.h.current_process(q,q['pid']) for q in nodes)
                    assert time.monotonic_ns()-start<20*60*1_000_000_000
                    time.sleep(4)
            finally:
                if p.poll() is None:
                    p.terminate()
                    try:p.wait(timeout=10)
                    except subprocess.TimeoutExpired:p.kill();p.wait(timeout=10)
        put(leaf/'exit.json',dict(exitCode=p.returncode,loadedProcessArgvObserved=observed))
        assert p.returncode==0 and observed
        receipt=read(leaf/'import-receipt.json'); runtime=read(leaf/'loaded-client.json')
        assert receipt['status']=='PARTITION_GROWTH_OUTSIDE_EIGHT_WINDOWS_FLUSHED_ONE_AT_A_TIME'
        assert (receipt['sourceRows'],receipt['excludedRows'],receipt['appendedRows'])==(51894720,740740,51153980)
        assert receipt['frozenWindowsPlanSha256']==sha(plan_path) and receipt['deploymentManifestSha256']==ep['manifestSha256']
        libs={x['path']:x['sha256'] for x in candidate['jars']}
        assert sha(cls)==compiler_identity
        assert Path(unquote(urlsplit(runtime['entry']).path))==cdir
        assert f'ElectricityWarmPartitionGrowthAppend source: file:{cdir}/' in classlog.read_text()
        for name in ('tablet','session','gson'):
            u=unquote(urlsplit(runtime[name]).path);assert sha(u)==libs[u]
        assert len(receipt['members'])==370 and all(x['skippedWindowRows']==2002 for x in receipt['members'].values())
        flushes=[json.loads(l) for l in (leaf/'partition-flushes.jsonl').read_text().splitlines()]
        assert [f['partition'] for f in flushes]==list(range(2139,2349))
        assert all(f['flushed'] and f['appendedRows']>0 for f in flushes)
        assert [sum(f[k] for f in flushes) for k in ('appendedRows','excludedRows')]==[51153980,740740]
        assert receipt['flushedPartitions']==len(flushes)
        receipt['partitionScheduleSha256']=sha(leaf/'partition-flushes.jsonl')
        return receipt

    def native_query(ep,label,seen):
        dest,res=b.h.query('contrast',label,ep,8,o,expected)
        q=overlap.observe(b,h,c,phase,dest,res,wanted,seen,prior)
        native={}
        ro=read(dest/'roster.json')
        for n,z in q['sourceByDataNode'].items():
            f,=[a['fragmentInstanceId'] for a in ro['fragmentInstances'] if a['containsDeviceScan'] and a['dataNodeId']==n]
            t=read(dest/'statistics'/f'{f}.thrift.json')
            trace,=t['43']['lst'][2:]
            assert trace['11']['str']=='ESTIMATED_FROM_TSFILE_BYTES' and trace['10']['tf']==1
            native[n]=dict(estimate=trace['12']['i32'],startup=z['initial'],action=z['decision']['action'],
                     history=z['decision']['reason'],joined=len(z['sameSourceHistoryLinks']),statSha256=sha(dest/'statistics'/f'{f}.thrift.json'))
        q['actualColdEstimateAndStartup']=native
        put(dest.parent/'week-probe-evidence.json',q)
        print(label,native,flush=True)
        return q

    def layout(q):
        return q['operatorGraph'],{n:(x['region'],x['logicalDeviceShape'],
            {j:(r['nativeRows'],r['allNativePartitions']) for j,r in x['lanes'].items()})
            for n,x in q['sourceByDataNode'].items()}

    def physical(owned,now):
        ids={}
        for n in owned:
            key=n['name']; q=now[key]; prior0=grown[key]
            assert prior0.items()<=q.items()
            data=Path(n['nodeRoot'])/'data'
            bounds={Path(p).parts[4] for p in prior0}
            fresh={p:h for p,h in q.items() if p not in prior0}
            if n['role']=='ConfigNode':
                assert not fresh;continue
            assert fresh and {Path(p).parts[4] for p in fresh}<=bounds
            old_region,={Path(p).parts[3] for p in prior0}
            assert all(Path(p).parts[2:4]==('uci_electricity_v4',old_region) for p in fresh)
            v=sum((data/k).stat().st_size for k in q)
            inventory=Counter((tuple(Path(p).parts[1:5]),(data/p).stat().st_size,hsh) for p,hsh in fresh.items())
            counts=Counter((int(Path(p).parts[4]),Path(p).parts[1]) for p in fresh)
            expected_counts=Counter({(p,'sequence'):1 for p in range(2139,2349)})
            expected_counts.update({(p,'unsequence'):1 for p in range(2139,2348)})
            ids[key]=dict(totalBytes=v,rawAdded=fresh,
                partitionClassCounts={f'{p}/{kind}':n for (p,kind),n in sorted(counts.items())},
                expectedPartitionClassMatrix=counts==expected_counts,
                addedNormalized=sorted([part,size,hsh,k] for (part,size,hsh),k in inventory.items()),
                passes=formal['minimumGrownBytesExclusive']<v<=formal['maximumGrownBytesInclusive'])
        return ids

    arms={}
    selected=formal['qualification']['arms'] if MODE=='qualification' else formal['arms']
    for i,mode in enumerate(selected):
        label=('qa','qb')[i] if MODE=='qualification' else ('control','warm')[i]
        leaf=OUT/label;leaf.mkdir()
        evidence=E/label;evidence.mkdir()
        b.specs=factory(leaf,mode);b.h.D=evidence
        arm={};live=[];last=grown
        try:
            ep=b.deploy(leaf,orig,candidate,False,live,grown)
            m=read(ep['manifest']); m['runtimeProfile']={**profile,'runtimeScanHistoryMode':mode,'historyEnabled':mode!='IGNORE_MEASURED'}
            fp=leaf/'effective-manifest.json';put(fp,m)
            ep={**ep,'manifest':str(fp),'manifestSha256':sha(fp)}
            seen={}
            seed_roles=formal['qualification']['preGrowthQueries'] if MODE=='qualification' else formal['perArmSequence'][:4]
            for label0 in seed_roles:
                scan=native_query(ep,label0,seen)
                assert all(x['estimate']==x['startup']==2 for x in scan['actualColdEstimateAndStartup'].values())
                arm[label0]=scan
            assert all(layout(z)==layout(arm['seed-prime']) for z in arm.values())
            r=growth(ep,live,leaf/'growth')
            wal_driven={}
            for node in live:
                if node['role']!='DataNode':continue
                path=Path(node['nodeRoot'])/'logs/stdout.log'
                assert path.is_file()
                wal_driven[node['name']]=[line for line in path.read_text().splitlines()
                    if 'WALNode' in line and 'flushes memTable' in line]
            put(leaf/'wal-flush-audit.json',wal_driven)
            last=old.inventory(b,live)
            shapes=physical(live,last);put(leaf/'post-growth-geometry.json',shapes)
            q=native_query(ep,'grown-first',seen)
            arm['grown-first']=q
            b.postcheck(leaf,live,candidate,last)
            arms[label]=dict(mode=mode,queries=arm,importProjectionSha256=r['projectionSha256'],
                 partitionScheduleSha256=r['partitionScheduleSha256'],physical=shapes,
                 walDrivenFlushes=sum(map(len,wal_driven.values())),allRecordEvidenceSha256=sha(leaf/'loaded-service-proof.json'))
        finally:
            b.clean_clients(evidence);b.h.D=evidence;b.h.stop_owned(leaf,live,'closed')
        assert old.inventory(b,live)==last and old.inventory(b,original)==grown
    control,warm=tuple(arms.values())
    a0=control['queries']['grown-first'];a1=warm['queries']['grown-first']
    bound=all(x['passes'] for a0x in arms.values() for x in a0x['physical'].values())
    physical_equal=all(control['physical'][n]['addedNormalized']==warm['physical'][n]['addedNormalized'] and
                       control['physical'][n]['totalBytes']==warm['physical'][n]['totalBytes'] for n in control['physical'])
    logical_equal=layout(a0)==layout(a1)
    data_equal=(control['importProjectionSha256']==warm['importProjectionSha256'] and
                control['partitionScheduleSha256']==warm['partitionScheduleSha256'])
    partition_strict=all(x['expectedPartitionClassMatrix'] for a in arms.values() for x in a['physical'].values())
    wal_isolated=all(a['walDrivenFlushes']==0 for a in arms.values())
    match=wal_isolated and bound and physical_equal and logical_equal and data_equal and partition_strict
    x0=a0['actualColdEstimateAndStartup']; x1=a1['actualColdEstimateAndStartup']
    control_ok=all(x0[n]['estimate']==x1[n]['estimate']==3 and x0[n]['startup']==3
        and x0[n]['joined']==0 for n in (1,2))
    if MODE=='qualification':
        accepted=match and control_ok and all(x['startup']==3 and x['joined']==0 for x in x1.values())
        status='PARTITION_MAJOR_IDENTICAL_PHYSICAL_FIXTURE_AND_NATIVE_LAYOUT' if accepted else 'PARTITION_MAJOR_FIXTURE_QUALIFICATION_REJECTED'
    else:
        accepted=match and control_ok and all(x1[n]['startup']==2 and x1[n]['joined']==2
            and x1[n]['action']=='REBALANCE_NEXT_FRAGMENT' for n in (1,2))
        status='ACTUAL_WARM_STARTUP_CAUSAL_CONTRAST_VERIFIED' if accepted else 'WARM_STARTUP_CONTRAST_DID_NOT_MEET_DECLARED_GATES'
    s=E/'summary.json'
    put(s,dict(status=status,mode=MODE,
         noWalDrivenFlush=wal_isolated,marginAndNaturalWidthGate=bound,exactMatchedPhysicalInventory=physical_equal,matchedPredictedPartitionClassMatrix=partition_strict,
         samePostGrowthLayout=logical_equal,identicalInputProjection=data_equal,
         arms=arms,postGrowthWidths=dict(zip(arms,(x0,x1),strict=True)),planSha256=sha(FROZEN),
         releaseSha256=sha(CANDIDATE),scope='Two DataNodes on one physical host; qualification is not treatment evidence. No speedup inferred.'))
    print(sha(s),status,x0,x1,flush=True)

if __name__=='__main__':
    assert len(sys.argv)==(3 if MODE=='qualification' else 4)
    with (F/'local-timed-experiments.lock').open('a') as o:
        fcntl.flock(o,fcntl.LOCK_EX)
        try:
            main()
        except BaseException:
            if E.exists():(E/'failure.txt').write_text(traceback.format_exc())
            raise
