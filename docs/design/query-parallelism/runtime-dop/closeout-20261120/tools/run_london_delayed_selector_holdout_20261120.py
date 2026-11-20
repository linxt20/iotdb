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

"""Run four frozen pairs comparing the Electricity-trained selectors under the delayed London trace."""
from contextlib import contextmanager,ExitStack
from pathlib import Path
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import time
import traceback

R=Path('/root/rivermind-data/iotdb-paper-experiments-20261117')
F=R/'shared-pressure-feedback-candidate-01'
M=Path('/tmp/Dev_0')
QA=M/'sxing-iotdb-london-five-centikwh-evidence-20261119-15'
WORKNAME='sxing-iotdb-london-delayed-selector-holdout-20261120-01'
BASE_DRIVER=M/'sxing-iotdb-london-two-pulse-background-gate-driver-20261119-01'
PREVIOUS=M/'sxing-iotdb-london-natural-short-wave-calibration-20261119-01'
O=M/WORKNAME
PILOT=BASE_DRIVER.joinpath("london_scalar_two_pulse_prereg_20261119.json")
BG=BASE_DRIVER.joinpath("oracle")
GATE=M/'sxing-iotdb-selector-nonaliased-frozen-20261119-01/selector-background-gate-prereg.json'
THRESHOLD=GATE.with_name('background-only-gate.json')
CALIBRATION=GATE.with_name('run_london_delayed_selector_background_gate_20261119.py')
MATCHED=M/'sxing-iotdb-london-two-pulse-feedback-pairs-driver-20261119-01/run_london_two_pulse_matched_feedback_20261119.py'
CALIBRATION_SUMMARY=M/'sxing-iotdb-london-delayed-selector-background-gate-20261119-01/summary.json'
PARENT_THRESHOLD_SHA='38ae4f28e0c3c84201b13df41de6132553154bc3ac6c36411b0f02999e9c8da6'
ARRIVAL=GATE.with_name("new-arrival-selector-prereg.json")
SELECTORS=GATE.with_name("selector-freeze.json")
QUALIFIED=M/'sxing-iotdb-london-scalar-two-pulse-audit-20261119-01/scalar-two-pulse-adjudication.json'
PILOT_SHA="d0b7b2080720b092565ffc8586cd5ebfbbe6049e024ea47be3c6a4d31e39a7ac"
PLAN=M/'sxing-iotdb-london-natural-short-wave-20261119-prereg01/london_natural_short_wave_prereg_20261119.json'
CAL=F/'calibrate_london_double_input_meter_week_background_i64_20261119.py'
PHASE=BASE_DRIVER.joinpath('london_early_wave_phase_proof_20261119.py')
BOOT=M/'sxing-iotdb-london-fine-screen-driver-20261119-15/screen_london_five_centikwh_histogram_heap12_r3_20261119.py'
PINS={CALIBRATION:'24f35da388f98adbd02b036b9633608a2c665ee975709e14b3dada939aacd504',
      MATCHED:'11a6583bad8771fa436bd0b2d252f5892bf3b28037365bb8fdb07fc79da5ec59',
      PLAN:'ad1ea6c7175d596d0dcbbe2f0b59c6d86733c1d4e8942634b8f9521ba435d882',
      CAL:'141c4f22f9e61d1c1b80cd1fa3a0d1c8900e45f35bde485d018c4ae757f9770e',
      PHASE:'469611e4e48a04562041fe43485517ab633e66185c38970d3140fa271b83b59b',
      BOOT:'8115f7dc7bb0dc992c8bf3daaa908839881867f361cb9deaef36d3cfeb9c3928',
      QA/'independent-capacity-screen.json':'d9dc5ace1acd34791cd480d240dd68654d97ae77cf54b977a60f6867fe309917',
      PREVIOUS/'summary.json':'1ddb984d910e03261ffec80f6299fa9230972fa55577cece409622cf79f15401',
      PREVIOUS/'rate-0.5/rate-screen.json':'08c0595956e783a4287f3d5b0912cb1f1719f37d35b127a600536d686428f01b'}
N=10**9

def sha(path):
    with Path(path).open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()

def read(path):return json.loads(Path(path).read_text())

def put(path,obj):
    with Path(path).open('x') as f:json.dump(obj,f,indent=2,ensure_ascii=False);f.write('\n')

def load(path):
    assert sha(path)==PINS[path]
    spec=importlib.util.spec_from_file_location(path.stem,path)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert Path(mod.__file__).resolve()==path
    return mod

@contextmanager
def owned_client(args,root,cwd=None):
    with root.open('x') as info:
        proc=subprocess.Popen(args,cwd=cwd,env=dict(os.environ,IOTDB_ACCEPTANCE_PASSWORD='root'),stdout=info,stderr=subprocess.STDOUT)
    try:
        yield proc
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:proc.wait(timeout=10)
            except subprocess.TimeoutExpired:proc.kill();proc.wait(timeout=10)

def read_live_witness(root,p,phase,width):
    path=root/'runtime-live-timeline.jsonl'
    finish=time.monotonic()+60
    while not path.is_file():
        assert time.monotonic()<finish and p.poll() is None
        time.sleep(.03)
    latest={}
    with path.open() as stream:
        while time.monotonic()<finish and p.poll() is None:
            pos=stream.tell()
            line=stream.readline()
            if not line.endswith('\n'):
                stream.seek(pos);time.sleep(.02);continue
            item=json.loads(line)
            good=phase.live(item,width)
            if good:
                _,dn,ts,_=good
                assert dn in (1,2)
                latest[dn]=(ts,item)
            now=time.monotonic_ns()
            if set(latest)=={1,2} and all(0<=now-ts<=N for ts,_ in latest.values()):
                assert len({z['response']['observation']['fragmentInstanceId'] for _,z in latest.values()})==2
                return {'originMonotonicNanos':now,'witnesses':{n:z for n,(_,z) in latest.items()}}
    raise RuntimeError('Could not witness both source groups live at the required initial width')

def place(ctx,leaf,profile):
    b,h,rel=ctx['b'],ctx['h'],ctx['release']
    previous=ctx['parent'];source=ctx['boot'].S
    def move(x):return x.replace(str(source),str(leaf))
    mf=json.loads(move(json.dumps(previous)))
    nodes=mf['configNodes']+mf['nodes']
    oldnodes=previous['configNodes']+previous['nodes']
    width,on=profile['initial'],profile['sharedPressureFeedback']
    mf['runtimeProfile']={**mf['runtimeProfile'],'fixedWorkers':0,'initialWorkers':width,
                          'sharedPressureFeedback':on,'historyEnabled':False,'ablationMode':'standard'}
    seal=ctx['control']
    starts=[]
    for n,old in zip(nodes,oldnodes,strict=True):
        root=Path(n['nodeRoot']);base=Path(old['nodeRoot'])
        assert root.is_relative_to(leaf) and base.is_relative_to(source)
        for sub in ('conf','identity','logs','tmp'):(root/sub).mkdir(parents=True)
        shutil.copytree(base/'data',root/'data',symlinks=False)
        assert b.tsfiles(root/'data')==ctx['tsfiles'][n['name']]
        for f in sorted((base/'conf').iterdir()):
            assert sha(f)==seal[str(f.relative_to(source))] and f.resolve()==f
            text=move(f.read_text())
            active_xml='logback-datanode.xml' if n['role']=='DataNode' else 'logback-confignode.xml'
            if f.name==active_xml:
                prefix='IOTDB' if n['role']=='DataNode' else 'CONFIGNODE'
                previous_key='${'+prefix+'_HOME}/logs/'
                assert text.count(previous_key)>4, (f,text.count(previous_key))
                text=text.replace(previous_key,'${'+prefix+'_LOGS}/')
                assert previous_key not in text
            if f.name=='iotdb-system.properties' and n['role']=='DataNode':
                for key,oldv,newv in (
                    ('runtime_scan_parallelism_fixed_workers','8','0'),
                    ('runtime_scan_parallelism_initial_workers','8',str(width)),
                    ('enable_runtime_scan_shared_pressure_feedback','false',str(on).lower())):
                    pattern=r'^'+key+'=(.*)$'
                    assert re.findall(pattern,text,re.M)==[oldv]
                    text=re.sub(pattern,key+'='+newv,text,flags=re.M)
                for key in ctx['boot'].DISABLED+('enable_dynamic_scan_parallelism',):
                    assert re.findall(r'^'+key+'=(.*)$',text,re.M)==['false']
                assert re.findall(r'^enable_runtime_scan_parallelism=(.*)$',text,re.M)==['true']
                assert not re.search(r'^runtime_scan_ablation_mode=',text,re.M)
                text=text.rstrip()+'\nruntime_scan_ablation_mode=standard\n'
            (root/'conf'/f.name).write_text(text)
        assert sha(old['configPath'])==old['configSha256']
        n['configSha256']=sha(n['configPath'])
        pre=base/'identity/prelaunch.json'
        assert sha(pre)==seal[str(pre.relative_to(source))]
        previous_launch=read(pre)
        assert previous_launch['serverJarSha256']==rel['serverJarSha256']
        argv=[move(x) for x in previous_launch['argv']]
        if n['role']=='DataNode':
            assert argv.count('-Xmx12g')==1
            argv=[('-Xmx24g' if z=='-Xmx12g' else z) for z in argv]
        logs_property='IOTDB_LOGS' if n['role']=='DataNode' else 'CONFIGNODE_LOGS'
        assert argv.count('-D'+logs_property+'='+str(root/'logs'))==1,n['name']
        classes=root/'identity/classes-loaded.log'
        assert argv[0]==str(b.JAVA) and str(source) not in ''.join(argv) and sum(str(classes) in x for x in argv)==1
        port=n['rpc'] or n['internalEndpoint']['port']
        assert all(not b.owners(x) for x in [port,n['internalEndpoint']['port']]+[n[k] for k in ('jmxPort','consensus','mpp','schema') if n.get(k)])
        put(root/'identity/prelaunch.json',{'argv':argv,'sourcePrelaunchSha256':sha(pre),'serverJarSha256':rel['serverJarSha256'],
                'configPath':n['configPath'],'configSha256':n['configSha256']})
        starts.append((n,root,argv,port,classes))
    return mf,starts

def bg_args(ctx,root,ep,name,offsets,trigger):
    b,cal=ctx['b'],ctx['cal']
    m=ctx['bgOracle'][name]
    leaf=root/name;param=root/(name+'-parameters.json');classlog=root/(name+'-classload.log')
    put(param,{'out':str(leaf),'runId':root.parent.name+'-'+name,'jdbcUrl':ep['jdbcUrl'],
        'username':'root','passwordEnv':'IOTDB_ACCEPTANCE_PASSWORD','maximumInFlight':ctx['maximumInFlight'],
        'sqlFile':str(cal.BG/(name+'.sql')),'sqlSha256':m['sqlSha256'],
        'oracle':str(cal.BG/(name+'.jsonl')),'oracleSha256':m['oracleSha256'],'types':m['types'],
        'expectedRows':m['resultRows'],'arrivalOffsetsNanos':offsets,'trigger':str(trigger)})
    argv=[str(b.JAVA),'-Xmx2g','-Xlog:class+load=info:file='+str(classlog)+':time,level,tags:filecount=0',
          '-cp',ctx['backgroundClassPath'],'FluctuatingBackgroundQueryDriver',str(param)]
    put(root/(name+'-launch.json'),{'argv':argv,'parametersSha256':sha(param),'case':name})
    return argv,leaf,classlog

def fg_args(ctx,fg,manifest):
    fg.mkdir()
    p=fg/'params.json';out=fg/'ordinary-sql'
    template=ctx['foregroundTemplate']
    put(p,{**template,'manifest':str(manifest),'sqlFile':str(ctx['sql']),'out':str(out)})
    argv=[str(ctx['b'].JAVA),'--add-opens=java.base/java.lang=ALL-UNNAMED','-Xmx2g',
          '-Xlog:class+load=info:file='+str(fg/'classload.log')+':time,uptime,level,tags:filecount=0',
          '-cp',ctx['classpath'],'StaticScanAcceptanceDriver','--mode','ordinary-sql','--params',str(p)]
    put(fg/'preexecution.json',{'argv':argv,'parametersSha256':sha(p),'sqlSha256':sha(ctx['sql']),
                               'oracleSha256':sha(ctx['truth']),'manifestSha256':sha(manifest)})
    return argv,out

def verify_fg(ctx,fg,out,width):
    b,phase,ex=ctx['b'],ctx['phase'],ctx['exact']
    rc=read(out/'result-receipt.json');raw=out/'result.rows.jsonl'
    assert sha(raw)==rc['sha256'] and rc['fullResultDrained']
    assert ex.typed_equal(ctx['truth'],raw)==(rc['rows'],167932474) and rc['rows']==2898043
    assert sha(out/'executed.sql')==rc['sqlSha256']
    assert (out/'executed.sql').read_bytes().split(b'/* runtime_sql_acceptance_run=',1)[0].rstrip()==ctx['sql'].read_bytes().rstrip()
    cfg=(fg/'classload.log').read_text()
    assert all(k+' source: file:'+str(ctx['cli']/'classes') in cfg for k in ('StaticScanAcceptanceDriver','OrdinarySqlAcceptanceDriver'))
    a=ex.recompute(out)
    assert a['startupByNode']=={1:width,2:width} and sum(a['sourceRowsByNode'].values())==167932474
    assert a['verifiedTopology']=={'sources':2,'buckets':2,'edges':4}
    return {'elapsedSeconds':rc['elapsedNanos']/N,'resultSha256':sha(raw),
            'layoutSha256':phase.digest(phase.normalized_roster(out/'roster.json')),'audit':a}

def trial(ctx,leaf,ep,width,on,background_preflight,layout):
    root=leaf/'loaded';root.mkdir()
    trigger=root/'origin.json'
    c,b=ctx['cal'],ctx['b']
    arrivals={}
    for i,name in enumerate(ctx['selected']['shapes']):
        offsets=c.offsets(ctx['selected']['ratePerClient'],i)
        argv,out,classes=bg_args(ctx,root,ep,name,offsets,trigger)
        arrivals[name]=(argv,out,offsets,classes)
    with ExitStack() as manager:
        procs={name:manager.enter_context(owned_client(argv,root/(name+'-stdout.log')))
               for name,(argv,_,_,_) in arrivals.items()}
        finish=time.monotonic()+40
        while not all((x[1]/'ready.json').is_file() for x in arrivals.values()):
            assert time.monotonic()<finish and all(p.poll() is None for p in procs.values())
            time.sleep(.03)
        fg=root/'foreground';args,out=fg_args(ctx,fg,Path(ep['manifest']))
        mon=b.common.CpuMonitor(root/'datanode-cpu.jsonl',ep['pids']);mon.start()
        try:
            p=manager.enter_context(owned_client(args,fg/'stdout.log'))
            witness=read_live_witness(out,p,ctx['phase'],width)
            put(trigger.with_suffix('.pending'),witness);trigger.with_suffix('.pending').rename(trigger)
            code=p.wait(timeout=550)
            put(fg/'exit.json',{'exitCode':code})
            actual={}
            for name,po in procs.items():
                bg_code=po.wait(timeout=310)
                _,directory,offsets,classlog=arrivals[name]
                put(root/(name+'-exit.json'),{'exitCode':bg_code})
                assert 'FluctuatingBackgroundQueryDriver source: file:'+str(ctx['bg']/'classes') in classlog.read_text()
                actual[name]=c.read_driver(directory,ctx['bgOracle'][name],offsets,witness['originMonotonicNanos'],name,ctx['bg'])
                assert bg_code==0 or actual[name]['errors'] or actual[name]['rejected']
            last=max((x['lastDrainNanos'] or 0 for x in actual.values()),default=0)
            settle=max(witness['originMonotonicNanos']+ctx['phase'].OFFER_END,last)
            while time.monotonic_ns()<settle+7*N: time.sleep(.1)
        finally:mon.close()
    assert code==0,'Foreground did not return a complete result'
    proof=verify_fg(ctx,fg,out,width)
    assert proof['layoutSha256']==layout
    phases=ctx['phase'].verify(out,witness,last,width,on)
    times=[t for x in actual.values() for t in x['latencies']]
    offered=sum(len(x[2]) for x in arrivals.values())
    rejected=sum(x['rejected'] for x in actual.values())
    errors=sum(len(x['errors']) for x in actual.values())
    assert len(times)+rejected+errors==offered and offered>0
    origin=witness['originMonotonicNanos']
    cpu=c.phase_cpu(root/'datanode-cpu.jsonl',origin,settle+7*N)
    p95=c.pct(times)
    assert (width,on) in ((2,False),(8,True))
    readiness=pressure_readiness(out,phases,ctx['phase'],width)
    qualified=phases['fullPhaseCoverage'] and not rejected and not errors and len(times)==offered and proof['audit']['commonActiveSeconds']>=37
    ceiling=any(x['maxConsecutiveEligible']>=2 for x in readiness.values())
    complete=not rejected and not errors and len(times)==offered
    eligible=complete and last<=origin+ctx['phase'].LATEST_DRAIN and p95<=ctx['p95GateSeconds']
    result={'status':'VERIFIED_DELAYED_SELECTOR_TRIAL','actualConfiguredFeedback':on,'configuredStartup':width,
            'observedConservativePressureProxy':ceiling,'meetsStandaloneGate':bool(eligible),
            'offerToDrainDefinition':'planned monotonic timestamp to full typed result delivery',
            'p95GateSeconds':ctx['p95GateSeconds'],'lastBackgroundDrainOffsetSeconds':(last-origin)/N,
          'pressureReadinessObservations':readiness,
          'qualifiedCoverageAndCorrectness':bool(qualified),'elapsedSeconds':proof['elapsedSeconds'],
          'backgroundP95Seconds':p95,'backgroundCompleted':len(times),'backgroundOffered':offered,
          'backgroundRejected':rejected,'backgroundErrors':errors,'background':actual,
          'observedPressureAndWorkers':phases,'foregroundProof':proof,'cpuTriggerToRecoveryEnd':cpu,
          'backgroundPreflight':background_preflight}
    put(root/'summary.json',result)
    return result

class StorageBeforeService(RuntimeError):
    pass

def run_cell(ctx,leaf,profile,reference):
    v,b,h,release=ctx['volume'],ctx['b'],ctx['h'],ctx['release']
    width,on=profile['initial'],profile['sharedPressureFeedback']
    live_storage=v.headroom(10*v.GIB,0)
    if not live_storage['allowed']:raise StorageBeforeService(live_storage)
    b.h.D=leaf
    mf,starts=place(ctx,leaf,profile)
    owned=[]
    runtime={}
    put(leaf/'prelaunch-binding.json',{'profile':profile,'entrySha256':sha(__file__),'planSha256':sha(PLAN),
        'preRegisteredCalibration':True,'heapDiagnosticPreregSha256':PILOT_SHA,'service':{
          n['name']:sha(root/'identity/prelaunch.json') for n,root,_,_,_ in starts}})
    try:
        with ExitStack() as stack:
            try:
                for n,root,argv,port,classes in starts:
                    log=stack.enter_context((root/'logs/service.log').open('x'))
                    proc=subprocess.Popen(argv,cwd=root,stdout=log,stderr=subprocess.STDOUT,stdin=subprocess.DEVNULL,start_new_session=True)
                    n['pid']=proc.pid;owned.append(n);h.wait_up(b,n,proc,port,argv)
                    klass='org.apache.iotdb.db.service.DataNode' if n['role']=='DataNode' else 'org.apache.iotdb.confignode.service.ConfigNode'
                    events=b.own_class(n,classes,klass,release)
                    descriptors=set()
                    for fd in Path('/proc',str(proc.pid),'fd').iterdir():
                        try: dest=os.readlink(fd)
                        except FileNotFoundError: continue
                        if dest.startswith('/') and '/logs/' in dest:
                            descriptors.add(dest)
                    assert descriptors and all(Path(dest).is_relative_to(root/'logs') for dest in descriptors), (n['name'],sorted(descriptors))
                    tag='log_datanode_all.log' if n['role']=='DataNode' else 'log_confignode_all.log'
                    assert str(root/'logs'/tag) in descriptors, (tag,sorted(descriptors))
                    put(root/'identity/live-log-paths.json',{'actualPid':proc.pid,'logDescriptors':sorted(descriptors)})
                    maps=(root/'identity/proc-maps.txt')
                    maps.write_bytes(Path('/proc',str(proc.pid),'maps').read_bytes())
                    if n['role']=='DataNode':
                        assert argv.count('-Xmx24g')==1 and b.argv_of(proc.pid)==[os.fsencode(x) for x in argv]
                        observed=re.findall(r'initial allocateMemoryForRead = (\d+)',(root/'logs/service.log').read_text())
                        assert observed==['7730941132'], (n['name'],observed)
                        n['observedHeapBinding']={'xmxGiB':24,'initialReadBudgetBytes':int(observed[0])}
                    runtime[n['name']]={'loaded':events,'mapsSha256':sha(maps),'configSha256':sha(n['configPath']),
                        'javaSha256':sha(Path('/proc',str(proc.pid),'exe').resolve()),'heap':n.get('observedHeapBinding'),'logBindingSha256':sha(root/'identity/live-log-paths.json')}
                assert all(b.tsfiles(Path(n['nodeRoot'])/'data')==ctx['tsfiles'][n['name']] for n in owned)
                manifest=leaf/'effective.json';put(manifest,mf)
                ep={'manifest':str(manifest),'manifestSha256':sha(manifest),'jdbcUrl':mf['jdbcUrl'],'pids':[n['pid'] for n in mf['nodes']]}
                put(leaf/'preexecution.json',{'profile':profile,'entrySha256':sha(__file__),'planSha256':sha(PLAN),
                    'preRegisteredCalibration':True,'heapDiagnosticPreregSha256':PILOT_SHA,'sourceTsFileSetSha256':ctx['sourceTsFileSetSha256'],
                    'manifestSha256':ep['manifestSha256'],'bindings':runtime,
                    'controlPlane':{n['name']:sha(Path(n['nodeRoot'])/'identity/prelaunch.json') for n in owned}})
                warm=leaf/'prime';argv,out=fg_args(ctx,warm,manifest)
                with owned_client(argv,warm/'stdout.log') as p:
                    rc=p.wait(timeout=550)
                put(warm/'exit.json',{'exitCode':rc});assert rc==0
                wk=verify_fg(ctx,warm,out,width)
                if 'fg' in reference:assert wk['layoutSha256']==reference['fg']
                else:reference['fg']=wk['layoutSha256']
                put(warm/'proof.json',wk)
                probes={}
                for name in ctx['selected']['shapes']:
                    dest=leaf/('background-plan-'+name)
                    z=ctx['cal'].probe(ep,b,ctx['cli'],ctx['classpath'],ctx['bgOracle'],name,dest)
                    assert z['eligibleAsInvariantBackground'],(name,'native background is not invariant')
                    layout=ctx['phase'].digest(ctx['phase'].normalized_roster(dest/'ordinary-sql/roster.json'))
                    if name in reference:assert layout==reference[name],(name,'background layout differs')
                    else:reference[name]=layout
                    probes[name]={'layoutSha256':layout,'proof':z}
                return trial(ctx,leaf,ep,width,on,probes,wk['layoutSha256'])
            finally:
                b.clean_clients(leaf);b.h.stop_owned(leaf,owned,'stopped')
    finally:
        assert all(b.tsfiles(Path(n['nodeRoot'])/'data')==ctx['tsfiles'][n['name']] for n,_,_,_,_ in starts)

def pressure_readiness(path,proof,phase,width):
    """Use complete, source-bound observations; this does not establish a controller action."""
    roster=phase.source_roster(path/'roster.json')
    start=proof['originMonotonicNanos']
    high=(start+phase.OFFER_START,start+phase.OFFER_END)
    rows={n:[] for n in roster.values()}
    for item in phase.lines(path/'runtime-live-timeline.jsonl'):
        native=phase.live(item,width)
        if not native:continue
        fi,n,t,total=native
        if fi not in roster or not high[0]<=t<high[1]:continue
        assert roster[fi]==n
        ob=item['response']['observation']
        p=ob['schedulerPressure']; group,=ob['groups'].values()
        slots=p['configuredQueryThreads'];assert slots==8
        ready=p['other']['runnableReady']; busy=p['own']['running']+p['other']['running']
        cap=max(1,slots-min(max(1,slots//2),total))
        active=group['lastObservedWorkers']
        rows[n].append((t,ready,busy,active,cap))
    out={}
    for n,batch in rows.items():
        prev=None;streak=0;qualified=0
        raw=[]
        for t,ready,busy,active,cap in sorted(set(batch)):
            assert prev is None or t>prev
            hit=ready>0 and busy>=8 and active>cap
            streak=(streak+1 if prev is not None and t-prev<=int(1.5*N) else 1) if hit else 0
            qualified=max(qualified,streak)
            raw.append({'captureNanos':t,'externalReady':ready,'busySlots':busy,'activeWidth':active,'hypotheticalCap':cap,'eligible':hit})
            prev=t
        out[n]={'maxConsecutiveEligible':qualified,'samples':raw}
    return out


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
    assert all(not os.environ.get(key) for key in ('PYTHONPATH','CLASSPATH','JAVA_TOOL_OPTIONS','_JAVA_OPTIONS','JDK_JAVA_OPTIONS'))
    assert Path('/proc/self/exe').resolve()==Path('/opt/conda/bin/python3.12')
    assert all(sha(p)==h for p,h in PINS.items())
    assert sha(PILOT)==PILOT_SHA
    assert sha(BASE_DRIVER/'run_london_two_pulse_background_gate_20261119.py')=='68c91e22d10ffeae56d9384c1fdcaaa25d7a15e89820b29bb08a3c05e29e5683'
    pilot=read(PILOT)
    assert pilot['status']=='FROZEN_BEFORE_TWO_PULSE_CALIBRATION'
    assert len(sys.argv)==2 and sha(THRESHOLD)==sys.argv[1]==PARENT_THRESHOLD_SHA
    assert sha(GATE)=='338d06eaf6f63001a0ce3f5d6ea8fc72c77e6fce485e3a55a0a9a46305705532'
    threshold=read(THRESHOLD)
    assert threshold['status']=='LONDON_DELAYED_SELECTOR_BACKGROUND_ONLY_THRESHOLD_FROZEN'
    assert sha(CALIBRATION_SUMMARY)==threshold['inputNativeSummarySha256'] and sha(GATE)==threshold['preregistrationSha256']
    records=threshold['perTrial']
    assert len(records)==4 and all(x['qualified'] and x['completed']==120 and x['serverFailed']==x['clientRejected']==0 for x in records.values())
    assert threshold['gateP95Seconds']==max(x['p95Seconds'] for x in records.values())*1.25
    gate=read(GATE)
    assert gate['status']=='REPEATED_QUALIFICATION_REQUIRED_AND_BACKGROUND_GATE_FROZEN'
    assert gate['target']==str(CALIBRATION_SUMMARY.parent) and gate['inputTrialPlanSha256']==sha(PILOT)
    assert gate['gateEntrySha256']==sha(CALIBRATION)
    assert sha(QUALIFIED)==gate['independentQualificationSha256']
    q=read(QUALIFIED)
    assert q['status']=='SCALAR_TWO_PULSE_WAVE_INDEPENDENTLY_RECOMPUTED'
    assert q['nativeTerminalStatus']=='TWO_PULSE_READY_FOR_INDEPENDENT_BACKGROUND_ONLY_GATE'
    assert q['selectedRatePerClient']==4 and not q['treatmentEnabled']
    assert len(q['cases'])==2 and all(r['qualifies'] and r['background']['passed']==120 for r in q['cases'].values())
    assert gate['multiplicativeThreshold']==1.25 and gate['minimumTrials']==4
    burst=R/'datasets/london-scalar-front-loaded-evidence-01/scalar-front-loaded-adjudication.json'
    assert sha(burst)==pilot['parentFrontLoadedAuditSha256']
    assert sha(BASE_DRIVER.joinpath('run_london_scalar_front_loaded_20261119.py'))==pilot['parentFrontLoadedDriverSha256']
    assert [r['background']['clientRejected'] for r in read(burst)['cases'].values()]==[2,1]
    assert pilot['protocol']['maximumInFlightPerClient']==20 and gate['maximumInFlightPerClient']==20
    assert sha(BASE_DRIVER.joinpath('run_london_scalar_two_pulse_20261119.py'))==gate['parentTrialDriverSha256']
    early=R/'datasets/london-early-wave-and-warm-projection-evidence-01/scalar-early-wave-adjudication.json'
    assert sha(early)==pilot['parentEarlyWaveAuditSha256']
    assert sha(BASE_DRIVER.joinpath('run_london_scalar_early_wave_20261119.py'))==pilot['parentEarlyWaveDriverSha256']
    parent=R/'datasets/london-scalar-arrival-screen-evidence-01/scalar-arrival-adjudication.json'
    assert sha(parent)==pilot['parentScalarAuditSha256'] and sha(PHASE)==pilot['earlyPhaseDriverSha256']
    assert read(parent)['nativeSummarySha256']==pilot['parentScalarNativeSummarySha256']
    assert sha(BASE_DRIVER.joinpath('run_london_scalar_arrival_screen_20261119.py'))==pilot['parentScalarArrivalDriverSha256']
    tariff=R/'datasets/london-tariff-plan-diagnostic-evidence-01/tariff-plan-adjudication.json'
    native=R/'datasets/london-scalar-native-background-evidence-01/scalar-native-plan-adjudication.json'
    assert sha(tariff)==pilot['tariffPlanAdjudicationSha256'] and sha(native)==pilot['scalarNativeAdjudicationSha256']
    assert sha(BASE_DRIVER.joinpath('run_london_scalar_background_native_plan_20261119.py'))==pilot['nativeProbeDriverSha256']
    assert sha(BASE_DRIVER.joinpath('run_london_tariff_pressure_diagnostic_r2_20261119.py'))==pilot['tariffDriverSha256']
    assert all(sha(BG/name)==h for name,h in pilot['background']['files'].items())
    assert sha(BASE_DRIVER.joinpath('run_london_short_wave_heap24_diagnostic_20261119.py'))==pilot['sameHeapDriverSha256']
    anchor=M/'sxing-iotdb-london-heap24-adjudication-20261119-01/heap24-diagnostic-adjudication.json'
    assert sha(anchor)==pilot['heap24AdjudicationSha256']
    assert sha(PLAN)==pilot['parentPreregistrationSha256']
    past=M/'sxing-iotdb-london-natural-short-wave-calibration-20261119-02/rate-4/rate-screen.json'
    assert sha(past)==pilot['historicalRate4ScreenSha256']
    assert sha(BASE_DRIVER.joinpath('continue_london_natural_short_wave_calibration_20261119.py'))==pilot['parentDriverSha256']
    info=read(PLAN)
    assert info['status']=='FROZEN_BEFORE_NEW_LOAD_CALIBRATION_OR_TREATMENT'
    assert info['startingEvidence']['commonActiveMinSecondsForNewCohort']==37
    cal,phase,boot=[load(q) for q in (CAL,PHASE,BOOT)]
    assert all(sha(p)==x for p,x in boot.PINS.items())
    exact=boot.load(boot.FULL_REPLAY,'london_fine_typed_full_result_topology')
    v=scratch_storage(boot.load(boot.VOLUME,'london_short_storage'))
    d=boot.load(boot.DRIVER,'bound_natural_london_preflight')
    cal.engine=cal.load(cal.BAG);cal.auditor=boot.load(boot.AUDIT,'natural_london_background')
    schedule=info['arrivalProtocol']
    assert (schedule['offerStartSeconds'],schedule['offerStopSecondsExclusive'],schedule['recoverySeconds'])==(5,20,7)
    assert pilot['protocol']['fixedPhaseOffsetsNanos']==[0,phase.OFFER_START,phase.OFFER_END,phase.LATEST_DRAIN,7*N]
    assert sha(ARRIVAL)==gate['frozenSelectorArrivalSha256']=='63e60bfd23ab5a8b1cfbc2b0f236bef57a8e270a468ab7b9857d320afef22f9d'
    assert sha(SELECTORS)==gate['frozenSelectorConfigSha256']=='d8d5e3e952036e568779bbb15805bcb189ab03b8611c5320b838a3f8f5983e5e'
    frozen=read(ARRIVAL);selectors=read(SELECTORS);arms=selectors['candidateRows']
    assert frozen['selectorFreezeSha256']==sha(SELECTORS)
    assert [(arms[k]['profile'],arms[k]['actualStartupWorkersPerSource'],arms[k]['sharedPressureFeedback'])
            for k in ('joint','sequential')]==[('start8-on',8,True),('start2-off',2,False)]
    assert frozen['blockOrders']==[['sequential','joint'],['joint','sequential'],
                                   ['joint','sequential'],['sequential','joint']]
    assert frozen['serviceLimits']=={'maxInFlightPerClient':20,'offerCutoffSeconds':16.5,
           'drainDeadlineSeconds':26.5,'recoveryCoverageSeconds':7,'queryThreadsPerDataNode':8,'heapGiBPerDataNode':24}
    frozen_offsets=frozen['offeredTimesNanosByClient']
    assert 8_100_000_000 in frozen_offsets[0]
    assert frozen_offsets!=pilot['protocol']['arrivalOffsetsNanosByClient']
    assert len(frozen_offsets)==2
    for q in frozen_offsets:
        assert len(q)==60 and q==sorted(set(q)) and phase.OFFER_START<=q[0]<q[-1]<phase.OFFER_END
    def offsets(r,idx):
        assert r==4 and idx in (0,1)
        return frozen_offsets[idx]
    cal.offsets=offsets
    screen=read(QA/'independent-capacity-screen.json')
    e=info['startingEvidence']
    assert e['exactScreenSha256']==sha(QA/'independent-capacity-screen.json') and screen['DataNodeMaxHeapGiB']==12
    assert not screen['eligibleForBackgroundOnlyCalibration']
    assert screen['entrySha256']==sha(BOOT)
    assert [screen['repetitions'][f'measure-0{i}']['commonActiveSeconds'] for i in (1,2,3)]==e['idleCommonActiveSeconds']
    truth_receipt=read(boot.TRUTH/'summary.json')
    sql=boot.TRUTH/'five-centikwh-histogram.sql'
    truth=Path(truth_receipt['oracle'])
    assert sha(sql)==screen['candidateSqlSha256']==e['sqlSha256']
    assert sha(truth)==truth_receipt['oracleSha256']==screen['independentOracleSha256']
    assert (truth_receipt['rawRows'],truth_receipt['groups'])==(e['exactRawRows'],e['exactGroups'])==(167932474,2898043)
    trial_input=Path(screen['archivedRawOnScratch'])/'cache-prime'
    t=trial_input/'params.json'
    assert sha(t)==read(trial_input/'preexecution.json')['parametersSha256']
    probe_template=read(t)
    assert probe_template['rosterPollMs']==250 and probe_template['minimumRuntimeTimelineGroups']==0
    src=boot.S/'fixed-eight-histogram15-deployment.json'
    assert sha(src)==e['sourceServiceManifestSha256']
    seal=QA/'deployment-control-plane.json'
    assert sha(seal)==screen['deploymentControlPlaneReceiptSha256']
    file_index={x['path']:x['sha256'] for x in read(seal)['files']}
    assert sha(src)==file_index[src.name]
    static_files=boot.LINEAGE/'physical-tsfile-identities.json'
    assert sha(static_files)==read(QA/'after-shutdown-tsfile-invariance.json')['physicalFilesSha256']
    files=read(static_files)
    old=read(src)
    assert old['runtimeProfile']['fixedWorkers']==8
    assert sha(cal.BG/'manifest.json')==info['backgroundCalibration']['oracleManifestSha256']
    cal.BG=BG
    f=cal.BG/'manifest.json'
    assert sha(f)==pilot['background']['oracleManifestSha256']
    names=pilot['background']['newFamily']
    oracle=read(f)['shapes']
    assert names==['january-scalar-1w','july-scalar-1w'] and all(oracle[n]['resultRows']==1 for n in names)
    for name in names:
        assert sha(cal.BG/(name+'.sql'))==oracle[name]['sqlSha256']
        assert sha(cal.BG/(name+'.jsonl'))==oracle[name]['oracleSha256']
    reserve=v.headroom(32*v.GIB,0)
    memory=boot.memory_snapshot()
    memory['requiredHeadroomBytes']=pilot['factor']['requiredCgroupHeadroomAfterReclaimableFileCacheBytes']
    assert reserve['allowed'] and memory['headroomAfterReclaimableFileCacheBytes']>=memory['requiredHeadroomBytes'], (reserve,memory)
    assert v.make_workspace(WORKNAME)==O
    d.O=O
    b,h,_,rel,_,_,_,_,_,_,_,_,cli,cp,_=d.preflight()
    assert old['runtimeJars']==rel['jars'] and sha(b.JAVA)==old['javaSha256']
    assert all(rel[k]==v for k,v in pilot['release'].items())
    assert sha(sql)==pilot['same']['nativeQuerySqlSha256'] and sha(truth)==pilot['same']['foregroundOracleSha256']
    oldnodes=old['configNodes']+old['nodes']
    assert len(oldnodes)==3 and all(not b.h.current_process(n,n['pid']) for n in oldnodes)
    assert {n['name']:b.tsfiles(Path(n['nodeRoot'])/'data') for n in oldnodes}==files
    bg=b.pilot.BG
    identities=read(F/'matched-feedback-mechanism-02/client-runtime-identities.json')[str(bg)]
    assert all(sha(bg/k)==d for k,d in identities.items())
    assert all(sha(z['path'])==z['sha256'] for z in read(bg/'class-files.json'))
    cp2=(bg/'classpath.txt').read_text().strip()
    of=b.B/'scheduler-pressure-release-20261118T060953Z/release-scheduler-pressure.json'
    assert sha(of)==b.PIN
    jo={u['path']:u['sha256'] for u in read(of)['jars']}
    cl=cp2.split(':')
    assert cl[0]==str(bg/'classes') and all(sha(p)==jo[p] for p in cl[1:])
    runtime=Path('/proc/self/exe').resolve()
    put(O/'preexecution.json',{'entry':str(Path(__file__).resolve()),'entrySha256':sha(__file__),
        'driverBindings':{str(p):x for p,x in PINS.items()},'release':rel,'configuredRateCandidates':[4],
        'pythonRuntime':{'executable':str(runtime),'sha256':sha(runtime),'sysPath':sys.path,
                         'pythonpath':os.environ.get('PYTHONPATH')},'host':os.uname().nodename,
        'heapDiagnosticPreregistration':str(PILOT),'heapDiagnosticPreregistrationSha256':PILOT_SHA,
        'directParentDriverSha256':pilot['sameHeapDriverSha256'],'heap24AdjudicationSha256':sha(anchor),
        'tariffPlanAdjudicationSha256':sha(tariff),'scalarNativeAdjudicationSha256':sha(native),
        'newBackgroundOraclePath':str(BG),'newBackgroundOracleSha256':sha(f),'historicalRate4Sha256':sha(past),
        'snapshotTsFileSetSha256':sha(static_files),'sourceProfileManifestSha256':sha(src),
        'foregroundTemplateSha256':sha(t),'sqlSha256':sha(sql),'truthSha256':sha(truth),
        'backgroundClientIdentities':identities,'backgroundClasspath':cp2,'javaSha256':sha(b.JAVA),
        'memory':memory,'storage':reserve,'phaseWindow':'0/1.5/16.5/26.5+7','selectorHoldout':True,'priorScalarAuditSha256':sha(parent),'priorEarlyWaveAuditSha256':sha(early),'frozenOffsetsNanos':frozen_offsets,'maximumInFlightPerClient':pilot['protocol']['maximumInFlightPerClient'],'priorFrontLoadedAuditSha256':sha(burst),
        'frozenSelectorArrivalSha256':sha(ARRIVAL),'selectorFreezeSha256':sha(SELECTORS),
        'thresholdSha256':sha(THRESHOLD),'calibrationPlanSha256':sha(GATE),'independentQualificationSha256':sha(QUALIFIED),
        'runScope':'Joint versus sequential selectors, not an isolated feedback switch. Earlier trace anchors identity only.'})
    ctx={'b':b,'h':h,'release':rel,'volume':v,'boot':boot,'parent':old,'control':file_index,'tsfiles':files,
         'sourceTsFileSetSha256':sha(static_files),'phase':phase,'exact':exact,'cal':cal,'bgOracle':oracle,'bg':bg,'cli':cli,'classpath':cp,
         'backgroundClassPath':cp2,'sql':sql,'truth':truth,'foregroundTemplate':probe_template,'maximumInFlight':pilot['protocol']['maximumInFlightPerClient']}
    native_probes=read(native)
    reference={n:t['layoutSha256'] for n,t in native_probes['results'].items()}
    assert native_probes['status']=='BOTH_SCALAR_BACKGROUND_SHAPES_INDEPENDENTLY_QUALIFIED'
    ctx['selected']={'ratePerClient':4,'shapes':names}
    ctx['p95GateSeconds']=threshold['gateP95Seconds']
    blocks={}
    for i,order in enumerate(frozen['blockOrders'],1):
        block=O/f'pair-{i:02d}';block.mkdir()
        rows={}
        for position,which in enumerate(order,1):
            arm=arms[which];width=arm['actualStartupWorkersPerSource'];on=arm['sharedPressureFeedback']
            leaf=block/f'{position:02d}-{which}';leaf.mkdir()
            put(leaf/'assignment.json',{'block':block.name,'position':position,'selector':which,**arm})
            try:
                m=boot.memory_snapshot()
                assert m['headroomAfterReclaimableFileCacheBytes']>=memory['requiredHeadroomBytes']
                put(leaf/'prelaunch-memory.json',m)
                x=run_cell(ctx,leaf,{'initial':width,'sharedPressureFeedback':on},reference)
                mm=boot.memory_snapshot();put(leaf/'postshutdown-memory.json',mm)
                assert mm['events']['oom_kill']==m['events']['oom_kill']
                assert x['backgroundOffered']==120
            except StorageBeforeService as reason:
                x={'status':'PRE_SERVICE_STORAGE_GUARD','storage':reason.args[0]}
            except Exception:
                failure=leaf/'runner-failure.txt';failure.write_text(traceback.format_exc())
                x={'status':'INCOMPLETE_DELAYED_SELECTOR_TRIAL','failureSha256':sha(failure)}
            x={**x,'selector':which,'profile':arm['profile'],'configuredStartup':width,
                 'actualConfiguredFeedback':on}
            stamp=leaf/'trial-receipt.json';put(stamp,x)
            rows[which]={'role':leaf.name,'profile':arm['profile'],'status':x['status'],
                         'qualifiedCoverageAndCorrectness':x.get('qualifiedCoverageAndCorrectness'),
                         'gateMet':x.get('meetsStandaloneGate'),'completed':x.get('backgroundCompleted'),
                         'rejected':x.get('backgroundRejected'),'errors':x.get('backgroundErrors'),
                         'foregroundSeconds':x.get('elapsedSeconds'),'backgroundP95Seconds':x.get('backgroundP95Seconds'),
                         'receiptSha256':sha(stamp)}
            print('LONDON_DELAYED_SELECTOR',i,which,rows[which],flush=True)
        blocks[block.name]=rows
    report=O/'summary.json'
    put(report,{'status':'FOUR_FROZEN_SELECTOR_PAIRS_FINISHED_PENDING_INDEPENDENT_AUDIT','blocks':blocks,
         'selectorFreezeSha256':sha(SELECTORS),'arrivalPlanSha256':sha(ARRIVAL),'thresholdSha256':sha(THRESHOLD),
         'preexecutionSha256':sha(O/'preexecution.json'),'p95GateSeconds':ctx['p95GateSeconds'],
         'scope':'All assigned cells preserved. Configurations differ in startup and feedback. No verified effect in this native summary.'})
    print(sha(report),read(report)['status'],flush=True)


if __name__=='__main__':
    with (F/'local-timed-experiments.lock').open('rb') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        main()
