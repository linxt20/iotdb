#!/usr/bin/env python3
# Licensed under the Apache License, Version 2.0. See https://www.apache.org/licenses/LICENSE-2.0
"""B6 five core mutation contracts plus round-robin traces; frozen new baseline only."""
from pathlib import Path
import argparse
import difflib
import hashlib
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import zipfile
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
FLAGS = ['-Dspotless.check.skip=true', '-Drat.skip=true', '-Dlicense.skip=true',
         '-Dmaven.javadoc.skip=true', '-Dcheckstyle.skip=true', '-Denforcer.skip=true',
         '-DfailIfNoTests=false']
REQUIRED = {'core1_duplicate_partition', 'core2_ordered_root_collect', 'core3_omitted_queue',
            'core4_skipped_first_driver', 'core4_deregister_account', 'core5_nominal_as_cap',
            'rr_constructor_threshold', 'rr_no_candidate_forced_switch'}
ASSERTIONS = {'java.lang.AssertionError', 'org.junit.ComparisonFailure',
              'junit.framework.AssertionFailedError', 'org.junit.internal.ArrayComparisonFailure'}


def require(value, message):
    if not value:
        raise AssertionError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            digest.update(block)
    return digest.hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2, ensure_ascii=False)
        stream.write('\n')


def bound(item):
    path = Path(item['path'])
    require(path.is_absolute() and path.is_file() and sha(path) == item['sha256'], 'Identity differs: ' + str(path))
    return path


def validate_work_root(work, pvc_root):
    require(work.is_absolute() and pvc_root.is_absolute(), "Work and PVC paths must be absolute")
    require(not work.exists() and not work.is_symlink(), "Preserve history: work root must be new")
    pvc = pvc_root.resolve(strict=True)
    parent = work.parent.resolve(strict=True)
    require(parent.is_relative_to(pvc), "Source/target/temp must stay inside the bound PVC task root")
    require(parent.stat().st_dev == pvc.stat().st_dev, "Work parent escaped the bound PVC device")
    filesystem = subprocess.check_output(["stat", "-f", "-c", "%T", str(parent)], text=True).strip()
    require(filesystem != "tmpfs", "This runner requires persistent PVC, not tmpfs")
    return {"path": str(work), "parentRealpath": str(parent), "pvcRealpath": str(pvc),
            "filesystem": filesystem, "device": parent.stat().st_dev}


def method_name(value):
    # PowerMock/Surefire display decorations do not create extra logical methods.
    return re.split(r'[\[(]', value or '', maxsplit=1)[0]


def xml_contract(reports, targets, negative):
    cases = []
    for path in sorted(Path(reports).glob('TEST-*.xml')):
        root = ET.parse(path).getroot()
        for item in root.iter('testcase'):
            children = [child for child in item if child.tag in ('failure', 'error', 'skipped', 'flakyFailure', 'flakyError', 'rerunFailure', 'rerunError')]
            cases.append({'class': item.get('classname'), 'method': method_name(item.get('name')),
                          'displayName': item.get('name'), 'xml': str(path),
                          'completeXmlNode': ET.tostring(item, encoding='unicode'),
                          'diagnostics': [{'tag': child.tag, 'type': child.get('type'),
                                           'message': child.get('message', ''), 'text': child.text or ''}
                                          for child in children]})
    require(cases, 'No per-class Surefire XML; JVM/compile errors never count as a mutation kill')
    wanted = {(target['class'], target['method']) for target in targets}
    require(len(wanted) == len(targets), 'Duplicate requested logical test method')
    for case in cases:
        if (case['class'], case['method']) not in wanted:
            require(all(item['tag'] == 'skipped' for item in case['diagnostics']), 'Unexpected non-target failure/error')
    for target in targets:
        matches = [case for case in cases if case['class'] == target['class'] and case['method'] == target['method']]
        if len(matches) == 2 and negative and target.get("allowPowerMockFailureFinishArtifact") is True:
            failures = [case for case in matches if len(case["diagnostics"]) == 1
                        and case["diagnostics"][0]["tag"] == "failure"]
            empty_success = [case for case in matches if not case["diagnostics"]]
            require(len(failures) == len(empty_success) == 1
                    and len({case["xml"] for case in matches}) == 1
                    and len({case["displayName"] for case in matches}) == 1,
                    "PowerMock finish artifact must be one failure plus one identical empty-success event in one XML")
            failures[0]["providerEventRole"] = "SOLE_EXPECTED_ASSERTION_FAILURE"
            empty_success[0]["providerEventRole"] = "POWERMOCK_EMPTY_SUCCESS_FINISH_ARTIFACT"
            matches = failures
        require(len(matches) == 1, 'Missing or duplicate unique logical method: ' + str(target))
        diagnostics = matches[0]['diagnostics']
        if negative:
            require(len(diagnostics) == 1 and diagnostics[0]['tag'] == 'failure'
                    and diagnostics[0]['type'] in ASSERTIONS, 'Mutant did not hit the specified JUnit assertion')
            diagnostic = diagnostics[0]
            require(re.search(target['assertionMessageRegex'], diagnostic['message']), 'Wrong assertion message')
            require(re.search(target['assertionStackRegex'], diagnostic['text']), 'Wrong assertion source/stack')
        else:
            require(not diagnostics, 'Baseline/restored logical method did not pass')
    return cases


def mutate(original, replacements):
    changed = original
    for replacement in replacements:
        require(changed.count(replacement['old']) == 1, 'Mutation anchor is not unique in the bound source')
        changed = changed.replace(replacement['old'], replacement['new'], 1)
    require(changed != original, 'Mutation did not change executable source')
    return changed


def validate(manifest):
    require(manifest['bindingState'] == 'BOUND_NEW_CANONICAL_BASELINE_AND_CARRIERS_PASS',
            'Bind the newly frozen canonical source/package and actual carrier baseline PASS first')
    for key in ('sourceArchive', 'sourceManifest', 'package', 'baselineTestProvenance', 'storageIdentity'):
        bound(manifest[key])
    source = read(manifest['sourceManifest']['path'])
    with zipfile.ZipFile(manifest['package']['path']) as package:
        entries = [name for name in package.namelist() if name.endswith('/lib/iotdb-server-2.0.11-SNAPSHOT.jar')]
        require(len(entries) == 1 and hashlib.sha256(package.read(entries[0])).hexdigest()
                == manifest['serverJarSha256'], 'Actual frozen package server JAR identity differs')
    provenance = read(manifest['baselineTestProvenance']['path'])
    require(provenance['status'] == 'PASS'
            and provenance['sourceManifestSha256'] == manifest['sourceManifest']['sha256']
            and provenance['serverJarSha256'] == manifest['serverJarSha256'], 'Baseline test provenance is stale')
    require({case['id'] for case in manifest['cases']} == REQUIRED and len(manifest['cases']) == len(REQUIRED),
            'All original five core contracts, independent deregistration gate and both RR trajectories are required')
    for case in manifest['cases']:
        require(case['sourceSha256'] == source[case['source']], 'Mutation source is not in the frozen baseline')
        require(case['replacements'] and case['tests'] and case['requiredLoadedClasses'], 'Incomplete mutation carrier')
        for target in case['tests']:
            require(target['sourceSha256'] == source[target['source']], 'Carrier source is not in the frozen baseline')
            require(provenance['testSourceSha256'][target['source']] == target['sourceSha256'],
                    'Current carrier lacks actual matching compiled-test provenance')
    return source


class Runner:
    def __init__(self, manifest, work, output, source_manifest):
        self.m, self.work, self.output, self.source_manifest = manifest, work, output, source_manifest
        self.source = work / 'source'
        self.module = self.source / 'iotdb-core/datanode'
        self.target = self.module / 'target'
        self.java = bound(manifest['java'])
        self.javac = bound(manifest['javac'])
        self.maven = bound(manifest['maven'])
        self.logs = work / 'evidence'
        self.logs.mkdir()
        self.temp = work / 'tmp'
        self.temp.mkdir()
        self.originals = {}
        self.repository_context = {
            'branch': subprocess.check_output(['git', '-C', '/root/iotdb', 'branch', '--show-current'], text=True).strip(),
            'head': subprocess.check_output(['git', '-C', '/root/iotdb', 'rev-parse', 'HEAD'], text=True).strip(),
            'workingTreeStatus': subprocess.check_output(['git', '-C', '/root/iotdb', 'status', '--porcelain'], text=True),
            'identityScope': 'Repository context only; actual executed source is the separately SHA-verified frozen archive'}
        self.java_version = subprocess.run([self.java, '-version'], text=True, capture_output=True, check=True).stderr

    def command(self, label, command, env=None, timeout=300):
        directory = self.logs / label
        directory.mkdir(parents=True)
        selected = dict(os.environ)
        selected.pop('JAVA_TOOL_OPTIONS', None)
        selected.pop('_JAVA_OPTIONS', None)
        selected.update({
            "JAVA_HOME": str(self.java.parent.parent),
            "PATH": str(self.java.parent) + os.pathsep + selected.get("PATH", ""),
            "TMPDIR": str(self.temp), "TMP": str(self.temp), "TEMP": str(self.temp),
            "MAVEN_OPTS": "-Xmx2g -Djava.io.tmpdir=" + str(self.temp),
            "_JAVA_OPTIONS": "-Xmx2g -Djava.io.tmpdir=" + str(self.temp)})
        selected.update(env or {})
        save(directory / 'execution-before.json', {'command': list(map(str, command)), 'cwd': str(self.source),
             'driverPath': str(HERE / 'run-negative-contracts.py'), 'driverSha256': sha(HERE / 'run-negative-contracts.py'),
             'java': self.m['java'], 'javac': self.m['javac'], 'maven': self.m['maven'],
             'javaVersion': self.java_version, 'repositoryContext': self.repository_context,
             'frozenBaselineTestProvenance': self.m['baselineTestProvenance'],
             'sourceManifest': self.m['sourceManifest'], 'python': sys.executable, 'sysPath': sys.path,
             'environment': {key: selected.get(key) for key in ('JAVA_HOME', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'PYTHONPATH', 'TMPDIR', 'TMP', 'TEMP', 'MAVEN_OPTS')}})
        with (directory / 'raw-ansi.log').open('x') as stream:
            process = subprocess.Popen(list(map(str, command)), cwd=self.source, env=selected,
                                       stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
            try:
                code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
                code = 124
        save(directory / 'exit.json', {'exitCode': code})
        (directory / 'clean.log').write_text(re.sub(r'\x1b\[[0-9;]*m', '', (directory / 'raw-ansi.log').read_text(errors='replace')))
        return code

    def prepare(self):
        with tarfile.open(self.m['sourceArchive']['path']) as archive:
            require(not any('/target/' in '/' + item.name for item in archive), 'Frozen source must not smuggle old compiled classes')
        with tarfile.open(self.m['sourceArchive']['path']) as archive:
            archive.extractall(self.work, filter='data')
        require(self.source.is_dir(), 'Source archive must expand as source/')
        for relative, digest in self.source_manifest.items():
            require(sha(self.source / relative) == digest, 'Frozen source differs: ' + relative)
        require(not (self.target / 'classes').exists() and not (self.target / 'test-classes').exists(), 'No old compiled targets allowed')
        code = self.command('fresh-baseline-compile', [self.maven, '-B', '-pl', 'iotdb-core/datanode',
            'test-compile', 'dependency:build-classpath', '-DincludeScope=test',
            '-Dmdep.outputFile=' + str(self.work / 'dependencies.classpath'), *FLAGS], timeout=1800)
        require(code == 0, 'Fresh source compilation failed; not a valid mutation outcome')
        dependencies = (self.work / 'dependencies.classpath').read_text().strip().split(':')
        require(all(Path(path).is_file() for path in dependencies), 'Dependency classpath is not exact files')
        self.cp = ':'.join([str(self.target / 'classes'), str(self.target / 'test-classes'), *dependencies])
        save(self.logs / 'runtime-classpath.json', {'classpath': self.cp,
            'dependencies': [{'path': path, 'sha256': sha(path)} for path in dependencies],
            'freshCompiledFiles': [{'path': str(path.relative_to(self.source)), 'sha256': sha(path)}
                                  for root in ('classes', 'test-classes') for path in sorted((self.target / root).rglob('*.class'))]})
        self.agent = self.work / 'identity-agent.jar'
        agent_classes = self.work / 'identity-agent-classes'
        agent_classes.mkdir()
        code = self.command('compile-identity-agent', [self.javac, '--release', '17', '-d', agent_classes,
                                                     bound(self.m['identityAgentSource'])])
        require(code == 0, 'Identity tool compilation failed')
        agent_manifest = self.work / 'identity-agent.mf'
        agent_manifest.write_text('Manifest-Version: 1.0\nPremain-Class: NegativeContractClassIdentityAgent\nCan-Redefine-Classes: false\nCan-Retransform-Classes: false\n\n')
        code = self.command('package-identity-agent', [self.java.with_name('jar'), '--create', '--file', self.agent,
                                                     '--manifest', agent_manifest, '-C', agent_classes, '.'])
        require(code == 0, 'Identity tool packaging failed')

    def family(self, classname):
        path = self.target / 'classes' / Path(*classname.split('.'))
        return sorted(item for item in path.parent.glob('*.class')
                      if item.name == path.name + '.class' or item.name.startswith(path.name + '$'))

    def compile_family(self, case, label):
        prior = self.work / 'compiled-families' / label / 'before'
        prior.mkdir(parents=True)
        for path in self.family(case['class']):
            shutil.move(str(path), prior / path.name)
        code = self.command(label + '/javac', [self.javac, '--release', '17', '-g', '-parameters',
            '-cp', self.cp, '-sourcepath', '', '-d', self.target / 'classes', self.source / case['source']])
        require(code == 0, 'Mutation/restoration compilation failed; never a successful counterexample')
        family = self.family(case['class'])
        require(family, 'Compiler did not emit the bound main/inner family')
        frozen = self.work / 'compiled-families' / label / 'after'
        frozen.mkdir()
        records = {}
        for path in family:
            shutil.copy2(path, frozen / path.name)
            records[path.name] = sha(path)
        save(frozen / 'class-manifest.json', records)
        return records

    def loaded_identity(self, directory, case):
        receipts = [read(path) for path in directory.glob('loaded-class-identity-*.json')]
        actual = [value for receipt in receipts if receipt['scope'] == 'ACTUAL_SUREFIRE_LOADED_CLASS_RESOURCES'
                  for value in receipt['loadedClasses']]
        require(receipts and all(not receipt['errors'] for receipt in receipts), 'Missing/failed actual class identity agent receipt')
        for receipt in receipts:
            require(receipt.get('agentJarSha256') == sha(self.agent)
                    and receipt.get('javaExecutableSha256') == self.m['java']['sha256'], 'Actual Java/agent differs')
        for name in case['requiredLoadedClasses'] + [target['class'] for target in case['tests']]:
            matches = [item for item in actual if item['class'] == name]
            require(matches, 'Required production/carrier class was not actually loaded: ' + name)
            root = self.target / ('test-classes' if any(target['class'] == name for target in case['tests']) else 'classes')
            path = root / Path(*name.split('.')).with_suffix('.class')
            for item in matches:
                require(not item.get('failure') and item['classResourceSha256'] == sha(path), 'Loaded class resource SHA differs')
                require(str(path) in (item.get('classResourceUrl') or ''), 'Actual class resource resolved outside the new source target')
                code_source = item.get('codeSource')
                require((code_source and str(root) in code_source)
                        or (item.get('loaderClass') or '').startswith('org.powermock.'), 'Unknown actual classloader/code source')
        return receipts

    def test(self, case, label, negative):
        identity_dir = self.work / 'identity' / label
        identity_dir.mkdir(parents=True)
        config = identity_dir / 'identity.properties'
        names = case['requiredLoadedClasses'] + [target['class'] for target in case['tests']]
        config.write_text('outputDirectory=' + str(identity_dir) + '\nexpectedClasses=' + ','.join(names) + '\n')
        reports = self.target / 'surefire-reports'
        if reports.exists():
            moved = self.work / 'preserved-surefire-reports' / label
            moved.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(reports), moved)
        java_tmp = identity_dir / 'java-tmp'
        java_tmp.mkdir()
        selectors = ','.join(target['class'].rsplit('.', 1)[-1] + '#' + target['method'] for target in case['tests'])
        code = self.command(label + '/surefire', [self.maven, '-B', '-pl', 'iotdb-core/datanode',
            'surefire:test', '-Dtest=' + selectors, '-Dsurefire.failIfNoSpecifiedTests=true',
            '-DreuseForks=false', '-DforkCount=1', *FLAGS],
            {'JAVA_TOOL_OPTIONS': '-javaagent:' + str(self.agent) + '=' + str(config)
                 + ' -Xlog:class+load=info:file=' + str(identity_dir / 'classload-%p.log'),
             '_JAVA_OPTIONS': '-Xmx1g -Djava.io.tmpdir=' + str(java_tmp)}, timeout=300)
        destination = self.logs / label / 'per-class-xml-and-raw-reports'
        require(reports.exists(), 'No Surefire reports; tool/JVM error is not a killed mutant')
        shutil.copytree(reports, destination)
        cases = xml_contract(destination, case['tests'], negative)
        require((code != 0 and code != 124) if negative else code == 0, 'Exit status does not match exact JUnit evidence')
        actual = self.loaded_identity(identity_dir, case)
        for target in case["tests"]:
            events = [item for item in cases if item["class"] == target["class"]
                      and item["method"] == target["method"]]
            if any(item.get("providerEventRole") == "POWERMOCK_EMPTY_SUCCESS_FINISH_ARTIFACT" for item in events):
                observed = [item for receipt in actual for item in receipt["loadedClasses"]
                            if item["class"] == target["class"]]
                require(any((item.get("loaderClass") or "").startswith("org.powermock.") for item in observed),
                        "PowerMock empty-success artifact lacks actual PowerMock carrier loader provenance")
        save(self.logs / label / 'test-contract.json', {'status': 'PASS', 'negative': negative,
             'exitCode': code, 'providerTestcaseEvents': cases,
             'logicalMethodEvidence': [{'class': target['class'], 'method': target['method'],
                 'allProviderEvents': [item for item in cases if item['class'] == target['class']
                                      and item['method'] == target['method']]} for target in case['tests']],
             'actualLoadedIdentities': actual})

    def execute(self):
        results = []
        self.prepare()
        for case in self.m['cases']:
            path = self.source / case['source']
            original = path.read_text()
            require(sha(path) == case['sourceSha256'], 'Prior mutation was not restored')
            self.originals[path] = original
            name = case['id']
            family_before = self.compile_family(case, name + '/baseline')
            self.test(case, name + '/baseline', False)
            changed = mutate(original, case['replacements'])
            directory = self.logs / name
            (directory / 'original.java').write_text(original)
            (directory / 'mutated.java').write_text(changed)
            (directory / 'mutation.patch').write_text(''.join(difflib.unified_diff(original.splitlines(True),
                changed.splitlines(True), fromfile=case['source'], tofile=case['source'])))
            error = None
            mutant_family = {}
            try:
                path.write_text(changed)
                mutant_family = self.compile_family(case, name + '/mutant')
                require(mutant_family != family_before, 'No main/inner compiled bytes changed')
                self.test(case, name + '/mutant', True)
            except BaseException as failure:
                error = repr(failure)
            finally:
                path.write_text(original)
                restored_family = self.compile_family(case, name + '/restored')
                self.test(case, name + '/restored', False)
                require(restored_family == family_before, 'Restored main/inner bytes differ from baseline compiler output')
            result = {'id': name, 'status': 'PASS' if error is None else 'FAIL', 'failure': error,
                      'baselineCompiledFamily': family_before, 'mutantCompiledFamily': mutant_family,
                      'restoredCompiledFamily': restored_family, 'sourceRestored': sha(path) == case['sourceSha256']}
            save(directory / 'case-verdict.json', result)
            results.append(result)
        return results

    def archive(self):
        members = [{'path': str(path.relative_to(self.work)), 'sha256': sha(path)}
                   for path in sorted(self.work.rglob('*')) if path.is_file()]
        save(self.output / 'archive-members.json', members)
        target = self.output / 'complete-source-patches-bytecode-xml-identity.tar.gz'
        with tarfile.open(target, 'x:gz', dereference=True) as output:
            for item in members:
                output.add(self.work / item['path'], arcname=item['path'], recursive=False)
        actual = {}
        with tarfile.open(target, 'r|gz') as archive:
            for item in archive:
                digest = hashlib.sha256()
                stream = archive.extractfile(item)
                for block in iter(lambda: stream.read(1048576), b''):
                    digest.update(block)
                actual[item.name] = digest.hexdigest()
        require(actual == {item['path']: item['sha256'] for item in members}, 'Archive round-trip class/source/XML identity mismatch')
        save(self.output / 'archive-receipt.json', {'status': 'PASS', 'path': str(target), 'sha256': sha(target),
             'bytes': target.stat().st_size, 'members': len(members), 'allDecompressedShaVerified': True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--work', '--ram', dest='work', type=Path, required=True,
                        help='New isolated persistent PVC root; --ram is a legacy spelling only')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    m = read(args.manifest)
    source = validate(m)
    if not args.run:
        print(json.dumps({'status': 'BOUND_FILES_ONLY_NO_JAVA_EXECUTION', 'cases': len(m['cases'])}))
        return
    require(not args.output.exists() and not args.output.is_symlink(), 'Preserve history: every run needs new roots')
    work_identity = validate_work_root(args.work, Path(m['pvcTaskRoot']))
    persistent = Path(m['persistentOutputRoot'])
    require(not persistent.exists() and persistent.parent.resolve().is_relative_to(Path(m['pvcTaskRoot']).resolve()), 'New evidence must be inside the bound PVC task root')
    persistent.mkdir()
    args.output.symlink_to(persistent, target_is_directory=True)
    args.work.mkdir()
    save(persistent / 'execution-before.json', {'manifestPath': str(args.manifest.resolve()), 'manifestSha256': sha(args.manifest),
         'publicOutput': str(args.output), 'realOutput': str(persistent.resolve()), 'work': str(args.work), 'workStorage': work_identity,
         'runnerPath': str(Path(__file__).resolve()), 'runnerSha256': sha(__file__)})
    runner = Runner(m, args.work, persistent, source)
    results, failure, archive_failure = [], None, None
    try:
        results = runner.execute()
    except BaseException as error:
        failure = repr(error)
    finally:
        for path, original in runner.originals.items():
            path.write_text(original)
        save(runner.logs / 'final-source-restored.json', {'files': [{'path': str(path), 'sha256': sha(path)} for path in runner.originals]})
        try:
            runner.archive()
        except BaseException as error:
            archive_failure = repr(error)
        final = {'status': 'PASS' if not failure and not archive_failure and len(results) == len(REQUIRED)
                 and all(item['status'] == 'PASS' for item in results) else 'FAIL',
                 'cases': results, 'failure': failure, 'archiveFailure': archive_failure,
                 'scope': 'ORIGINAL_FIVE_CORE_B6_PLUS_DEREGISTRATION_AND_TWO_ROUND_ROBIN_MUTATIONS',
                 'publicOutput': str(args.output), 'realOutput': str(persistent.resolve()),
                 'pvcOriginalsPreserved': True, 'productionRepositoryMutated': False}
        save(persistent / 'acceptance-final.json', final)
    require(final['status'] == 'PASS', 'B6 mutation contracts incomplete; evidence preserved')


if __name__ == '__main__':
    main()
