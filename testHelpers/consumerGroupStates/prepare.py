"""Apply the fixture-only gate to one pinned Apache Kafka source tree."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys


def prepare(version, source):
    """Verify the official source bytes, apply the exact patch, and install the helper class."""
    fixture = Path(__file__).resolve().parent
    manifest = json.loads((fixture / 'provenance.json').read_text())
    entry = manifest[version]
    relative = Path('group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java')
    target = Path(source) / relative
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    if digest != entry['sha256']:
        raise ValueError('coordinator source digest does not match the pinned version')
    patch = fixture / (version + '.patch')
    if hashlib.sha256(patch.read_bytes()).hexdigest() != entry['gatePatchSha256']:
        raise ValueError('gate patch digest does not match provenance')
    result = subprocess.run(['timeout', '600', 'patch', '--batch', '--fuzz=0', '--no-backup-if-mismatch', '-p1', '-i', str(patch)],
                            cwd=source, capture_output=True, text=True)
    print(result.stdout, end='')
    if result.returncode or 'offset' in result.stdout or 'fuzz' in result.stdout:
        raise RuntimeError('gate patch did not apply exactly')
    shutil.copyfile(fixture / 'AssignmentGate.java', target.parent / 'AssignmentGate.java')


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise ValueError('usage: prepare.py <Kafka version> <source tree>')
    prepare(sys.argv[1], sys.argv[2])
