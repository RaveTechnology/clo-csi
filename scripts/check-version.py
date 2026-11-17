import json,pathlib,sys,yaml
version=sys.argv[1].removeprefix('v');status=json.loads(pathlib.Path('RELEASE_STATUS.json').read_text());chart=yaml.safe_load(pathlib.Path('deploy/csi/Chart.yaml').read_text())
if version!=status['version'] or version!=chart['version'] or version!=chart['appVersion']:raise SystemExit('tag, release status and chart version mismatch')
if not status['productionReady'] and '-' not in version:raise SystemExit('unaccepted CSI must remain a prerelease')
