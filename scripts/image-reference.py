import hashlib,json,pathlib,re,sys
p=pathlib.Path('dist');d=json.loads((p/'image-metadata.json').read_text());digest=d['containerimage.digest']
if not re.fullmatch(r'sha256:[a-f0-9]{64}',digest):raise ValueError('invalid image digest')
(p/'image-reference.txt').write_text(sys.argv[1]+'@'+digest+'\n')
files=[x for x in p.iterdir() if x.is_file() and x.name not in ['SHA256SUMS','image-metadata.json']]
(p/'SHA256SUMS').write_text(''.join(hashlib.sha256(x.read_bytes()).hexdigest()+'  '+x.name+'\n' for x in sorted(files)))
