"""Credential cutover helpers: values enter through env/stdin, never reports."""
from __future__ import annotations

from configparser import ConfigParser
from hashlib import sha256
from json import dumps, loads
from os import environ
from pathlib import Path
from re import match
from shlex import split
from subprocess import run
from uuid import uuid4
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import Request, urlopen
from xml.etree.ElementTree import fromstring

from click import Choice, ClickException, option

from .cli.base import ctbk

ACCOUNT = '2363642879f18d37d52dca114059937e'
AWS_ACCOUNT = '688066488567'
KEYS = ('R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY')
SECRET_NAMES = {
    'ctbk/r2-access-key-id': KEYS[0],
    'ctbk/r2-secret-access-key': KEYS[1],
    'ctbk-reproc/r2-access-key-id': KEYS[0],
    'ctbk-reproc/r2-secret-access-key': KEYS[1],
}


def credentials() -> dict[str, str]:
    values = {key: environ[f'R2_RW_{key[3:]}'] for key in KEYS}
    token = environ['R2_RW_TOKEN']
    if sha256(token.encode()).hexdigest() != values[KEYS[1]]:
        raise ClickException('R2_RW_TOKEN and R2_RW_SECRET_ACCESS_KEY do not match')
    return values


def pair_matches(actual: dict[str, str], expected: dict[str, str]) -> bool:
    return all(actual.get(key) == expected[key] for key in KEYS)


def aws_session():
    from boto3 import Session
    session = Session(profile_name='h', region_name='us-east-1')
    if session.client('sts').get_caller_identity()['Account'] != AWS_ACCOUNT:
        raise ClickException('AWS profile h is not the HCCS account')
    return session


def aws_inventory(session, expected: dict[str, str]) -> tuple[dict, dict, list]:
    sm = session.client('secretsmanager')
    secret_values = {}
    secrets = []
    for name, key in SECRET_NAMES.items():
        try:
            out = sm.get_secret_value(SecretId=name)
        except sm.exceptions.ResourceNotFoundException:
            continue  # Legacy names may have been retired by the HCCS migration.
        secret_values[name] = out['SecretString']
        secrets.append({'name': name, 'matches': out['SecretString'] == expected[key], 'version': out['VersionId']})
    functions = []
    lam = session.client('lambda')
    for page in lam.get_paginator('list_functions').paginate():
        for item in page['Functions']:
            if not item['FunctionName'].startswith('ctbk-'):
                continue
            config = lam.get_function_configuration(FunctionName=item['FunctionName'])
            env = config.get('Environment', {}).get('Variables', {})
            if not any(key in env for key in KEYS):
                continue
            functions.append({'name': item['FunctionName'], 'matches': pair_matches(env, expected), 'revision': config['RevisionId']})
    batch = session.client('batch')
    latest = {}
    for page in batch.get_paginator('describe_job_definitions').paginate(status='ACTIVE'):
        for definition in page['jobDefinitions']:
            name = definition['jobDefinitionName']
            if not (name.startswith('ctbk-') or name == 'pyrmts-engine'):
                continue
            if name not in latest or definition['revision'] > latest[name]['revision']:
                latest[name] = definition
    definitions = []
    for name, definition in sorted(latest.items()):
        props = definition['containerProperties']
        refs = {item['name']: item['valueFrom'] for item in props.get('secrets', []) if item['name'] in KEYS}
        plain = {item['name']: item['value'] for item in props.get('environment', []) if item['name'] in KEYS}
        definitions.append({'name': name, 'revision': definition['revision'], 'secretRefs': refs, 'plaintextPairPresent': bool(plain), 'plaintextMatches': pair_matches(plain, expected) if plain else None})
    jobs = []
    for queue in ('ctbk-reproc', 'pyrmts-engine'):
        for status in ('SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING'):
            for page in batch.get_paginator('list_jobs').paginate(jobQueue=queue, jobStatus=status):
                jobs.extend({'name': item['jobName'], 'id': item['jobId'], 'status': status} for item in page['jobSummaryList'])
    return {'secrets': secrets, 'functions': functions, 'jobDefinitions': definitions, 'activeJobs': jobs}, secret_values, functions


def safe_command(
    args: list[str],
    values: str | None = None,
    cwd: Path | None = None,
) -> str:
    result = run(args, input=values, text=True, capture_output=True, cwd=cwd)
    if result.returncode:
        # Do not replay a subprocess's diagnostic: secret-taking commands can
        # echo input on failure. The command identity and exit code are sufficient.
        raise ClickException(f'{args[0]} {args[1]} failed (exit {result.returncode}); output withheld')
    return result.stdout


@ctbk.group('r2-creds', help='Verify and propagate the HCCS R2_RW_* pair without printing values. Never creates/revokes tokens.')
def r2_creds() -> None:
    pass


@r2_creds.command('audit')
def audit() -> None:
    expected = credentials()
    report, _, _ = aws_inventory(aws_session(), expected)
    report['newKeyFingerprint'] = sha256(expected[KEYS[0]].encode()).hexdigest()[:12]
    print(dumps(report, indent=2))


@r2_creds.command('verify', help='Verify LIST and an isolated PUT/GET/DELETE round trip in the ctbk scratch prefix.')
def verify() -> None:
    from boto3 import client
    expected = credentials()
    s3 = client('s3', endpoint_url=f'https://{ACCOUNT}.r2.cloudflarestorage.com', aws_access_key_id=expected[KEYS[0]], aws_secret_access_key=expected[KEYS[1]], region_name='auto')
    s3.list_objects_v2(Bucket='ctbk', Prefix='health/', MaxKeys=1)
    key = f'scratch/credential-check/{uuid4()}.txt'
    data = b'ctbk credential round trip\n'
    s3.put_object(Bucket='ctbk', Key=key, Body=data)
    try:
        if s3.get_object(Bucket='ctbk', Key=key)['Body'].read() != data:
            raise ClickException('scratch GET bytes differ from PUT')
    finally:
        s3.delete_object(Bucket='ctbk', Key=key)
    print(dumps({'account': ACCOUNT, 'bucket': 'ctbk', 'list': True, 'writeReadDelete': True}))


@r2_creds.command('probe', help='Verify both API Workers mint usable URLs with the new access key; signed URLs stay in memory.')
def probe() -> None:
    expected = credentials()
    for worker in ('ctbk-gbfs-api', 'ctbk-gbfs-api-dev'):
        endpoint = f'https://{worker}.hccs-ctbk.workers.dev/api/files/presign'
        query = urlencode({'path': 'stations/station-canonicalize-map.json', 'expires': 60})
        stage = 'mint'
        hostname = urlsplit(endpoint).hostname
        try:
            request = Request(f'{endpoint}?{query}', headers={'User-Agent': 'ctbk-r2-creds/1.0'})
            with urlopen(request, timeout=45) as response:
                signed = loads(response.read())['url']
            credential = parse_qs(urlsplit(signed).query)['X-Amz-Credential'][0].split('/')[0]
            if credential != expected[KEYS[0]]:
                raise ClickException(f'{worker} still signs with another access key')
            stage = 'signed GET'
            hostname = urlsplit(signed).hostname
            with urlopen(signed, timeout=45) as response:
                status = response.status
                response.read(1)
        except HTTPError as exc:
            body = exc.read()
            code = fromstring(body).findtext('Code') if body.startswith(b'<?xml') else None
            raise ClickException(f'{worker} {stage} failed (HTTP {exc.code}, code={code}, host={hostname}); URL withheld') from None
        print(dumps({'worker': worker, 'usesNewKey': True, 'signedGetStatus': status}))


@r2_creds.command('apply')
@option('-r', '--root', type=Path, required=True, help='Primary project checkout, containing local DVX config and infra stack.')
@option('-t', '--target', type=Choice(['aws', 'github', 'workers', 'pulumi', 'dvx', 'profiles']), required=True)
def apply(root: Path, target: str) -> None:
    expected = credentials()
    if target == 'aws':
        session = aws_session()
        report, current, functions = aws_inventory(session, expected)
        if report['activeJobs']:
            raise ClickException('active Batch jobs found; wait for them before cutover')
        if any(item['plaintextPairPresent'] and not item['plaintextMatches'] for item in report['jobDefinitions']):
            raise ClickException('latest Batch definition has old plaintext R2 credentials; migrate it to Secrets Manager first')
        sm = session.client('secretsmanager')
        for name, value in current.items():
            if value != expected[SECRET_NAMES[name]]:
                sm.put_secret_value(SecretId=name, SecretString=expected[SECRET_NAMES[name]])
        lam = session.client('lambda')
        for item in functions:
            if item['matches']:
                continue
            config = lam.get_function_configuration(FunctionName=item['name'])
            env = config['Environment']['Variables']
            env.update(expected)
            lam.update_function_configuration(FunctionName=item['name'], Environment={'Variables': env}, RevisionId=config['RevisionId'])
            lam.get_waiter('function_updated_v2').wait(FunctionName=item['name'])
        report, _, _ = aws_inventory(session, expected)
        if not all(item['matches'] for item in report['secrets'] + report['functions']):
            raise ClickException('AWS cutover verification failed')
        print(dumps(report, indent=2))
    elif target == 'github':
        for key in KEYS:
            safe_command(['gh', 'secret', 'set', key, '-R', 'hudcostreets/ctbk.dev'], expected[key])
        print(dumps({'githubSecretsUpdated': list(KEYS), 'repository': 'hudcostreets/ctbk.dev'}))
    elif target == 'workers':
        # These Workers use S3 credentials for presigning. Their native R2
        # bindings are independent of these credentials and remain unchanged.
        config = Path(__file__).resolve().parents[1] / 'gbfs/api/wrangler.toml'
        auth_env = dict(environ, CLOUDFLARE_ACCOUNT_ID=ACCOUNT, CLOUDFLARE_API_TOKEN=environ['CF_PULUMI_HCCS_TOKEN'])
        for worker in ('ctbk-gbfs-api', 'ctbk-gbfs-api-dev', 'ctbk-gbfs-cascade', 'ctbk-gbfs-cascade-dev'):
            result = run(['pnpm', 'exec', 'wrangler', 'secret', 'bulk', '--name', worker, '--config', str(config)], input=dumps(expected), text=True, capture_output=True, cwd=config.parent, env=auth_env)
            if result.returncode:
                raise ClickException(f'Worker {worker} secret bulk failed (exit {result.returncode}); output withheld')
            print(dumps({'workerSecretsUpdated': worker, 'keys': list(KEYS)}))
    elif target == 'pulumi':
        for key, config_key in zip(KEYS, ('lambda_r2_access_key_id', 'lambda_r2_secret_access_key')):
            safe_command(['pulumi', 'config', 'set', config_key, '--secret', '--stack', 'hccs'], expected[key] + '\n', root / 'infra')
        print(dumps({'encryptedPulumiConfigUpdated': 'hccs', 'keys': ['lambda_r2_access_key_id', 'lambda_r2_secret_access_key']}))
    elif target == 'dvx':
        path = root / '.dvc/config.local'
        config = ConfigParser()
        config.read(path)
        merged = ConfigParser()
        merged.read([root / '.dvc/config', path])
        updated = []
        for section in config.sections():
            if not section.strip("'").startswith('remote '):
                continue
            endpoint = merged.get(section, 'endpointurl', fallback='')
            if ACCOUNT not in endpoint:
                continue
            config.set(section, 'access_key_id', expected[KEYS[0]])
            config.set(section, 'secret_access_key', expected[KEYS[1]])
            if config.has_option(section, 'session_token'):
                config.remove_option(section, 'session_token')
            updated.append(section)
        if not updated:
            raise ClickException('No HCCS R2 remotes found; no config changed')
        with path.open('w') as stream:
            config.write(stream)
        print(dumps({'localDvxRemotesUpdated': updated}))
    elif target == 'profiles':
        old_ids = set()
        for line in (root / '.envrc').read_text().splitlines():
            found = match(r'\s*#\s*(?:export\s+)?R2_RW_ACCESS_KEY_ID\s*=(.*)', line)
            if found:
                value, = split(found[1], comments=True)
                old_ids.add(value)
        path = Path.home() / '.aws/credentials'
        config = ConfigParser()
        config.read(path)
        updated = []
        for section in config.sections():
            if config.get(section, 'aws_access_key_id', fallback='') not in old_ids:
                continue
            config.set(section, 'aws_access_key_id', expected[KEYS[0]])
            config.set(section, 'aws_secret_access_key', expected[KEYS[1]])
            if config.has_option(section, 'aws_session_token'):
                config.remove_option(section, 'aws_session_token')
            updated.append(section)
        if updated:
            with path.open('w') as stream:
                config.write(stream)
        print(dumps({'localAwsProfilesUpdated': updated}))
