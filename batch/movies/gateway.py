"""Private, HMAC-authenticated control plane for HCCS Batch movie renders."""
from base64 import b64decode, b64encode
from functools import lru_cache
from hashlib import sha256
from hmac import compare_digest, new as hmac_new
from json import dumps, loads
from os import environ
from re import fullmatch
from time import time

import boto3
from botocore.exceptions import ClientError


@lru_cache
def clients() -> tuple:
    return boto3.client('batch'), boto3.client('s3'), boto3.client('secretsmanager')


@lru_cache
def secret() -> bytes:
    return clients()[2].get_secret_value(SecretId=environ['MOVIE_GATEWAY_SECRET_ARN'])['SecretString'].encode()


def result(status: int, body: dict) -> dict:
    return {'statusCode': status, 'headers': {'content-type': 'application/json', 'cache-control': 'no-store'}, 'body': dumps(body)}


def object_json(key: str) -> dict:
    return loads(clients()[1].get_object(Bucket=environ['MOVIE_BUCKET'], Key=key)['Body'].read())


def put_json(key: str, value: dict, **kwargs) -> dict:
    return clients()[1].put_object(Bucket=environ['MOVIE_BUCKET'], Key=key, Body=dumps(value).encode(), ContentType='application/json', **kwargs)


def manifest(key: str) -> tuple[dict, str]:
    item = clients()[1].get_object(Bucket=environ['MOVIE_BUCKET'], Key=key)
    return loads(item['Body'].read()), item['ETag']


def update_manifest(key: str, value: dict, etag: str) -> dict:
    """A concurrent cancellation wins over a stale queued/progress writer."""
    try:
        put_json(key, value, IfMatch=etag)
        return value
    except clients()[1].exceptions.ClientError as error:
        if error.response['ResponseMetadata']['HTTPStatusCode'] != 412:
            raise
        return manifest(key)[0]


def terminate(batch_id: str) -> None:
    batch = clients()[0]
    try:
        batch.terminate_job(jobId=batch_id, reason='Cancelled by movie owner')
    except ClientError:
        # Finishing between status and cancellation is a legitimate race.
        found = batch.describe_jobs(jobs=[batch_id])['jobs']
        if not found or found[0]['status'] not in ['SUCCEEDED', 'FAILED']:
            raise


def reconcile(identity: str, value: dict, etag: str) -> dict:
    """Find a successfully submitted job even if its Lambda died before saving the ID."""
    if value.get('batchId') or value['state'] == 'failed':
        return value
    batch = clients()[0]
    found = batch.list_jobs(jobQueue=environ['MOVIE_QUEUE'], filters=[{'name': 'JOB_NAME', 'values': ['movie-' + identity]}], maxResults=100)['jobSummaryList']
    if found:
        if len(found) != 1:
            raise RuntimeError('Multiple Batch jobs for one movie identity')
        batch_id = found[0]['jobId']
        if value['state'] == 'cancelled':
            terminate(batch_id)
        return update_manifest(f'jobs/{identity}/job.json', {**value, 'batchId': batch_id}, etag)
    if value['state'] == 'submitting' and time() - value['submittedAt'] >= 120:
        return update_manifest(f'jobs/{identity}/job.json', {**value, 'state': 'failed'}, etag)
    return value


def handler(event: dict, _context) -> dict:
    method = event['requestContext']['http']['method']
    path = event['rawPath']
    headers = event.get('headers', {})
    stamp = headers.get('x-movie-timestamp', '')
    body = event.get('body') or ''
    if event.get('isBase64Encoded'):
        body = b64decode(body).decode()
    message = f'{stamp}\n{method}\n{path}\n{body}'.encode()
    signature = hmac_new(secret(), message, sha256).hexdigest()
    if not stamp.isdigit() or abs(time() - int(stamp)) > 60 or not compare_digest(headers.get('x-movie-signature', ''), signature):
        return result(403, {'error': 'forbidden'})
    batch, s3, _ = clients()
    if method == 'POST' and path == '/jobs':
        if len(body.encode()) > 12_000:
            return result(413, {'error': 'request too large'})
        job = loads(body)
        if not fullmatch(r'[a-f0-9-]{36}', str(job.get('id', ''))):
            return result(400, {'error': 'invalid job'})
        if not all(isinstance(job.get(key), (int, float)) and 0 < job[key] <= cap for key, cap in [('width', 1920), ('height', 1920), ('fps', 60), ('seconds', 120)]):
            return result(400, {'error': 'invalid render settings'})
        if job['width'] * job['height'] > 2_073_600 or len(job.get('query', '')) > 8192:
            return result(400, {'error': 'invalid render settings'})
        key = f"jobs/{job['id']}/job.json"
        try:
            initial = {'state': 'submitting', 'submittedAt': time(), 'frames': job['frames']}
            created = put_json(key, initial, IfNoneMatch='*')
        except s3.exceptions.ClientError as error:
            if error.response['ResponseMetadata']['HTTPStatusCode'] != 412:
                raise
            existing, etag = manifest(key)
            reconcile(job['id'], existing, etag)
            return result(200, {'id': job['id']})
        submitted = batch.submit_job(
            jobName=f"movie-{job['id']}", jobQueue=environ['MOVIE_QUEUE'], jobDefinition=environ['MOVIE_JOB_DEFINITION'],
            containerOverrides={'environment': [
                {'name': 'MOVIE_JOB', 'value': b64encode(dumps(job).encode()).decode()},
                {'name': 'MOVIE_BUCKET', 'value': environ['MOVIE_BUCKET']},
                {'name': 'MOVIE_SITE_ORIGIN', 'value': environ['MOVIE_SITE_ORIGIN']},
            ]},
            timeout={'attemptDurationSeconds': 7200},
            tags={'project': 'ctbk', 'purpose': 'movie', 'movie': job['id']},
        )
        saved = update_manifest(key, {**initial, 'batchId': submitted['jobId'], 'state': 'queued'}, created['ETag'])
        if saved['state'] == 'cancelled':
            terminate(submitted['jobId'])
        return result(202, {'id': job['id']})
    match = fullmatch(r'/jobs/([a-f0-9-]{36})(/file)?', path)
    if not match:
        return result(404, {'error': 'not found'})
    identity, file = match.groups()
    key = f'jobs/{identity}/job.json'
    try:
        current, etag = manifest(key)
    except s3.exceptions.NoSuchKey:
        if method == 'DELETE' and not file:
            try:
                put_json(key, {'state': 'cancelled'}, IfNoneMatch='*')
                return result(200, {'state': 'cancelled', 'frame': 0})
            except s3.exceptions.ClientError as error:
                if error.response['ResponseMetadata']['HTTPStatusCode'] != 412:
                    raise
                current, etag = manifest(key)
        else:
            return result(404, {'error': 'not found'})
    current = reconcile(identity, current, etag)
    batch_id = current.get('batchId')
    try:
        progress = object_json(f'jobs/{identity}/progress.json')
    except s3.exceptions.NoSuchKey:
        progress = {'frame': 0}
    if current['state'] == 'failed':
        return result(200, {'state': 'failed', 'frame': 0, 'error': 'Renderer submission expired. Please export again.'})
    if method == 'DELETE' and not file:
        if progress.get('complete'):
            return result(200, {'state': 'complete', 'frame': progress['frame']})
        current, etag = manifest(key)
        current = update_manifest(key, {**current, 'state': 'cancelled'}, etag)
        if batch_id:
            terminate(batch_id)
        return result(200, {'state': 'cancelled', 'frame': progress['frame']})
    if method != 'GET':
        return result(405, {'error': 'method not allowed'})
    if current['state'] == 'cancelled':
        if file:
            return result(409, {'error': 'export was cancelled'})
        return result(200, {'state': 'cancelled', 'frame': 0})
    if not batch_id:
        return result(200, {'state': 'queued', 'frame': 0})
    if progress.get('complete'):
        state = 'SUCCEEDED'
    else:
        info = batch.describe_jobs(jobs=[batch_id])['jobs']
        if not info:
            return result(404, {'error': 'job expired'})
        state = info[0]['status']
    if file:
        if state != 'SUCCEEDED':
            return result(409, {'error': 'export is not complete'})
        url = s3.generate_presigned_url('get_object', Params={
            'Bucket': environ['MOVIE_BUCKET'], 'Key': f'jobs/{identity}/movie.mp4',
            'ResponseContentDisposition': f'attachment; filename="ctbk-{identity}.mp4"',
        }, ExpiresIn=300)
        return result(200, {'url': url})
    mapped = {'SUBMITTED': 'queued', 'PENDING': 'queued', 'RUNNABLE': 'queued', 'STARTING': 'queued', 'RUNNING': 'rendering', 'SUCCEEDED': 'complete', 'FAILED': 'failed'}[state]
    return result(200, {'state': mapped, 'frame': progress.get('frame', 0), **({'error': 'Renderer failed. Please retry the export.'} if mapped == 'failed' else {})})
