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


def put_json(key: str, value: dict, **kwargs) -> None:
    clients()[1].put_object(Bucket=environ['MOVIE_BUCKET'], Key=key, Body=dumps(value).encode(), ContentType='application/json', **kwargs)


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
            put_json(key, {'state': 'submitting'}, IfNoneMatch='*')
        except s3.exceptions.ClientError as error:
            if error.response['ResponseMetadata']['HTTPStatusCode'] != 412:
                raise
            existing = object_json(key)
            return result(200 if existing.get('batchId') else 409, {'id': job['id']})
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
        put_json(key, {'batchId': submitted['jobId'], 'state': 'queued'})
        return result(202, {'id': job['id']})
    match = fullmatch(r'/jobs/([a-f0-9-]{36})(/file)?', path)
    if not match:
        return result(404, {'error': 'not found'})
    identity, file = match.groups()
    key = f'jobs/{identity}/job.json'
    try:
        manifest = object_json(key)
    except s3.exceptions.NoSuchKey:
        return result(404, {'error': 'not found'})
    batch_id = manifest.get('batchId')
    if not batch_id:
        return result(409, {'error': 'job is being submitted'})
    if method == 'DELETE' and not file:
        batch.terminate_job(jobId=batch_id, reason='Cancelled by movie owner')
        put_json(key, {**manifest, 'state': 'cancelled'})
        return result(200, {'state': 'cancelled', 'frame': 0})
    if method != 'GET':
        return result(405, {'error': 'method not allowed'})
    if manifest['state'] == 'cancelled':
        if file:
            return result(409, {'error': 'export was cancelled'})
        return result(200, {'state': 'cancelled', 'frame': 0})
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
    try:
        progress = object_json(f'jobs/{identity}/progress.json')
    except s3.exceptions.NoSuchKey:
        progress = {'frame': 0}
    mapped = {'SUBMITTED': 'queued', 'PENDING': 'queued', 'RUNNABLE': 'queued', 'STARTING': 'queued', 'RUNNING': 'rendering', 'SUCCEEDED': 'complete', 'FAILED': 'failed'}[state]
    return result(200, {'state': mapped, 'frame': progress.get('frame', 0), **({'error': 'Renderer failed. Please retry the export.'} if mapped == 'failed' else {})})
