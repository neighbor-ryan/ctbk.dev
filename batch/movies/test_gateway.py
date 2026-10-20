from hashlib import sha256
from hmac import new as hmac_new
from importlib.util import module_from_spec, spec_from_file_location
from io import BytesIO
from json import dumps, loads
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call

from botocore.exceptions import ClientError
import pytest


@pytest.fixture
def gateway(monkeypatch):
    spec = spec_from_file_location('movie_gateway', Path(__file__).with_name('gateway.py'))
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, 'time', lambda: 1000)
    monkeypatch.setattr(module, 'secret', lambda: b'test-secret')
    for name, value in {'MOVIE_BUCKET': 'private-movies', 'MOVIE_QUEUE': 'movie-queue', 'MOVIE_JOB_DEFINITION': 'movie-definition', 'MOVIE_SITE_ORIGIN': 'https://ctbk.dev'}.items():
        monkeypatch.setenv(name, value)
    objects = {}

    class Missing(Exception):
        pass

    def get_object(*, Bucket, Key):
        if Key not in objects:
            raise Missing()
        return {'Body': BytesIO(dumps(objects[Key]).encode())}

    def put_object(*, Bucket, Key, Body, ContentType, **kwargs):
        if kwargs.get('IfNoneMatch') == '*' and Key in objects:
            raise ClientError({'ResponseMetadata': {'HTTPStatusCode': 412}, 'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        objects[Key] = loads(Body)

    s3 = SimpleNamespace(get_object=get_object, put_object=put_object, exceptions=SimpleNamespace(NoSuchKey=Missing, ClientError=ClientError), generate_presigned_url=Mock(return_value='https://private.s3.amazonaws.com/movie?temporary'))
    batch = SimpleNamespace(submit_job=Mock(return_value={'jobId': 'batch-123'}), describe_jobs=Mock(return_value={'jobs': [{'status': 'RUNNING'}]}), terminate_job=Mock())
    monkeypatch.setattr(module, 'clients', lambda: (batch, s3, None))
    return module, objects, batch, s3


def request(gateway, method: str, path: str, value: dict | None = None, timestamp: str = '1000') -> tuple:
    module = gateway[0]
    body = dumps(value) if value is not None else ''
    signature = hmac_new(b'test-secret', f'{timestamp}\n{method}\n{path}\n{body}'.encode(), sha256).hexdigest()
    response = module.handler({'requestContext': {'http': {'method': method}}, 'rawPath': path, 'body': body, 'headers': {'x-movie-timestamp': timestamp, 'x-movie-signature': signature}}, None)
    return response['statusCode'], loads(response['body'])


ID = '00000000-0000-0000-0000-000000000001'
JOB = {'id': ID, 'width': 1080, 'height': 1350, 'fps': 30, 'seconds': 30, 'frames': 900, 'query': 'anim=1&clip=260929T04-260930T04'}


def test_expired_signatures_cannot_submit_jobs(gateway):
    assert request(gateway, 'POST', '/jobs', JOB, timestamp='900') == (403, {'error': 'forbidden'})
    assert gateway[2].submit_job.call_args_list == []


def test_duplicate_submissions_are_idempotent_and_use_fixed_infrastructure(gateway):
    assert request(gateway, 'POST', '/jobs', JOB) == (202, {'id': ID})
    assert request(gateway, 'POST', '/jobs', JOB) == (200, {'id': ID})
    from base64 import b64encode
    assert gateway[2].submit_job.call_args_list == [call(
        jobName='movie-' + ID, jobQueue='movie-queue', jobDefinition='movie-definition',
        containerOverrides={'environment': [
            {'name': 'MOVIE_JOB', 'value': b64encode(dumps(JOB).encode()).decode()},
            {'name': 'MOVIE_BUCKET', 'value': 'private-movies'},
            {'name': 'MOVIE_SITE_ORIGIN', 'value': 'https://ctbk.dev'},
        ]},
        timeout={'attemptDurationSeconds': 7200}, tags={'project': 'ctbk', 'purpose': 'movie', 'movie': ID},
    )]


def test_downloads_require_success_and_use_short_lived_private_links(gateway):
    module, objects, batch, s3 = gateway
    objects[f'jobs/{ID}/job.json'] = {'batchId': 'batch-123', 'state': 'queued'}
    objects[f'jobs/{ID}/progress.json'] = {'frame': 45}
    assert request(gateway, 'GET', f'/jobs/{ID}') == (200, {'state': 'rendering', 'frame': 45})
    assert request(gateway, 'GET', f'/jobs/{ID}/file') == (409, {'error': 'export is not complete'})
    batch.describe_jobs.return_value = {'jobs': [{'status': 'SUCCEEDED'}]}
    assert request(gateway, 'GET', f'/jobs/{ID}/file') == (200, {'url': 'https://private.s3.amazonaws.com/movie?temporary'})
    assert s3.generate_presigned_url.call_args_list == [(('get_object',), {'Params': {'Bucket': 'private-movies', 'Key': f'jobs/{ID}/movie.mp4', 'ResponseContentDisposition': f'attachment; filename="ctbk-{ID}.mp4"'}, 'ExpiresIn': 300})]


def test_cancelled_jobs_remain_cancelled(gateway):
    module, objects, batch, s3 = gateway
    objects[f'jobs/{ID}/job.json'] = {'batchId': 'batch-123', 'state': 'queued'}
    assert request(gateway, 'DELETE', f'/jobs/{ID}') == (200, {'state': 'cancelled', 'frame': 0})
    batch.describe_jobs.return_value = {'jobs': [{'status': 'SUCCEEDED'}]}
    assert request(gateway, 'GET', f'/jobs/{ID}') == (200, {'state': 'cancelled', 'frame': 0})
    assert request(gateway, 'GET', f'/jobs/{ID}/file') == (409, {'error': 'export was cancelled'})
    assert batch.terminate_job.call_args_list == [((), {'jobId': 'batch-123', 'reason': 'Cancelled by movie owner'})]
    assert s3.generate_presigned_url.call_args_list == []
