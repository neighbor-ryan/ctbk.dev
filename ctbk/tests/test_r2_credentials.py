from hashlib import sha256
from json import loads
from subprocess import CompletedProcess

from click.testing import CliRunner
from pytest import raises
from click import ClickException

from ctbk import r2_credentials as module


def set_credentials(monkeypatch):
    values = {
        'R2_RW_TOKEN': 'synthetic-token',
        'R2_RW_ACCESS_KEY_ID': 'synthetic-key-id',
        'R2_RW_SECRET_ACCESS_KEY': sha256(b'synthetic-token').hexdigest(),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    return {key: values[f'R2_RW_{key[3:]}'] for key in module.KEYS}


def test_subprocess_failure_does_not_replay_secret_input(monkeypatch):
    monkeypatch.setattr(module, 'run', lambda *args, **kwargs: CompletedProcess(args, 3, 'sensitive stdout', 'sensitive stderr'))
    with raises(ClickException) as exc:
        module.safe_command(['gh', 'secret', 'set', 'KEY'], 'sensitive input')
    assert str(exc.value) == 'gh secret failed (exit 3); output withheld'


def test_dvx_cutover_only_changes_hccs_remote(monkeypatch, tmp_path):
    expected = set_credentials(monkeypatch)
    config_path = tmp_path / '.dvc/config.local'
    config_path.parent.mkdir()
    config_path.write_text(f'''[remote "r2"]
    endpointurl = https://{module.ACCOUNT}.r2.cloudflarestorage.com
    access_key_id = old-hccs-key
    secret_access_key = old-hccs-secret
    session_token = unrelated-aws-session
[remote "rac"]
    endpointurl = https://rac-account.r2.cloudflarestorage.com
    access_key_id = rac-key
    secret_access_key = rac-secret
''')
    result = CliRunner().invoke(module.apply, ['-r', str(tmp_path), '-t', 'dvx'])
    assert result.exit_code == 0, result.output
    assert loads(result.output) == {'localDvxRemotesUpdated': ['remote "r2"']}
    config = module.ConfigParser()
    config.read(config_path)
    assert dict(config['remote "r2"']) == {
        'endpointurl': f'https://{module.ACCOUNT}.r2.cloudflarestorage.com',
        'access_key_id': expected[module.KEYS[0]],
        'secret_access_key': expected[module.KEYS[1]],
    }
    assert dict(config['remote "rac"']) == {
        'endpointurl': 'https://rac-account.r2.cloudflarestorage.com',
        'access_key_id': 'rac-key',
        'secret_access_key': 'rac-secret',
    }


def test_active_batch_job_prevents_any_aws_cutover(monkeypatch, tmp_path):
    set_credentials(monkeypatch)
    monkeypatch.setattr(module, 'aws_session', lambda: object())
    monkeypatch.setattr(module, 'aws_inventory', lambda *args: ({'activeJobs': [{'id': 'job'}]}, {}, []))
    result = CliRunner().invoke(module.apply, ['-r', str(tmp_path), '-t', 'aws'])
    assert result.exit_code == 1
    assert result.output == 'Error: active Batch jobs found; wait for them before cutover\n'


def test_dvx_inherits_tracked_endpoint_with_quoted_section(monkeypatch, tmp_path):
    expected = set_credentials(monkeypatch)
    directory = tmp_path / '.dvc'
    directory.mkdir()
    (directory / 'config').write_text(f'''['remote "r2"']
    endpointurl = https://{module.ACCOUNT}.r2.cloudflarestorage.com
    url = s3://ctbk/.dvc
''')
    (directory / 'config.local').write_text('''['remote "r2"']
    access_key_id = old-key
    secret_access_key = old-secret
''')
    result = CliRunner().invoke(module.apply, ['-r', str(tmp_path), '-t', 'dvx'])
    assert result.exit_code == 0, result.output
    assert loads(result.output) == {'localDvxRemotesUpdated': ["'remote \"r2\"'"]}
    config = module.ConfigParser()
    config.read(directory / 'config.local')
    assert dict(config["'remote \"r2\"'"]) == {
        'access_key_id': expected[module.KEYS[0]],
        'secret_access_key': expected[module.KEYS[1]],
    }
