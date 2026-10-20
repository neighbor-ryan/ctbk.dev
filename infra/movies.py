"""Opt-in HCCS movie rendering: private S3 artifacts and bounded Fargate jobs."""
import json
from pathlib import Path

import pulumi
import pulumi_aws as aws
import pulumi_cloudflare as cf

from aws_hccs import ACCOUNT, REGION, BATCH_SG, BATCH_SUBNETS, _assume


def provision(account_id) -> None:
    config = pulumi.Config()
    db = cf.D1Database('ctbk-movies', account_id=account_id, name='ctbk-movies', read_replication={'mode': 'disabled'})
    bucket = aws.s3.BucketV2('ctbk-movies', bucket=f'ctbk-movies-{ACCOUNT}', force_destroy=False)
    aws.s3.BucketPublicAccessBlock('ctbk-movies-private', bucket=bucket.id, block_public_acls=True, block_public_policy=True, ignore_public_acls=True, restrict_public_buckets=True)
    aws.s3.BucketLifecycleConfigurationV2('ctbk-movies-expiry', bucket=bucket.id, rules=[{'id': 'expire-movies', 'status': 'Enabled', 'expiration': {'days': 30}, 'abort_incomplete_multipart_upload': {'days_after_initiation': 1}}])
    repo = aws.ecr.Repository('ctbk-movies-image', name='ctbk-movies', image_tag_mutability='IMMUTABLE')
    exec_role = aws.iam.Role('ctbk-movies-execution', name='ctbk-movies-execution', assume_role_policy=_assume('ecs-tasks.amazonaws.com'))
    aws.iam.RolePolicyAttachment('ctbk-movies-ecs', role=exec_role.name, policy_arn='arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy')
    task_role = aws.iam.Role('ctbk-movies-task', name='ctbk-movies-task', assume_role_policy=_assume('ecs-tasks.amazonaws.com'))
    s3_policy = bucket.arn.apply(lambda arn: json.dumps({'Version': '2012-10-17', 'Statement': [{'Effect': 'Allow', 'Action': ['s3:GetObject', 's3:PutObject'], 'Resource': arn + '/jobs/*'}]}))
    aws.iam.RolePolicy('ctbk-movies-task-objects', role=task_role.name, policy=s3_policy)
    log = aws.cloudwatch.LogGroup('ctbk-movies-log', name='/ctbk-movies/batch', retention_in_days=30)
    compute = aws.batch.ComputeEnvironment('ctbk-movies-compute', name='ctbk-movies-spot', type='MANAGED', state='ENABLED', compute_resources={'type': 'FARGATE_SPOT', 'max_vcpus': 8, 'subnets': BATCH_SUBNETS, 'security_group_ids': [BATCH_SG]})
    queue = aws.batch.JobQueue('ctbk-movies-queue', name='ctbk-movies', priority=1, state='ENABLED', compute_environment_orders=[{'order': 1, 'compute_environment': compute.arn}])
    aws.iam.RolePolicy('ctbk-gha-movies-images', role='ctbk-gha', policy=repo.arn.apply(lambda arn: json.dumps({'Version': '2012-10-17', 'Statement': [{'Effect': 'Allow', 'Action': ['ecr:BatchCheckLayerAvailability', 'ecr:BatchGetImage', 'ecr:GetDownloadUrlForLayer', 'ecr:InitiateLayerUpload', 'ecr:UploadLayerPart', 'ecr:CompleteLayerUpload', 'ecr:PutImage', 'ecr:DescribeImages'], 'Resource': arn}]})))
    image = config.get('movie_image')
    if image:
        definition = aws.batch.JobDefinition('ctbk-movies-definition', name='ctbk-movies', type='container', platform_capabilities=['FARGATE'], timeout={'attempt_duration_seconds': 7200}, retry_strategy={'attempts': 1}, container_properties=pulumi.Output.all(exec_role.arn, task_role.arn, log.name).apply(lambda args: json.dumps({
            'image': image, 'executionRoleArn': args[0], 'jobRoleArn': args[1],
            'runtimePlatform': {'operatingSystemFamily': 'LINUX', 'cpuArchitecture': 'X86_64'},
            'resourceRequirements': [{'type': 'VCPU', 'value': '4'}, {'type': 'MEMORY', 'value': '8192'}],
            'networkConfiguration': {'assignPublicIp': 'ENABLED'},
            'logConfiguration': {'logDriver': 'awslogs', 'options': {'awslogs-group': args[2], 'awslogs-region': REGION}},
        })))
        secret = aws.secretsmanager.Secret('ctbk-movie-gateway-secret', name='ctbk/movie-gateway', description='HMAC authenticates the ctbk movie Worker to its Batch gateway')
        aws.secretsmanager.SecretVersion('ctbk-movie-gateway-secret-value', secret_id=secret.id, secret_string=config.require_secret('movie_gateway_secret'))
        role = aws.iam.Role('ctbk-movies-gateway-role', name='ctbk-movies-gateway', assume_role_policy=_assume('lambda.amazonaws.com'))
        aws.iam.RolePolicyAttachment('ctbk-movies-gateway-basic', role=role.name, policy_arn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole')
        aws.iam.RolePolicy('ctbk-movies-gateway-objects', role=role.name, policy=s3_policy)
        aws.iam.RolePolicy('ctbk-movies-gateway-control', role=role.name, policy=pulumi.Output.all(queue.arn, definition.arn, secret.arn).apply(lambda args: json.dumps({'Version': '2012-10-17', 'Statement': [
            {'Effect': 'Allow', 'Action': ['batch:SubmitJob', 'batch:TagResource'], 'Resource': [args[0], args[1]]},
            {'Effect': 'Allow', 'Action': ['batch:DescribeJobs', 'batch:ListJobs', 'batch:TerminateJob'], 'Resource': '*'},
            {'Effect': 'Allow', 'Action': 'secretsmanager:GetSecretValue', 'Resource': args[2]},
        ]})))
        gateway_source = Path(__file__).resolve().parents[1] / 'batch/movies/gateway.py'
        function = aws.lambda_.Function('ctbk-movies-gateway', name='ctbk-movies-gateway', role=role.arn, runtime='python3.13', handler='gateway.handler', code=pulumi.AssetArchive({'gateway.py': pulumi.FileAsset(str(gateway_source))}), timeout=30, memory_size=256, reserved_concurrent_executions=2, environment={'variables': {
            'MOVIE_GATEWAY_SECRET_ARN': secret.arn, 'MOVIE_BUCKET': bucket.id,
            'MOVIE_QUEUE': queue.arn, 'MOVIE_JOB_DEFINITION': definition.arn, 'MOVIE_SITE_ORIGIN': 'https://ctbk.dev',
        }})
        url = aws.lambda_.FunctionUrl('ctbk-movies-url', function_name=function.name, authorization_type='NONE')
        aws.lambda_.Permission('ctbk-movies-url-invoke', action='lambda:InvokeFunctionUrl', function=function.name, principal='*', function_url_auth_type='NONE')
        aws.lambda_.Permission('ctbk-movies-url-function', action='lambda:InvokeFunction', function=function.name, principal='*', invoked_via_function_url=True)
        pulumi.export('movie_gateway_url', url.function_url)
    pulumi.export('movie_database_id', db.id)
    pulumi.export('movie_image_repository', repo.repository_url)
    pulumi.export('movie_artifact_bucket', bucket.id)
