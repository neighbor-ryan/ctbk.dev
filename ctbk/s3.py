import json
from sys import argv, stdout
from functools import cache

import boto3


@cache
def get_s3():
    return boto3.client('s3')


def parse_bkt_key(args: tuple[str, ...]) -> tuple[str, str]:
    if len(args) == 1:
        arg = args[0]
        if arg.startswith('s3://'):
            arg = arg[len('s3://'):]
        bkt, key = arg.split('/', 1)
    elif len(args) == 2:
        bkt, key = args
    else:
        raise ValueError('Too many arguments')
    return bkt, key


def get_etag(*args: str) -> str:
    """
    Get the ETag of an S3 object.

    Args:
        bkt (str): The name of the S3 bucket
        key (str): The key (path) of the object in the bucket

    Returns:
        str: The ETag value of the S3 object
    """
    bkt, key = parse_bkt_key(args)
    s3 = get_s3()
    res = s3.head_object(Bucket=bkt, Key=key)
    etag = res.get('ETag', '').strip('"')

    return etag


def get_etags(*args: str) -> dict[str, str]:
    bkt, key = parse_bkt_key(args)
    s3 = get_s3()
    res = s3.list_objects_v2(Bucket=bkt, Prefix=key)
    etags = {
        obj['Key']: obj['ETag'].strip('"')
        for obj in res.get('Contents', [])
    }
    return etags


if __name__ == '__main__':
    for arg in argv[1:]:
        if arg.endswith('/'):
            etags = get_etags(arg)
            json.dump(etags, stdout, indent=2)
        else:
            etag = get_etag(arg)
            print(etag)
