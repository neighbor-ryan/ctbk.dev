"""Stream the existing browser renderer into a private S3 movie artifact."""
from base64 import b64decode
from json import dumps, loads
from os import environ
from pathlib import Path
from re import search
from subprocess import PIPE, STDOUT, Popen
from time import monotonic

import boto3


def main() -> None:
    job = loads(b64decode(environ['MOVIE_JOB']))
    root = Path('tmp/movies')
    root.mkdir(parents=True, exist_ok=True)
    output = root / f"{job['id']}.mp4"
    s3 = boto3.client('s3')
    prefix = f"jobs/{job['id']}/"
    last_write = 0
    frame = 0
    args = ['node', 'scripts/render-timelapse.mjs', '--width', str(job['width']), '--height', str(job['height']), '--fps', str(job['fps']), environ['MOVIE_SITE_ORIGIN'] + '/timelapse?' + job['query'], str(output)]
    with Popen(args, stdout=PIPE, stderr=STDOUT, text=True, bufsize=1) as process:
        for line in process.stdout:
            print(line, end='', flush=True)
            match = search(r'frame (\d+)/(\d+)', line)
            if match:
                frame = int(match[1])
                if monotonic() - last_write > 5:
                    s3.put_object(Bucket=environ['MOVIE_BUCKET'], Key=prefix + 'progress.json', Body=dumps({'frame': frame}).encode(), ContentType='application/json')
                    last_write = monotonic()
        code = process.wait()
    if code:
        raise RuntimeError(f'Renderer exited with code {code}')
    s3.upload_file(str(output), environ['MOVIE_BUCKET'], prefix + 'movie.mp4', ExtraArgs={'ContentType': 'video/mp4'})
    s3.upload_file(str(output) + '.json', environ['MOVIE_BUCKET'], prefix + 'movie.json', ExtraArgs={'ContentType': 'application/json'})
    s3.put_object(Bucket=environ['MOVIE_BUCKET'], Key=prefix + 'progress.json', Body=dumps({'frame': job['frames']}).encode(), ContentType='application/json')
    output.unlink()
    Path(str(output) + '.json').unlink()


if __name__ == '__main__':
    main()
