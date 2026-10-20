FROM mcr.microsoft.com/playwright:v1.60.0-noble
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg python3 python3-boto3 && rm -rf /var/lib/apt/lists/*
RUN npm install -g pnpm@9
WORKDIR /app
COPY www/package.json www/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY www/scripts/render-timelapse.mjs scripts/render-timelapse.mjs
COPY batch/movies/run.py run.py
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "run.py"]
