FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /state \
    && rm -rf /app/Download /app/data /app/logs /app/.run /app/reports /app/jobs.db \
    && ln -s /state/Download /app/Download \
    && ln -s /state/data /app/data \
    && ln -s /state/logs /app/logs \
    && ln -s /state/run /app/.run \
    && ln -s /state/reports /app/reports \
    && ln -s /state/jobs.db /app/jobs.db

COPY docker/entrypoint.sh /usr/local/bin/beam-entrypoint
RUN chmod +x /usr/local/bin/beam-entrypoint

ENV PYTHONUNBUFFERED=1
ENV WEBAPP_HOST=0.0.0.0
ENV WEBAPP_PORT=8501

EXPOSE 8501

ENTRYPOINT ["beam-entrypoint"]
CMD ["python", "-m", "uvicorn", "webapp.main:app", "--host", "0.0.0.0", "--port", "8501"]
