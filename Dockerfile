FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps (CA certs for AWS endpoints)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY app.py /app/

# Streamlit defaults
EXPOSE 8501

# Environment (override at runtime)
ENV AWS_DEFAULT_REGION=us-east-1
ENV ATHENA_WORKGROUP=primary
# Provide at runtime:
# - ATHENA_S3_OUTPUT=s3://kerok-athena-query-output-storage-v1/
# - ATHENA_DATABASE=kerok-healthcare-bronze
# - ATHENA_CATALOG=AwsDataCatalog
# And AWS creds (either role on EC2 or env vars)

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
