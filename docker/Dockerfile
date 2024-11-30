FROM python:3.12-slim

ENV DAGSTER_HOME=/app/dagster_home

RUN mkdir -p /app/dagster_home

RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean
    
    
WORKDIR /app

COPY requirements.txt .
COPY docker/dagster.yaml /app/dagster_home/dagster.yaml
COPY ETL /app
    
    
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 3000

# CMD ["dagster-webserver", "-w", "workspace.yaml", "-h", "0.0.0.0", "-p", "3000"]


