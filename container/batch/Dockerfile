FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    pandas \
    pyarrow \
    requests \
    google-cloud-storage

COPY *.py /app/

ENTRYPOINT ["python", "-u", "/app/binance_historical_to_gcs.py"]