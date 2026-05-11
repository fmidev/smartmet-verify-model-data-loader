ARG PYTHON_VERSION=3.14.4
FROM python:${PYTHON_VERSION}-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONPATH=/app/src \
    TZ=UTC

RUN useradd -r -u 1000 appuser
USER appuser

ENTRYPOINT ["python", "-u", "-m", "smartmet_verify_model_data_loader"]
