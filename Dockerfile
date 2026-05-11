FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_model_data.py .

RUN useradd -r -u 1000 appuser
USER appuser

ENTRYPOINT ["python3", "-u", "load_model_data.py"]
