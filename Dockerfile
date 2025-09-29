FROM apache/airflow:3.1.0-python3.11

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt