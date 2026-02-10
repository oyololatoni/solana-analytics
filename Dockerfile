FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# IMPORTANT:
# - Uses Fly-injected PORT when present
# - Falls back to 8000 if PORT is missing
CMD ["sh", "-c", "echo STARTING UVICORN && uvicorn api.main:app --host 0.0.0.0 --port ${PORT:-8000}"]


