FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
# Pas de CMD : docker-compose choisit le script (producer / speed_consumer / archiver)
