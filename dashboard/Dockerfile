FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ ./config/
COPY database/ ./database/
COPY dashboard/ ./dashboard/

EXPOSE 8501

HEALTHCHECK CMD python -c "import os, urllib.request; port = os.environ.get('PORT', '8501'); urllib.request.urlopen('http://localhost:' + port + '/_stcore/health')" || exit 1

CMD ["sh", "-c", "streamlit run dashboard/app.py --server.port=${PORT:-8501} --server.address=0.0.0.0 --server.headless=true --browser.gatherUsageStats=false"]
