FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY collect_payouts.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "collect_payouts.py", "run"]
