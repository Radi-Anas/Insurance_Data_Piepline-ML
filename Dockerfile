FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \\
    chromium \\
    chromium-driver \\
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "prefect_flow.py"]
